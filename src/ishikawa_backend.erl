%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher S. Meiklejohn.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(ishikawa_backend).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).
-behaviour(trcb).

%% API
-export([start_link/0,
         update/1]).

%% trcb callbacks
-export([tcbcast/1,
         tcbdeliver/3,
         tcbstable/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("ishikawa.hrl").

-define(PEER_SERVICE, partisan_peer_service).
-define(WAIT_TIME_BEFORE_CHECK_RESEND, 5000).
-define(WAIT_TIME_BEFORE_RESEND, 10000).

-record(state, {myself :: node(),
                actor :: actor(),
                vv :: timestamp(),
                members :: [node()],
                svv :: timestamp(),
                rtm :: timestamp_matrix(),
                time_ref :: integer(),
                to_be_delivered_queue :: [{actor(), message(), timestamp()}],
                to_be_ack_queue :: [{{actor(), timestamp()}, integer(), [node()]}],
                msg_handling_fun :: fun()}).

%%%===================================================================
%%% trcb callbacks
%%%===================================================================

%% Broadcast message.
-spec tcbcast(message()) -> ok.
tcbcast(MessageBody) ->
    gen_server:call(?MODULE, {tcbcast, MessageBody}, infinity).

%% Deliver a message.
-spec tcbdeliver(actor(), message(), timestamp()) -> ok.
tcbdeliver(Actor, MessageBody, MessageVClock) ->
    gen_server:cast(?MODULE, {tcbdeliver, Actor, MessageBody, MessageVClock}).

%% Determine if a timestamp is stable.
-spec tcbstable(timestamp()) -> {ok, boolean()}.
tcbstable(Timestamp) ->
    gen_server:call(?MODULE, {tcbstable, Timestamp}, infinity).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Update membership.
-spec update(term()) -> ok.
update(State) ->
    Members = ?PEER_SERVICE:decode(State),
    gen_server:cast(?MODULE, {membership, Members}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init(list()) -> {ok, #state{}}.
init([]) ->
    Fun = fun(Msg) ->
        lager:warning("Delivering message: ~p", [Msg]),
        ok
    end,
    init([Fun]);
init([Fun]) ->
    %% Seed the process at initialization.
    rand_compat:seed(erlang:phash2([node()]),
                     erlang:monotonic_time(),
                     erlang:unique_integer()),

    %% Generate actor identifier.
    Actor = myself(),

    %% Generate local version vector.
    VClock = vclock:fresh(),

    %% Generate local stable version vector.
    SVV = vclock:fresh(),

    %% Generate local recent timestamp matrix.
    RTM = mclock:fresh(),

    %% Generate local to be delivered messages queue.
    ToBeDeliveredQueue = [],

    %% Generate local to be acknowledged messages queue.
    ToBeAckQueue = [],

    %% Message handling funtion.
    MessageHandlingFun = Fun,

    %% Who am I?
    Myself = myself(),

    %% Add membership callback.
    ?PEER_SERVICE:add_sup_callback(fun ?MODULE:update/1),

    %% Add initial members.
    {ok, Members} = ?PEER_SERVICE:members(),
    lager:info("Initial membership: ~p", [Members]),

    {_, TRef} = timer:send_after(?WAIT_TIME_BEFORE_CHECK_RESEND, check_resend),

    {ok, #state{myself=Myself,
                actor=Actor,
                vv=VClock,
                members=Members,
                svv=SVV,
                rtm=RTM,
                time_ref=TRef,
                to_be_delivered_queue=ToBeDeliveredQueue,
                to_be_ack_queue=ToBeAckQueue,
                msg_handling_fun=MessageHandlingFun}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call({tcbcast, Message},
            _From,
            #state{myself=Myself,
                   actor=Actor,
                   members=Members,
                   vv=VClock0,
                   to_be_ack_queue=ToBeAckQueue0}=State) ->
    %% Node sending the message.
    Sender = Myself,

    %% Generate message.
    Msg = {tcbcast, Actor, encode(Message), VClock0, Sender},

    %% Transmit to membership.
    [send(Msg, Peer) || Peer <- Members],

    %% Get current time in milliseconds.
    CurrentTime = get_timestamp(),

    %% Add members to the queue of not ack messages and increment the vector clock.
    ToBeAckQueue = ToBeAckQueue0 ++ [{{Actor, VClock0}, CurrentTime, Members}],
    VClock = vclock:increment(Actor, VClock0),

    %% Attempt to deliver locally if we received it on the wire.
    tcbdeliver(Actor, Message, VClock0),

    {reply, ok, State#state{to_be_ack_queue=ToBeAckQueue, vv=VClock}};

handle_call({tcbstable, _Timestamp}, _From, State) ->
    %% TODO: Implement me.
    {reply, {ok, false}, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.

%% TODO: What are the rules on when we can deliver a particular message
%%       in the system.
%%       Why did Georges wait for the message to be acknolwedged by
%%       everyone?  That seems unnecessary.
handle_cast({tcbdeliver, Actor, MessageBody, MessageVClock} = Msg,
            #state{vv=VClock0,
                   myself=Myself,
                   rtm=RTM0,
                   to_be_delivered_queue=Queue0,
                   msg_handling_fun=Fun} = State) ->
    lager:info("Attempting to deliver message: ~p at ~p", [Msg, Myself]),

    %% Check if the message should be delivered and delivers it or not
    {VClock, Queue} = trcb:causal_delivery({Actor, MessageBody, MessageVClock},
                                           VClock0,
                                           Queue0,
                                           Fun),

    %% Update the Recent Timestamp Matrix
    RTM = mclock:update_rtm(RTM0, Actor, MessageVClock),

    %% Update the Stable Version Vector
    SVV = mclock:update_stablevv(RTM),

    {noreply, State#state{vv=VClock, to_be_delivered_queue=Queue, svv=SVV, rtm=RTM}};

handle_cast({tcbcast_ack, Actor, _Message, VClock, Sender} = Msg,
            #state{to_be_ack_queue=ToBeAckQueue0} = State) ->
    lager:info("Received message: ~p from ~p", [Msg, Sender]),

    %% Get list of waiting ackwnoledgements.
    {_, Timestamp, Members0} = lists:keyfind({Actor, VClock},
                                     1,
                                     ToBeAckQueue0),

    %% Remove this member as an outstanding member.
    Members = lists:delete(Sender, Members0),

    ToBeAckQueue = case length(Members) of
        0 ->
            %% None left, remove from ack queue.
            lists:keydelete({Actor, VClock}, 1, ToBeAckQueue0);
        _ ->
            %% Still some left, preserve.
            lists:keyreplace({Actor, VClock},
                             1,
                             ToBeAckQueue0, {{Actor, VClock}, Timestamp, Members})
    end,

    {noreply, State#state{to_be_ack_queue=ToBeAckQueue}};

handle_cast({tcbcast, Actor, MessageBody, MessageVClock, Sender} = Msg,
            #state{myself=Myself,
                   to_be_ack_queue=ToBeAckQueue0,
                   members=Members} = State) ->
    lager:info("Received message: ~p from ~p", [Msg, Sender]),

    case already_seen_message(Msg, ToBeAckQueue0) of
        true ->
            %% Already seen, do nothing.
            lager:info("Ignoring duplicate message from cycle."),
            {noreply, State};
        false ->
            %% Generate list of peers that need the message.
            ToMembers = Members -- lists:flatten([Sender, Myself, Actor]),
            lager:info("Broadcasting message to peers: ~p", [ToMembers]),

            %% Transmit to peers that need the message.
            [send(Msg, Peer) || Peer <- ToMembers],

            %% Get current time in milliseconds.
            CurrentTime = get_timestamp(),

            %% Generate message.
            MessageAck = {tcbcast_ack, Actor, MessageBody, MessageVClock, Myself},

            %% Send ack back to message sender.
            send(MessageAck, Sender),

            %% Attempt to deliver locally if we received it on the wire.
            tcbdeliver(Actor, MessageBody, MessageVClock),

            %% Add members to the queue of not ack messages and increment the vector clock.
            ToBeAckQueue = ToBeAckQueue0 ++ [{{Actor, MessageVClock}, CurrentTime, ToMembers}],

            {noreply, State#state{to_be_ack_queue=ToBeAckQueue}}
    end;

handle_cast({membership, Members}, State) ->
    {noreply, State#state{members=Members}};

handle_cast(Msg, State) ->
    lager:warning("Unhandled cast messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(check_resend, #state{myself=Myself, to_be_ack_queue=ToBeAckQueue0} = State) ->
    Now = get_timestamp(),
    ToBeAckQueue1 = lists:foldl(
        fun({{Actor, VClock} = Msg, Timestamp0, MembersList}, ToBeAckQueue) ->
            case MembersList =/= [] andalso (Now - Timestamp0 > ?WAIT_TIME_BEFORE_RESEND) of
                true ->
                    Message1 = {tcbcast, Actor, Msg, VClock, Myself},
                    %% Retransmit to membership.
                    %% TODO: Only retransmit where it's needed.
                    [send(Message1, Peer) || Peer <- MembersList],
                    lists:keyreplace({Actor, VClock},
                                     1,
                                     ToBeAckQueue,
                                     {{Actor, VClock}, get_timestamp(), MembersList});
                false ->
                    %% Do nothing.
                    ToBeAckQueue
            end
        end,
        ToBeAckQueue0,
        ToBeAckQueue0),
    {noreply, State#state{to_be_ack_queue=ToBeAckQueue1}};

handle_info(Msg, State) ->
    lager:warning("Unhandled info messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
send(Msg, Peer) ->
    lager:info("Sending message: ~p to peer: ~p", [Msg, Peer]),
    PeerServiceManager = ?PEER_SERVICE:manager(),
    PeerServiceManager:forward_message(Peer, ?MODULE, Msg).

%% @private
encode(Message) ->
    term_to_binary(Message).

%% @private get current time in milliseconds
-spec get_timestamp() -> integer().
get_timestamp() ->
  {Mega, Sec, Micro} = os:timestamp(),
  (Mega*1000000 + Sec)*1000 + round(Micro/1000).

%% @private
myself() ->
    node().

%% @private
already_seen_message({tcbcast, Actor, _Message, VClock, _Sender}, ToBeAckQueue) ->
    lists:keymember({Actor, VClock}, 1, ToBeAckQueue).
