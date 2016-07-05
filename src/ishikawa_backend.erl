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

-record(state, {actor :: actor(),
                vv :: timestamp(),
                members :: [node()],
                svv :: timestamp(),
                rtm :: timestamp_matrix(),
                to_be_delivered_queue :: [{actor(), message(), timestamp()}],
                to_be_ack_queue :: [{{actor(), timestamp()}, [node()]}]}).

%%%===================================================================
%%% trcb callbacks
%%%===================================================================

%% Broadcast message.
-spec tcbcast(message()) -> ok.
tcbcast(Message) ->
    gen_server:call(?MODULE, {tcbcast, Message}, infinity).

%% Deliver a message.
-spec tcbdeliver(actor(), message(), timestamp()) -> ok.
tcbdeliver(Actor, Message, Timestamp) ->
    gen_server:call(?MODULE, {tcbdeliver, Actor, Message, Timestamp}, infinity).

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
-spec init([]) -> {ok, #state{}}.
init([]) ->
    %% Seed the process at initialization.
    random:seed(erlang:phash2([node()]),
                erlang:monotonic_time(),
                erlang:unique_integer()),

    %% Generate actor identifier.
    Actor = gen_actor(),

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

    %% Add membership callback.
    ?PEER_SERVICE:add_sup_callback(fun ?MODULE:update/1),

    %% Add initial members.
    {ok, Members} = ?PEER_SERVICE:members(),
    lager:info("Initial membership: ~p", [Members]),

    {ok, #state{actor=Actor, vv=VClock, members=Members, svv=SVV, rtm=RTM, to_be_delivered_queue=ToBeDeliveredQueue, to_be_ack_queue=ToBeAckQueue}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.
handle_call({tcbcast, Message}, _From, #state{actor=Actor,
                                              vv=VClock0,
                                              members=Members,
                                              to_be_ack_queue=ToBeAckQueue} = State) ->
    %% First, increment the vector.
    VClock = vclock:increment(Actor, VClock0),

    %% Generate message.
    Message1 = {tcbcast, Actor, encode(Message), VClock},

    %% Transmit to membership.
    [send(Message1, Peer) || Peer <- Members],

    %% Add members to the queue of not ack messages.
    ToBeAckQueue1 = [ToBeAckQueue | [{{Actor, VClock}, Members}]],

    {reply, ok, State#state{vv=VClock, to_be_ack_queue=ToBeAckQueue1}};
handle_call({tcbdeliver, Origin, Message, Timestamp}, _From, #state{vv=VClock0,
                                              rtm=RTM0,
                                              to_be_delivered_queue=Queue0} = State) ->

    %% Check if the message should be delivered
    {VClock, Queue} = trcb:check_causal_delivery({Origin, Message, Timestamp}, VClock0, Queue0),

    %% Update the Recent Timestamp Matrix
    RTM = mclock:update_rtm(RTM0, Origin, Timestamp),

    %% Update the Stable Version Vector
    SVV = mclock:update_stablevv(RTM),

    {reply, ok, State#state{vv=VClock, to_be_delivered_queue=Queue, svv=SVV, rtm=RTM}};
handle_call({tcbstable, _Timestamp}, _From, State) ->
    %% TODO: Implement me.
    {reply, {ok, false}, State};
handle_call({tcbcast, Actor, Message, VClock} = Msg, From, #state{to_be_ack_queue=Queue0, members=Members} = State) ->
    case lists:keyfind({Actor, VClock}, 1, Queue0) of
        {_, _} ->
            %% Generate message.
            MessageAck = {tcbcast_ack, Actor, Message, VClock},

            Queue1 = Queue0,

            %% Send Ack back to message sender
            send(MessageAck, From);
        false ->
            %% Add members to the queue of not ack messages.
            Queue1 = [Queue0 | [{Actor, Message, VClock}, Members]],

            %% Transmit to membership.
            [send(Msg, Peer) || Peer <- Members],

            %% Generate message.
            MessageAck = {tcbcast_ack, Actor, Message, VClock},

            %% Send Ack back to message sender
            send(MessageAck, From)
    end,
    {reply, ok, State#state{to_be_delivered_queue=Queue1}};
handle_call({tcbcast_ack, Actor, Message, VClock}, From, #state{to_be_ack_queue=QueueAck0} = State) ->
    case lists:keyfind({Actor, VClock}, 1, QueueAck0) of
        {_, QueueMsg} ->
            case length(QueueMsg)>0 of
                true ->
                    case length(QueueMsg) of
                        1 ->
                            QueueMsg1 = lists:delete(From, QueueMsg),
                            tcbdeliver(Actor, Message, VClock);
                        _ ->
                            QueueMsg1 = lists:delete(From, QueueMsg)
                    end
            end
    end,
    QueueAck1 = lists:keyreplace({Actor, VClock}, 1, QueueAck0, {{Actor, VClock}, QueueMsg1}),
    {reply, ok, State#state{to_be_ack_queue=QueueAck1}}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast({membership, Members}, State) ->
    {noreply, State#state{members=Members}};
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
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
gen_actor() ->
    Node = atom_to_list(node()),
    Unique = time_compat:unique_integer([positive]),
    TS = integer_to_list(Unique),
    Term = Node ++ TS,
    crypto:hash(sha, Term).

%% @private
send(Msg, Peer) ->
    PeerServiceManager = ?PEER_SERVICE:manager(),
    PeerServiceManager:forward_message(Peer, ?MODULE, Msg).

%% @private
encode(Message) ->
    term_to_binary(Message).
