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

-module(trcb).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("ishikawa.hrl").

-export([check_causal_delivery/4, try_to_deliever/3]).

%% Broadcast message.
-callback tcbcast(message()) -> ok.

%% Deliver a message.
-callback tcbdeliver(message(), timestamp()) -> ok.

%% Determine if a timestamp is stable.
-callback tcbstable(timestamp()) -> {ok, boolean()}.

%% check if a message should be deliver and deliver it, if not add itto the queue
-spec check_causal_delivery({message(), timestamp()}, actor(), timestamp(), [{timestamp(), message()}]) -> {timestamp(), [{timestamp(), message()}]}.
check_causal_delivery({Msg, MsgVV}, From, VV, Queue) ->
    case vclock:dominates(MsgVV, VV) of
        true ->
            NewVV = vclock:increment(From, VV),
            %% TODO: Handle message,
            try_to_deliever(Queue, From, {NewVV, Queue});
        false ->
            {VV, [Queue | [{Msg, MsgVV}]]}
    end.

%% Check for all messages in the queue to be delivered
%% Called upon delievery of a new message that could affect the delivery of messages in the queue
-spec try_to_deliever([{timestamp(), message()}], actor(), {timestamp(), [{timestamp(), message()}]}) -> {timestamp(), [{timestamp(), message()}]}.
try_to_deliever([], _From, {VV, Queue}) -> {VV, Queue};
try_to_deliever([{MsgVV, _Msg}=El | RQueue], From, {VV, Queue}=V) ->
    case vclock:dominates(MsgVV, VV) of
        true ->
            NewVV = vclock:increment(From, VV),
            %% TODO: Handle message,
            Queue1 = lists:delete(El, Queue),
            try_to_deliever(Queue1, From, {NewVV, Queue1});
        false ->
            try_to_deliever(RQueue, From, V)
    end.