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

-module(ishikawa).

-export([start/0,
         stop/0]).

%% API
-export([tcbcast/1,
         tcbdeliver/3,
         tcbstable/1]).

-include("ishikawa.hrl").

%% @doc Start the application.
start() ->
    application:ensure_all_started(ishikawa).

%% @doc Stop the application.
stop() ->
    application:stop(ishikawa).

%%%===================================================================
%%% API
%%%===================================================================

%% Broadcast message.
-spec tcbcast(message()) -> ok.
tcbcast(MessageBody) ->
    ishikawa_backend:tcbcast(MessageBody).

%% Deliver a message.
-spec tcbdeliver(actor(), message(), timestamp()) -> ok.
tcbdeliver(MessageActor, MessageBody, MessageVClock) ->
    ishikawa_backend:tcbdeliver(MessageActor, MessageBody, MessageVClock).

%% Determine if a timestamp is stable.
-spec tcbstable(timestamp()) -> {ok, boolean()}.
tcbstable(Timestamp) ->
    ishikawa_backend:tcbstable(Timestamp).
