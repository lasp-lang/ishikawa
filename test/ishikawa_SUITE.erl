%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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
%%

-module(ishikawa_SUITE).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").


-include("ishikawa.hrl").

-define(CLIENT_NUMBER, 3).
-define(NODES_NUMBER, 5).
-define(MAX_MSG_NUMBER, 5).
-define(PEER_PORT, 9000).

suite() ->
    [{timetrap, {seconds, 60}}].

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(_Config) ->
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(Case, _Config) ->
    ct:pal("Beginning test case ~p", [Case]),

    _Config.

end_per_testcase(Case, _Config) ->
    ct:pal("Ending test case ~p", [Case]),

    _Config.

all() ->
    [%causal_delivery_test1,
    causal_delivery_test2].

%% ===================================================================
%% Tests.
%% ===================================================================

%% Not very interesting to test causal delivery with a Client/Server star topology 
causal_delivery_test1(Config) ->
    %% Use the client/server peer service manager.
    Manager = partisan_client_server_peer_service_manager,

    %% Specify servers.
    Servers = [server],

    %% Specify clients.
    Clients = client_list(?CLIENT_NUMBER),

    %% Start nodes.
    Nodes = start(client_server_manager_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {client_number, ?CLIENT_NUMBER},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Verify membership.
    %%
    VerifyFun = fun({Name, Node}) ->
      {ok, Members} = rpc:call(Node, Manager, members, []),

        %% If this node is a server, it should know about all nodes.
        SortedNodes = case lists:member(Name, Servers) of
          true ->
            lists:usort([N || {_, N} <- Nodes]);
          false ->
            %% Otherwise, it should only know about the server
            %% and itself.
            lists:usort(
              lists:map(
                fun(Server) ->
                  proplists:get_value(Server, Nodes)
                end,
                Servers
              ) ++ [Node]
            )
        end,

        SortedMembers = lists:usort(Members),
        case SortedMembers =:= SortedNodes of
          true ->
            ok;
          false ->
            ct:fail("Membership incorrect; node ~p should have ~p but has ~p", [Node, Nodes, Members])
        end
    end,

    %% Verify the membership is correct.
    lists:foreach(VerifyFun, Nodes),

    %% Get the first node in the list.
    [{_ServerName, ServerNode} | ClientNodes] = Nodes,

    %% Configure the delivery function on each node to send the messages
    %% back to the test runner in delivery order.
    Self = self(),

    lists:foreach(fun({_ClientName, ClientNode}) ->
      DeliveryFun = fun({_VV, Msg}) ->
        Self ! {delivery, ClientNode, Msg},
        ok
      end,
      ok = rpc:call(ClientNode, ishikawa, tcbdelivery, [DeliveryFun])
    end,
    ClientNodes),

    %% Send a series of messages.
    {ok, _} = rpc:call(ServerNode, ishikawa, tcbcast, [1]),
    
    
    %% Ensure each node receives a message.
    lists:foreach(fun({_ClientName, ClientNode}) ->
      receive
        {delivery, ClientNode, 1} ->
          ok;
        {delivery, ClientNode, Message} ->
          ct:fail("Client ~p received incorrect message: ~p", [ClientNode, Message])
      after
        1000 ->
          ct:fail("Client ~p didn't receive message!", [ClientNode])
      end
    end, ClientNodes),

    %% Stop nodes.
    stop(Nodes),

    ok.

%% Test with full membership
causal_delivery_test2(Config) ->

    lager:info("HEY"),
    %% Use the default peer service manager.
    Manager = partisan_default_peer_service_manager,

    %% Start nodes.
    Nodes = start(default_manager_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {client_number, ?NODES_NUMBER}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Verify membership.
    %%
    VerifyFun = fun({_Name, Node}) ->
      {ok, Members} = rpc:call(Node, Manager, members, []),

      %% If this node is a server, it should know about all nodes.
      SortedNodes = lists:usort([N || {_, N} <- Nodes]) -- [Node],
      SortedMembers = lists:usort(Members) -- [Node],
      case SortedMembers =:= SortedNodes of
        true ->
          ok;
        false ->
          ct:fail("Membership incorrect; node ~p should have ~p but has ~p", [Node, SortedNodes, SortedMembers])
      end
    end,

    %% Verify the membership is correct.
    lists:foreach(VerifyFun, Nodes),

    ETS = ets:new(delMsgQ, [ordered_set, public]),

    %% Add for each node an empty set to record delivered messages
    lists:foreach(fun({_Name, Node}) ->
      ets:insert(ETS, {Node, []})
    end,
    Nodes),

    Self = self(),

    %% create map from name to number of messages
    RandNumMsgToBeSentMap = lists:foldl(
      fun({_Name, Node}, Acc) ->
        orddict:store(Node, ?MAX_MSG_NUMBER, Acc)
        %orddict:store(Node, rand:uniform(?MAX_MSG_NUMBER), Acc)
      end,
      orddict:new(),
    Nodes),

    ct:pal("MAP ~p", [RandNumMsgToBeSentMap]),

    TotNumMsgToRecv = lists:sum([V || {_, V} <- RandNumMsgToBeSentMap]) * ?NODES_NUMBER,

    Receiver = spawn(?MODULE, fun_receive, [ETS, Nodes, TotNumMsgToRecv, 0, Self]),

    lists:foreach(fun({_Name, Node}) ->
      DeliveryFun = fun({VV, _Msg}) ->
        lager:info("DELIVERY ~p ~p", [VV, Node]),
        Receiver ! {delivery, Node, VV},
        ok
      end,
      ok = rpc:call(Node,
                    ishikawa,
                    tcbdelivery,
                    [DeliveryFun])
    end,
    Nodes),

    %% Sending random messages and recording on delivery the VV of the messages in delivery order per Node
    lists:foreach(fun({_Name, Node}) ->
      spawn(?MODULE, fun_send, [Node, orddict:fetch(Node, RandNumMsgToBeSentMap)])
    end, Nodes),

% timer:sleep(30000),

    lists:foreach(fun({_Name, Node}) ->
      [{_, DelMsgQxx}] = ets:lookup(ETS, Node),
      ct:pal("ETS Node ~p Queue ~p", [Node, DelMsgQxx])
    end, Nodes),

    % loop_until_done(ETS),

    fun_ready_to_check(Nodes, ETS),

    % ct:fail("after spawning fun_send"),

    %% Stop nodes.
    stop(Nodes),

    ok.

fun_send(_Node, 0) ->
  ok;
fun_send(Node, Times) ->
  ct:pal("FUN SEND"),
  %timer:sleep(rand:uniform(5)*1000),
  timer:sleep(1000),
  {ok, _} = rpc:call(Node, ishikawa, tcbcast, [msg]),
  fun_send(Node, Times - 1).

fun_receive(ETS, Nodes, TotalMessages, TotalReceived, Runner) ->
  ct:pal("FUN RECEIVE ~p ~p ~p ~p ~p", [ETS, Nodes, TotalMessages, TotalReceived, Runner]),
  receive
    {delivery, Node, VV} ->
      ct:pal("RECEIVED from Node ~p", [Node]),
      [{_, DelMsgQ}] = ets:lookup(ETS, Node),
      ets:insert(ETS, {Node, DelMsgQ ++ [VV]}),
      %% For each node, update the number of delivered messages on every node
      TotalReceived1 = TotalReceived + 1,
      ct:pal("~p of ~p", [TotalReceived1, TotalMessages]),
      %% check if all msgs were delivered on all the nodes
      case TotalMessages =:= TotalReceived1 of
        true ->
          ct:pal("DONE SENT"),
          % fun_check_delivery(Nodes, ETS);
          Runner ! done;
        false ->
          fun_receive(ETS, Nodes, TotalMessages, TotalReceived1, Runner)
      end;
    M ->
      ct:fail("UNKWONN ~p", [M])
    end.

fun_check_delivery(Nodes, ETS) ->
      ct:pal("fun_check_delivery"),

  lists:foreach(
    fun({_Name, Node}) ->
      [{_, DelMsgQ2}] = ets:lookup(ETS, Node),
      lists:foldl(
        fun(I, AccI) ->
          lists:foldl(
            fun(J, AccJ) ->
              XXX = vclock:descends(lists:nth(I, DelMsgQ2), lists:nth(J, DelMsgQ2)),
              ct:pal("Value should be false, it is ~p", [XXX]),
              AccJ andalso not XXX
            end,
            AccI,
          lists:seq(I+1, length(DelMsgQ2))) 
        end,
        true,
      lists:seq(1, length(DelMsgQ2)-1))
    end,
  Nodes).%,
  % ets:insert(ETS, {done, 1}).

% loop_until_done(ETS) ->
%   [{_, Done}] = ets:lookup(ETS, done),
%   case Done =:= 1 of
%     true ->
%       ok;
%     false ->
%       loop_until_done(ETS)
%   end.

fun_ready_to_check(Nodes, ETS) ->
  receive
    done ->
      fun_check_delivery(Nodes, ETS);
    M ->
      ct:fail("received incorrect message: ~p", [M])
  end.

%% ===================================================================
%% Internal functions.
%% ===================================================================

%% @private
start(_Case, _Config, Options) ->
    %% Launch distribution for the test runner.
    ct:pal("Launching Erlang distribution..."),

    os:cmd(os:find_executable("epmd") ++ " -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@" ++ Hostname), shortnames]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok
    end,

    %% Load sasl.
    application:load(sasl),
    ok = application:set_env(sasl,
                             sasl_error_logger,
                             false),
    application:start(sasl),

    %% Load lager.
    {ok, _} = application:ensure_all_started(lager),

    ClientNumber = proplists:get_value(client_number, Options, 3),
    NodeNames = case proplists:get_value(partisan_peer_service_manager, Options) of
      partisan_client_server_peer_service_manager ->
        node_list(ClientNumber);
      partisan_default_peer_service_manager ->
        client_list(ClientNumber)
    end,
    
    %% Start all nodes.
    InitializerFun = fun(Name) ->
      ct:pal("Starting node: ~p", [Name]),

      NodeConfig = [{monitor_master, true}, {startup_functions, [{code, set_path, [codepath()]}]}],

      case ct_slave:start(Name, NodeConfig) of
          {ok, Node} ->
              {Name, Node};
          Error ->
              ct:fail(Error)
      end
     end,
    Nodes = lists:map(InitializerFun, NodeNames),

    %% Load applications on all of the nodes.
    LoaderFun = fun({_Name, Node}) ->
      ct:pal("Loading applications on node: ~p", [Node]),

      PrivDir = code:priv_dir(?APP),

      NodeDir = filename:join([PrivDir, "lager", Node]),

      %% Manually force sasl loading, and disable the logger.   
      ct:pal("P ~p N ~p", [PrivDir, NodeDir]),
  
      ok = rpc:call(Node, application, load, [sasl]),   

      ok = rpc:call(Node, application, set_env, [sasl, sasl_error_logger, false]),    

      ok = rpc:call(Node, application, start, [sasl]),    

      ok = rpc:call(Node, application, load, [partisan]),   

      ok = rpc:call(Node, application, load, [ishikawa]),    

      ok = rpc:call(Node, application, load, [lager]),   

      ok = rpc:call(Node, application, set_env, [sasl, sasl_error_logger, false]),

      ok = rpc:call(Node, application, set_env, [lager, log_root, NodeDir]),

      ok = rpc:call(Node, ishikawa_config, set, [deliver_locally, true])
   end,
  
  lists:map(LoaderFun, Nodes),

    %% Configure settings.
    ConfigureFun = fun({Name, Node}) ->
      %% Configure the peer service.
      PeerService = proplists:get_value(partisan_peer_service_manager, Options),
      ct:pal("Setting peer service maanger on node ~p to ~p", [Node, PeerService]),
      ok = rpc:call(Node, partisan_config, set,
        [partisan_peer_service_manager, PeerService]),

      MaxActiveSize = proplists:get_value(max_active_size, Options, 5),
      ok = rpc:call(Node, partisan_config, set,
        [max_active_size, MaxActiveSize]),

      Servers = proplists:get_value(servers, Options, []),
      Clients = proplists:get_value(clients, Options, []),

      %% Configure servers.
      case lists:member(Name, Servers) of
        true ->
          ok = rpc:call(Node, partisan_config, set, [tag, server]);
        false ->
          ok
      end,

      %% Configure clients.
      case lists:member(Name, Clients) of
        true ->
          ok = rpc:call(Node, partisan_config, set, [tag, client]);
        false ->
          ok
      end
    end,
    lists:map(ConfigureFun, Nodes),

    ct:pal("Starting nodes."),

    StartFun = fun({_Name, Node}) ->
      %% Start partisan.
      {ok, _} = rpc:call(Node, application, ensure_all_started, [ishikawa])
    end,
    lists:map(StartFun, Nodes),

    ct:pal("Clustering nodes."),
    Manager = proplists:get_value(partisan_peer_service_manager, Options),
    lists:map(fun(Node) -> cluster(Node, Nodes, Manager) end, Nodes),

    ct:pal("Partisan fully initialized."),

    Nodes.

%% @private
codepath() ->
    lists:filter(fun filelib:is_dir/1, code:get_path()).

%% @private
%%
%% We have to cluster each node with all other nodes to compute the
%% correct overlay: for instance, sometimes you'll want to establish a
%% client/server topology, which requires all nodes talk to every other
%% node to correctly compute the overlay.
%%
cluster(Node, Nodes, Manager) when is_list(Nodes) ->
  OtherNodes = case Manager of
    partisan_default_peer_service_manager ->
      Nodes -- [Node];
    partisan_client_server_peer_service_manager ->
      case Node of
        {server, _} ->
          Nodes -- [Node];
        _ ->
          Server = lists:keyfind(server, 1, Nodes),
          [Server]
      end;
    partisan_hyparview_peer_service_manager ->
      case Node of
        {server, _} ->
          [];
        _ ->
          Server = lists:keyfind(server, 1, Nodes),
          [Server]
      end
  end,
  lists:map(fun(OtherNode) -> join(Node, OtherNode) end, OtherNodes).

join({_, Node}, {_, OtherNode}) ->
  PeerPort = rpc:call(OtherNode,
    partisan_config,
    get,
    [peer_port, ?PEER_PORT]),
  ct:pal("Joining node: ~p to ~p at port ~p", [Node, OtherNode, PeerPort]),
  ok = rpc:call(Node,
    partisan_peer_service,
    join,
    [{OtherNode, {127, 0, 0, 1}, PeerPort}]).

%% @private
stop(Nodes) ->
  StopFun = fun({Name, _Node}) ->
    case ct_slave:stop(Name) of
      {ok, _} ->
        ok;
      Error ->
        ct:fail(Error)
    end
  end,
  lists:map(StopFun, Nodes),
  ok.

%% @private
node_list(ClientNumber) ->
  Clients = client_list(ClientNumber),
  [server | Clients].

%% @private
client_list(0) -> [];
client_list(N) -> lists:append(client_list(N - 1),
  [list_to_atom("client_" ++ integer_to_list(N))]).
