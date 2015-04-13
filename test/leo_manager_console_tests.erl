%%======================================================================
%%
%% Leo Manager
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
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
%%======================================================================
-module(leo_manager_console_tests).
-author('Yosuke Hara').

-include("leo_manager.hrl").
-include("tcp_server.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

manager_controller_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun version_/1,
                           fun status_0_/1,
                           fun status_1_/1,
                           fun detach_0_/1, fun detach_1_/1, fun detach_2_/1,
                           fun suspend_0_/1,  fun suspend_1_/1, fun suspend_2_/1,
                           fun resume_0_/1, fun resume_1_/1,
                           fun start_0_/1, fun start_1_/1, fun start_2_/1,
                           fun rebalance_0_/1, fun rebalance_1_/1, fun rebalance_2_/1,
                           fun du_0_/1, fun du_1_/1, fun du_2_/1, fun du_3_/1,
                           fun compact_0_/1, fun compact_1_/1,
                           fun whereis_/1, fun recover_/1,
                           fun purge_0_/1, fun purge_1_/1,
                           fun history_/1,
                           fun help_/1,
                           fun quit_/1
                          ]]}.

setup() ->
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),

    Node0 = list_to_atom("node_0@" ++ Hostname),
    net_kernel:start([Node0, shortnames]),
    {ok, Node1} = slave:start_link(list_to_atom(Hostname), 'node_1'),

    true = rpc:call(Node0, code, add_path, ["../deps/meck/ebin"]),
    true = rpc:call(Node1, code, add_path, ["../deps/meck/ebin"]),

    _ = tcp_server_sup:start_link(),
    _ = leo_manager_console:start_link(
          leo_manager_formatter_text, #tcp_server_params{num_of_listeners = 3}),
    {ok, Sock} = gen_tcp:connect(
                   "127.0.0.1", 10010, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]),
    {Node0, Node1, Sock}.

teardown({_, Node1, _}) ->
    leo_manager_console:stop(),
    meck:unload(),
    net_kernel:stop(),
    slave:stop(Node1),
    timer:sleep(250),
    ok.

%%--------------------------------------------------------------------
%%% TEST FUNCTIONS
%%--------------------------------------------------------------------
version_({_,_,Sock}) ->
    ok = gen_tcp:send(Sock, <<"version\r\n">>),
    catch gen_tcp:close(Sock),
    ok.

status_0_({Node0, Node1, Sock}) ->
    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_nodes_all,
                     fun() ->
                             {ok, [#node_state{node  = Node0,
                                               state = ?STATE_RUNNING}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_gateway_nodes_all,
                     fun() ->
                             {ok, [#node_state{node  = Node1,
                                               state = ?STATE_RUNNING}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_cluster_tbl_conf, [non_strict]),
    ok = meck:expect(leo_cluster_tbl_conf, get,
                     fun() ->
                             {ok, #?SYSTEM_CONF{}}
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, checksum,
                     fun(ring) ->
                             {ok, {12345, 67890}}
                     end),

    ok = meck:new(leo_hex, [non_strict]),
    ok = meck:expect(leo_hex, integer_to_hex,
                     fun(_) ->
                             []
                     end),

    ok = meck:new(leo_manager_api, [non_strict]),
    ok = meck:expect(leo_manager_api, load_system_config,
                     fun() ->
                             ok
                     end),

    Command = "status\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),

    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_manager_mnesia)),
    %% ?assertNotEqual([], meck:history(leo_redundant_manager_api)),

    catch gen_tcp:close(Sock),
    ok.

status_1_({Node0, _, Sock}) ->
    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_cluster_tbl_conf, [non_strict]),
    ok = meck:expect(leo_cluster_tbl_conf, get,
                     fun() ->
                             {ok, #?SYSTEM_CONF{}}
                     end),

    ok = meck:new(leo_storage_api, [non_strict]),
    ok = meck:expect(
           leo_storage_api, get_node_status,
           fun() ->
                   {ok, #cluster_node_status{avs           = [{{'dir',"/var/leofs/vol0/"}, {'num',64}}],
                                             dirs          = [{'log',             "/var/leofs/logs/"},
                                                              {'mnesia',          "/var/leofs/mnesia/"}],
                                             ring_checksum = [{'ring_cur',        "12345"},
                                                              {'ring_cur',        "67890"}],
                                             statistics    = [{'total_mem_usage',  0},
                                                              {'system_mem_usage', 1},
                                                              {'proc_mem_usage',   3},
                                                              {'ets_mem_usage',    5},
                                                              {'num_of_procs',     7}],
                                             version       = "0.0.0"
                                            }}
           end),
    ok = meck:new(leo_hex, [non_strict]),
    ok = meck:expect(leo_hex, integer_to_hex,
                     fun(_) ->
                             []
                     end),

    Command = "status " ++ atom_to_list(Node0) ++ "\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_manager_mnesia)),

    catch gen_tcp:close(Sock),
    ok.


detach_0_({Node0,_, Sock}) ->
    ok = meck:new(leo_cluster_tbl_conf, [non_strict]),
    ok = meck:expect(leo_cluster_tbl_conf, get,
                     fun() ->
                             {ok, #?SYSTEM_CONF{}}
                     end),

    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                     fun(_Node) ->
                             {ok, #node_state{state=?STATE_RUNNING}}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_nodes_by_status,
                     fun(_State) ->
                             {ok, [#node_state{}, #node_state{}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_gateway_nodes_all,
                     fun() ->
                             not_found
                     end),
    ok = meck:expect(leo_manager_mnesia, update_storage_node_status,
                     fun(_State) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, delete_storage_node,
                     fun(_Node) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, has_member,
                     fun(_Node) ->
                             true
                     end),
    ok = meck:expect(leo_redundant_manager_api, checksum,
                     fun(_) ->
                             {ok, {12345, 12345}}
                     end),
    ok = meck:expect(leo_redundant_manager_api, detach,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_redundant_manager_api, get_members,
                     fun() ->
                             {ok, [#member{state = ?STATE_RUNNING,
                                           node =  Node0}]}
                     end),
    ok = meck:expect(leo_redundant_manager_api, reserve, 3, ok),
    ok = meck:expect(leo_redundant_manager_api, reserve, 5, ok),
    ok = meck:expect(leo_redundant_manager_api, reserve, 6, ok),

    Command = "detach " ++ atom_to_list(Node0) ++ "\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_manager_mnesia)),

    {ok, Res} = gen_tcp:recv(Sock, 0, 1000),
    ?assertNotEqual(<<"OK\r\n">>, Res), %% ERROR

    catch gen_tcp:close(Sock),
    ok.

detach_1_({Node0, _, Sock}) ->
    ok = meck:new(leo_cluster_tbl_conf, [non_strict]),
    ok = meck:expect(leo_cluster_tbl_conf, get,
                     fun() ->
                             {ok, #?SYSTEM_CONF{n = 2}}
                     end),

    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                     fun(_Node) ->
                             {ok, #node_state{state=?STATE_RUNNING}}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_nodes_by_status,
                     fun(?STATE_DETACHED) ->
                             {ok, [#node_state{}]};
                        (?STATE_RUNNING) ->
                             {ok, [#node_state{}, #node_state{}, #node_state{}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, delete_storage_node,
                     fun(_Node) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, update_storage_node_status,
                     fun(_) ->
                             ok
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, has_member,
                     fun(_Node) ->
                             true
                     end),
    ok = meck:expect(leo_redundant_manager_api, checksum,
                     fun(_) ->
                             {ok, {12345, 67890}} %% >> NOT Match
                     end),
    ok = meck:expect(leo_redundant_manager_api, detach,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_redundant_manager_api, reserve,
                     fun(_,_,_) ->
                             ok
                     end),
    ok = meck:expect(leo_redundant_manager_api, reserve,
                     fun(_,_,_,_,_) ->
                             ok
                     end),
    ok = meck:expect(leo_redundant_manager_api, reserve,
                     fun(_,_,_,_,_,_) ->
                             ok
                     end),

    Command = "detach " ++ atom_to_list(Node0) ++ "\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_manager_mnesia)),
    ?assertNotEqual([], meck:history(leo_redundant_manager_api)),

    {ok, Res} = gen_tcp:recv(Sock, 0, 1000),
    ?assertEqual(true, string:str(binary_to_list(Res), "OK") > 0),

    catch gen_tcp:close(Sock),
    ok.

detach_2_({Node0,_, Sock}) ->
    ok = meck:new(leo_cluster_tbl_conf, [non_strict]),
    ok = meck:expect(leo_cluster_tbl_conf, get,
                     fun() ->
                             {ok, #?SYSTEM_CONF{}}
                     end),

    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                     fun(_Node) ->
                             {ok, #node_state{state=?STATE_RUNNING}}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_nodes_by_status,
                     fun(_State) ->
                             {ok, [#node_state{}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, delete_storage_node,
                     fun(_Node) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, update_storage_node_status,
                     fun(_State) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                     fun(_Node) ->
                             {ok, #node_state{}}
                     end),
    ok = meck:expect(leo_manager_mnesia, delete_storage_node,
                     fun(_Node) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    Command = "detach " ++ atom_to_list(Node0) ++ "\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),
    ?assertNotEqual([], meck:history(leo_manager_mnesia)),

    catch gen_tcp:close(Sock),
    ok.


suspend_0_({Node0, _, Sock}) ->
    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_gateway_nodes_all,
                     fun() ->
                             {ok, [#node_state{}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_nodes_all,
                     fun() ->
                             {ok, [#node_state{}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, update_storage_node_status,
                     fun(_, _State) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                     fun(_Node) ->
                             {ok, #node_state{node  = Node0,
                                              state = ?STATE_RUNNING}}
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, has_member,
                     fun(_Node) ->
                             true
                     end),
    ok = meck:expect(leo_redundant_manager_api, suspend,
                     fun(_Node) ->
                             ok
                     end),
    ok = meck:expect(leo_redundant_manager_api, get_members,
                     fun() ->
                             {ok, [#member{state = ?STATE_RUNNING,
                                           node =  Node0}]}
                     end),

    Command = "suspend " ++ atom_to_list(Node0) ++ "\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_manager_mnesia)),
    ?assertNotEqual([], meck:history(leo_redundant_manager_api)),

    {ok, Res} = gen_tcp:recv(Sock, 0, 1000),
    ?assertEqual(<<"OK\r\n">>, Res),

    catch gen_tcp:close(Sock),
    ok.

suspend_1_({Node0, _, Sock}) ->
    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_gateway_nodes_all,
                     fun() ->
                             {ok, [#node_state{}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_nodes_all,
                     fun() ->
                             {ok, [#node_state{}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, update_storage_node_status,
                     fun(_, _State) ->
                             {error, []} %% error!
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                     fun(_Node) ->
                             {ok, #node_state{node  = Node0,
                                              state = ?STATE_RUNNING}}
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, has_member,
                     fun(_Node) ->
                             true
                     end),
    ok = meck:expect(leo_redundant_manager_api, suspend,
                     fun(_Node, _Clock) ->
                             ok
                     end),

    Command = "suspend " ++ atom_to_list(Node0) ++ "\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_manager_mnesia)),

    {ok, Res} = gen_tcp:recv(Sock, 0, 1000),
    ?assertEqual(true, string:str(binary_to_list(Res), "[ERROR]") > 0),

    catch gen_tcp:close(Sock),
    ok.

suspend_2_({Node0, _, Sock}) ->
    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_gateway_nodes_all,
                     fun() ->
                             {ok, [#node_state{}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_nodes_all,
                     fun() ->
                             {ok, [#node_state{}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, update_storage_node_status,
                     fun(_, _State) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                     fun(_Node) ->
                             {ok, #node_state{node  = Node0,
                                              state = ?STATE_RUNNING}}
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, has_member,
                     fun(_Node) ->
                             true
                     end),
    ok = meck:expect(leo_redundant_manager_api, suspend,
                     fun(_Node) ->
                             {error, []} %% error!
                     end),

    Command = "suspend " ++ atom_to_list(Node0) ++ "\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_manager_mnesia)),
    ?assertNotEqual([], meck:history(leo_redundant_manager_api)),

    {ok, Res} = gen_tcp:recv(Sock, 0, 1000),
    ?assertEqual(true, string:str(binary_to_list(Res), "[ERROR]") > 0),

    catch gen_tcp:close(Sock),
    ok.


resume_0_({Node0, _Node1, Sock}) ->
    ok = meck:new(leo_cluster_tbl_conf, [non_strict]),
    ok = meck:expect(leo_cluster_tbl_conf, get,
                     fun() ->
                             {ok, #?SYSTEM_CONF{}}
                     end),

    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                     fun(_Node) ->
                             {ok, #node_state{node  = Node0,
                                              state = ?STATE_SUSPEND}} %% SUSPEND
                     end),
    ok = meck:expect(leo_manager_mnesia, get_gateway_nodes_all,
                     fun() ->
                             not_found
                     end),
    ok = meck:expect(leo_manager_mnesia, update_storage_node_status,
                     fun(_, _State) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, update_storage_node_status,
                     fun(_State) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, has_member,
                     fun(_Node) ->
                             true
                     end),
    ok = meck:expect(leo_redundant_manager_api, update_member_by_node,
                     fun(_Node, _State) ->
                             ok
                     end),
    ok = meck:expect(leo_redundant_manager_api, get_members,
                     fun() ->
                             {ok, [#member{node  = Node0,
                                           state = ?STATE_RUNNING}]}
                     end),
    ok = meck:expect(leo_redundant_manager_api, get_members,
                     fun(_) ->
                             {ok, [#member{node  = Node0,
                                           state = ?STATE_RUNNING}]}
                     end),
    ok = meck:expect(leo_redundant_manager_api, synchronize,
                     fun(?SYNC_TARGET_BOTH,_,_) ->
                             {ok, [{?CHECKSUM_MEMBER, {51234567890,51234567890}},
                                   {?CHECKSUM_RING,   {71234567890,71234567890}}
                                  ]}
                     end),
    ok = meck:expect(leo_redundant_manager_api, synchronize,
                     fun(Target, _) ->
                             case Target of
                                 ?SYNC_TARGET_RING_CUR ->
                                     {ok, [{?CHECKSUM_MEMBER, {51234567890,51234567890}},
                                           {?CHECKSUM_RING,   {71234567890,71234567890}}
                                          ]};
                                 ?SYNC_TARGET_RING_PREV ->
                                     {ok, [{?CHECKSUM_MEMBER, {51234567890,51234567890}},
                                           {?CHECKSUM_RING,   {71234567890,71234567890}}
                                          ]};
                                 ?SYNC_TARGET_MEMBER ->
                                     {ok, {51234567890,51234567890}}
                             end
                     end),
    ok = meck:expect(leo_redundant_manager_api, update_members,
                     fun(_Members) ->
                             ok
                     end),
    ok = meck:expect(leo_redundant_manager_api, checksum,
                     fun(_) ->
                             {ok, {1,1}}
                     end),

    Command = "resume " ++ atom_to_list(Node0) ++ "\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_manager_mnesia)),
    ?assertNotEqual([], meck:history(leo_redundant_manager_api)),

    {ok, Res} = gen_tcp:recv(Sock, 0, 1000),
    ?assertEqual(<<"OK\r\n">>, Res),

    catch gen_tcp:close(Sock),
    ok.

resume_1_({Node0,_, Sock}) ->
    ok = meck:new(leo_cluster_tbl_conf, [non_strict]),
    ok = meck:expect(leo_cluster_tbl_conf, get,
                     fun() ->
                             {ok, #?SYSTEM_CONF{}}
                     end),

    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                     fun(_Node) ->
                             {ok, #node_state{node  = Node0,
                                              state = ?STATE_RUNNING}} %% NOT SUSPEND
                     end),
    ok = meck:expect(leo_manager_mnesia, update_storage_node_status,
                     fun(_, _State) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, update_storage_node_status,
                     fun(_State) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, has_member,
                     fun(_Node) ->
                             true
                     end),
    ok = meck:expect(leo_redundant_manager_api, update_member_by_node,
                     fun(_Node, _State) ->
                             ok
                     end),
    ok = meck:expect(leo_redundant_manager_api, get_members,
                     fun() ->
                             {ok, [#member{node  = Node0,
                                           state = ?STATE_RUNNING}]}
                     end),
    ok = meck:expect(leo_redundant_manager_api, synchronize,
                     fun(?SYNC_TARGET_BOTH,_,_) ->
                             {ok, [{?CHECKSUM_MEMBER, {123,123}},
                                   {?CHECKSUM_RING,   {456,456}}
                                  ]}
                     end),
    ok = meck:expect(leo_redundant_manager_api, synchronize,
                     fun(Target, _) ->
                             case Target of
                                 ?SYNC_TARGET_RING_CUR ->
                                     {ok, [{?CHECKSUM_MEMBER, {123,123}},
                                           {?CHECKSUM_RING,   {456,456}}
                                          ]};
                                 ?SYNC_TARGET_RING_PREV ->
                                     {ok, [{?CHECKSUM_MEMBER, {123,123}},
                                           {?CHECKSUM_RING,   {456,456}}
                                          ]};
                                 ?SYNC_TARGET_MEMBER ->
                                     {ok, {123,123}}
                             end
                     end),
    ok = meck:expect(leo_redundant_manager_api, update_members,
                     fun(_Members) ->
                             ok
                     end),

    Command = "resume " ++ atom_to_list(Node0) ++ "\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_manager_mnesia)),

    {ok, Res} = gen_tcp:recv(Sock, 0, 1000),
    ?assertEqual(true, string:str(binary_to_list(Res), "[ERROR]") > 0),

    catch gen_tcp:close(Sock),
    ok.


start_0_({Node0, _, Sock}) ->
    ok = meck:new(leo_cluster_tbl_conf, [non_strict]),
    ok = meck:expect(leo_cluster_tbl_conf, get,
                     fun() ->
                             {ok, #?SYSTEM_CONF{}}
                     end),

    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_nodes_by_status,
                     fun(?STATE_RUNNING) ->
                             not_found; %% >> ?STATUS_STOP
                        (?STATE_ATTACHED) ->
                             {ok, [#node_state{}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, update_storage_node_status,
                     fun(_Mode, _State) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, create,
                     fun() ->
                             {ok, [#member{node = Node0}], {12345,12345}}
                     end),

    Command = "start\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_manager_mnesia)),
    %% ?assertNotEqual([], meck:history(leo_redundant_manager_api)),

    catch gen_tcp:close(Sock),
    ok.

start_1_({Node0, _, Sock}) ->
    ok = meck:new(leo_cluster_tbl_conf, [non_strict]),
    ok = meck:expect(leo_cluster_tbl_conf, get,
                     fun() ->
                             {ok, #?SYSTEM_CONF{}}
                     end),

    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_nodes_by_status,
                     fun(?STATE_RUNNING) ->
                             {ok, [#node_state{}]}; %% >> ?STATUS_RUNNING
                        (?STATE_ATTACHED) ->
                             {ok, [#node_state{}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, update_storage_node_status,
                     fun(_Mode, _State) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, create,
                     fun() ->
                             {ok, [#member{node = Node0}], {12345,12345}}
                     end),

    ok = meck:new(leo_storage_api, [non_strict]),
    ok = meck:expect(leo_storage_api, start,
                     fun(_) ->
                             {ok, {Node0, {12345,12345}}}
                     end),

    Command = "start\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_manager_mnesia)),
    ?assertEqual([], meck:history(leo_redundant_manager_api)),
    ?assertEqual([], meck:history(leo_storage_api)),

    %% {ok, Res} = gen_tcp:recv(Sock, 0, 1000),
    %% ?assertEqual(true, string:str(binary_to_list(Res), "[ERROR]") > 0),

    catch gen_tcp:close(Sock),
    ok.

start_2_({Node0, _, Sock}) ->
    ok = meck:new(leo_cluster_tbl_conf, [non_strict]),
    ok = meck:expect(leo_cluster_tbl_conf, get,
                     fun() ->
                             {ok, #?SYSTEM_CONF{}}
                     end),

    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_nodes_by_status,
                     fun(?STATE_RUNNING) ->
                             not_found; %% >> ?STATUS_STOP
                        (?STATE_ATTACHED) ->
                             {ok, []}   %% >> NOT_ATTACHED
                     end),
    ok = meck:expect(leo_manager_mnesia, update_storage_node_status,
                     fun(_Mode, _State) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, create,
                     fun() ->
                             {ok, [#member{node = Node0}], {12345,12345}}
                     end),

    ok = meck:new(leo_storage_api, [non_strict]),
    ok = meck:expect(leo_storage_api, start,
                     fun(_) ->
                             {ok, {Node0, {12345,12345}}}
                     end),

    Command = "start\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_manager_mnesia)),
    ?assertEqual([], meck:history(leo_redundant_manager_api)),
    ?assertEqual([], meck:history(leo_storage_api)),

    %% {ok, Res} = gen_tcp:recv(Sock, 0, 1000),
    %% ?assertEqual(true, string:str(binary_to_list(Res), "[ERROR]") > 0),

    catch gen_tcp:close(Sock),
    ok.


rebalance_0_({_Node0, _, Sock}) ->
    ok = meck:new(leo_cluster_tbl_conf, [non_strict]),
    ok = meck:expect(leo_cluster_tbl_conf, get,
                     fun() ->
                             {ok, #?SYSTEM_CONF{}}
                     end),

    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, checksum,
                     fun(_) ->
                             {ok, {12345, 67890}}
                     end),
    ok = meck:expect(leo_redundant_manager_api, get_members,
                     fun(_) ->
                             {error, not_found}
                     end),

    %% Command = "rebalance\r\n",
    ok = gen_tcp:send(Sock, <<"rebalance\r\n">>),
    ?debugVal(ok),
    timer:sleep(100),

    {ok, Res} = gen_tcp:recv(Sock, 0, 1000),
    ?assertMatch(true, string:str(binary_to_list(Res), "[ERROR]") > 0),

    catch gen_tcp:close(Sock),
    ok.

rebalance_1_({Node0, _, Sock}) ->
    ok = meck:new(leo_cluster_tbl_conf, [non_strict]),
    ok = meck:expect(leo_cluster_tbl_conf, get,
                     fun() ->
                             {ok, #?SYSTEM_CONF{}}
                     end),

    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, checksum,
                     fun(_) ->
                             {ok, {12345, 67890}}
                     end),
    ok = meck:expect(leo_redundant_manager_api, get_members,
                     fun(_) ->
                             {ok, [#member{node  = Node0,
                                           state = ?STATE_ATTACHED}]} %% 1-node
                     end),

    Command = "rebalance\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_redundant_manager_api)),

    {ok, Res} = gen_tcp:recv(Sock, 0, 1000),
    ?assertEqual(true, string:str(binary_to_list(Res), "[ERROR]") > 0),

    catch gen_tcp:close(Sock),
    ok.

rebalance_2_({Node0, Node1, Sock}) ->
    ok = meck:new(leo_cluster_tbl_conf, [non_strict]),
    ok = meck:expect(leo_cluster_tbl_conf, get,
                     fun() ->
                             {ok, #?SYSTEM_CONF{}}
                     end),

    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, update_storage_node_status,
                     fun(_, _) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, checksum,
                     fun(_) ->
                             {ok, {12345, 67890}}
                     end),
    ok = meck:expect(leo_redundant_manager_api, get_members,
                     fun(_) ->
                             {ok, [#member{node  = Node0, state = ?STATE_ATTACHED},
                                   #member{node  = Node1, state = ?STATE_RUNNING }]} %% 2-node
                     end),
    ok = meck:expect(leo_redundant_manager_api, rebalance,
                     fun() ->
                             {ok, [[{vnode_id, 255}, {src, Node0}, {dest, Node1}]]}
                     end),
    ok = meck:expect(leo_redundant_manager_api, synchronize,
                     fun(?SYNC_TARGET_BOTH,_,_) ->
                             {ok, [{?CHECKSUM_MEMBER, {123,123}},
                                   {?CHECKSUM_RING,   {456,456}}
                                  ]}
                     end),
    ok = meck:expect(leo_redundant_manager_api, synchronize,
                     fun(Target, _) ->
                             case Target of
                                 ?SYNC_TARGET_RING_CUR ->
                                     {ok, [{?CHECKSUM_MEMBER, {123,123}},
                                           {?CHECKSUM_RING,   {456,456}}
                                          ]};
                                 ?SYNC_TARGET_RING_PREV ->
                                     {ok, [{?CHECKSUM_MEMBER, {123,123}},
                                           {?CHECKSUM_RING,   {456,456}}
                                          ]};
                                 ?SYNC_TARGET_MEMBER ->
                                     {ok, {123,123}}
                             end
                     end),
    ok = meck:expect(leo_redundant_manager_api, get_ring,
                     fun(_) ->
                             {ok, []}
                     end),

    ok = rpc:call(Node1, meck, new,    [leo_redundant_manager_api, [no_link, non_strict]]),
    ok = rpc:call(Node1, meck, expect, [leo_redundant_manager_api, synchronize,
                                        fun(?SYNC_TARGET_BOTH,_,_) ->
                                                {ok, [{?CHECKSUM_MEMBER, {123,123}},
                                                      {?CHECKSUM_RING,   {456,456}}
                                                     ]}
                                        end]),
    ok = rpc:call(Node1, meck, expect, [leo_redundant_manager_api, synchronize,
                                        fun(Target, _) ->
                                                case Target of
                                                    ?SYNC_TARGET_RING_CUR ->
                                                        {ok, [{?CHECKSUM_MEMBER, {123,123}},
                                                              {?CHECKSUM_RING,   {456,456}}
                                                             ]};
                                                    ?SYNC_TARGET_RING_PREV ->
                                                        {ok, [{?CHECKSUM_MEMBER, {123,123}},
                                                              {?CHECKSUM_RING,   {456,456}}
                                                             ]};
                                                    ?SYNC_TARGET_MEMBER ->
                                                        {ok, {123,123}}
                                                end
                                        end]),

    Command = "rebalance\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_manager_mnesia)),
    ?assertNotEqual([], meck:history(leo_redundant_manager_api)),

    %% @TODO
    %% {ok, Res} = gen_tcp:recv(Sock, 0, 1000),
    %% ?assertEqual(<<"OK\r\n">>, Res),

    catch gen_tcp:close(Sock),
    ok.


du_0_({Node0,_, Sock}) ->
    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                     fun(_) ->
                             {ok, #node_state{}}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_object_storage_api, [non_strict]),
    ok = meck:expect(leo_object_storage_api, stats,
                     fun() ->
                             not_found
                     end),

    Command = "du " ++ atom_to_list(Node0) ++ "\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_object_storage_api)),
    catch gen_tcp:close(Sock),
    ok.

du_1_({Node0,_, Sock}) ->
    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                     fun(_) ->
                             {ok, #node_state{}}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_object_storage_api, [non_strict]),
    ok = meck:expect(leo_object_storage_api, stats,
                     fun() ->
                             {ok, []}
                     end),

    Command = "du " ++ atom_to_list(Node0) ++ "\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_object_storage_api)),
    catch gen_tcp:close(Sock),
    ok.

du_2_({Node0,_, Sock}) ->
    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                     fun(_) ->
                             {ok, #node_state{}}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_object_storage_api, [non_strict]),
    ok = meck:expect(leo_object_storage_api, stats,
                     fun() ->
                             {ok, [{ok, #storage_stats{}},
                                   {ok, #storage_stats{}},
                                   {ok, #storage_stats{}}]}
                     end),

    Command = "du " ++ atom_to_list(Node0) ++ "\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_object_storage_api)),
    catch gen_tcp:close(Sock),
    ok.

du_3_({Node0,_, Sock}) ->
    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                     fun(_) ->
                             {ok, #node_state{}}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_object_storage_api, [non_strict]),
    ok = meck:expect(leo_object_storage_api, stats,
                     fun() ->
                             {ok, [{ok, #storage_stats{}},
                                   {ok, #storage_stats{}},
                                   {ok, #storage_stats{}}]}
                     end),

    Command = "du summary " ++ atom_to_list(Node0) ++ "\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    catch gen_tcp:close(Sock),
    ok.


compact_0_({Node0, _, Sock}) ->
    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                     fun(_Node) ->
                             {ok, #node_state{state = ?STATE_RUNNING}}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_manager_api, [non_strict]),
    ok = meck:expect(leo_manager_api, compact,
                     fun(_,_,_,_) ->
                             {error, disk_error}
                     end),

    Command = "compact start " ++ atom_to_list(Node0) ++ " 1\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    %% ?assertEqual(1, length(meck:history(leo_manager_api))),
    catch gen_tcp:close(Sock),
    ok.

compact_1_({Node0, _, Sock}) ->
    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_storage_node_by_nameo,
                     fun(_) ->
                             {ok, #node_state{state = ?STATE_RUNNING}}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_manager_api, [non_strict]),
    ok = meck:expect(leo_manager_api, compact,
                     fun(_, _, _, _) ->
                             ok
                     end),

    Command = "compact start " ++ atom_to_list(Node0) ++ " 1 1\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertEqual(1, length(meck:history(leo_manager_api))),
    catch gen_tcp:close(Sock),
    ok.


whereis_({Node0, _Node1, Sock}) ->
    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, checksum,
                     fun(ring) ->
                             1
                     end),
    ok = meck:expect(leo_redundant_manager_api, get_redundancies_by_key,
                     fun(_Key) ->
                             {ok, #redundancies{id    = 1,
                                                nodes = [#redundant_node{node      = Node0,
                                                                         available = true}]}}
                     end),

    ok = meck:new(leo_utils, [non_strict]),
    ok = meck:expect(leo_utils, date_format,
                     fun(_) ->
                             ""
                     end),
    ok = meck:new(leo_hex, [non_strict]),
    ok = meck:expect(leo_hex, integer_to_hex,
                     fun(_) ->
                             0
                     end),

    ok = meck:new(leo_object_storage_api, [non_strict]),
    ok = meck:expect(leo_object_storage_api, head,
                     fun({AddrId, _Key}) ->
                             {ok, #?METADATA{addr_id   = AddrId,
                                             dsize     = 1,
                                             clock     = 2,
                                             timestamp = 3,
                                             checksum  = 4,
                                             del       = 0}}
                     end),

    Command = "whereis air/on/g/string\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_redundant_manager_api)),
    catch gen_tcp:close(Sock),
    ok.


recover_({Node0, _Node1, Sock}) ->
    ok = meck:new(leo_cluster_tbl_conf, [non_strict]),
    ok = meck:expect(leo_cluster_tbl_conf, get,
                     fun() ->
                             {ok, #?SYSTEM_CONF{}}
                     end),

    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_redundant_manager_api, [non_strict]),
    ok = meck:expect(leo_redundant_manager_api, checksum,
                     fun(ring) ->
                             1
                     end),

    ok = meck:expect(leo_redundant_manager_api, get_redundancies_by_key,
                     fun(_Key) ->
                             {ok, #redundancies{id    = 1,
                                                nodes = [#redundant_node{node      = Node0,
                                                                         available = true}]}}
                     end),
    ok = meck:expect(leo_redundant_manager_api, checksum,
                     fun(_) ->
                             {ok, {1,1}}
                     end),
    ok = meck:expect(leo_redundant_manager_api, get_members,
                     fun() ->
                             {ok, [#member{node  = Node0,
                                           state = ?STATE_RUNNING}]}
                     end),

    ok = meck:new(leo_utils, [non_strict]),
    ok = meck:expect(leo_utils, date_format,
                     fun(_) ->
                             ""
                     end),
    ok = meck:new(leo_hex, [non_strict]),
    ok = meck:expect(leo_hex, integer_to_hex,
                     fun(_) ->
                             0
                     end),

    ok = rpc:call(Node0, meck, new,    [leo_storage_api, [no_link, non_strict]]),
    ok = rpc:call(Node0, meck, expect, [leo_redundant_manager_api, synchronize,
                                        fun(?SYNC_TARGET_BOTH,_,_) ->
                                                {ok, [{?CHECKSUM_MEMBER, {123,123}},
                                                      {?CHECKSUM_RING,   {456,456}}
                                                     ]}
                                        end]),
    ok = rpc:call(Node0, meck, expect, [leo_redundant_manager_api, synchronize,
                                        fun(Target, _) ->
                                                case Target of
                                                    ?SYNC_TARGET_RING_CUR ->
                                                        {ok, [{?CHECKSUM_MEMBER, {123,123}},
                                                              {?CHECKSUM_RING,   {456,456}}
                                                             ]};
                                                    ?SYNC_TARGET_RING_PREV ->
                                                        {ok, [{?CHECKSUM_MEMBER, {123,123}},
                                                              {?CHECKSUM_RING,   {456,456}}
                                                             ]};
                                                    ?SYNC_TARGET_MEMBER ->
                                                        {ok, {123,123}}
                                                end
                                        end]),

    Command1 = "recover file air/on/g/string\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command1)),
    timer:sleep(100),

    Command2 = "recover node " ++ atom_to_list(Node0) ++ "\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command2)),
    timer:sleep(100),

    Command3 = "recover ring " ++ atom_to_list(Node0) ++ "\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command3)),
    timer:sleep(100),

    catch gen_tcp:close(Sock),
    ok.


purge_0_({Node0, _, Sock}) ->
    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_gateway_nodes_all,
                     fun() ->
                             {ok, [#node_state{node  = Node0,
                                               state = ?STATE_RUNNING}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_gateway_api, [non_strict]),
    ok = meck:expect(leo_gateway_api, purge,
                     fun(_Path) ->
                             ok
                     end),

    Command = "purge air/on/g/string\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_manager_mnesia)),
    ?assertNotEqual([], meck:history(leo_gateway_api)),
    catch gen_tcp:close(Sock),
    ok.

purge_1_({Node0, _, Sock}) ->
    ok = meck:new(leo_manager_mnesia, [non_strict]),
    ok = meck:expect(leo_manager_mnesia, insert_history,
                     fun(_) ->
                             ok
                     end),
    ok = meck:expect(leo_manager_mnesia, get_gateway_nodes_all,
                     fun() ->
                             {ok, [#node_state{node  = Node0,
                                               state = ?STATE_RUNNING}]}
                     end),
    ok = meck:expect(leo_manager_mnesia, get_available_command_by_name,
                     fun(Cmd) ->
                             {ok, [#cmd_state{name = Cmd,
                                              available = true}]}
                     end),

    ok = meck:new(leo_gateway_api, [non_strict]),
    ok = meck:expect(leo_gateway_api, purge,
                     fun(_Path) ->
                             ok
                     end),

    Command = "purge /\r\n",
    ok = gen_tcp:send(Sock, list_to_binary(Command)),
    timer:sleep(100),

    ?assertNotEqual([], meck:history(leo_manager_mnesia)),
    ?assertNotEqual([], meck:history(leo_gateway_api)),
    catch gen_tcp:close(Sock),
    ok.

history_({_,_,Sock}) ->
    ok = gen_tcp:send(Sock, <<"history\r\n">>),
    catch gen_tcp:close(Sock),
    ok.

help_({_,_,Sock}) ->
    ok = gen_tcp:send(Sock, <<"help\r\n">>),
    catch gen_tcp:close(Sock),
    ok.

quit_({_,_,Sock}) ->
    ok = gen_tcp:send(Sock, <<"quit\r\n">>),
    catch gen_tcp:close(Sock),
    ok.

-endif.
