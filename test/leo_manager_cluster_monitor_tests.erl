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
-module(leo_manager_cluster_monitor_tests).
-author('Yosuke Hara').

-include("leo_manager.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

manager_cluster_monitor_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun all_/1
                          ]]}.

setup() ->
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),
    Node = list_to_atom("node_0@" ++ Hostname),
    net_kernel:start([Node, shortnames]),
    Node.

teardown(_) ->
    net_kernel:stop(),
    ok.

%%--------------------------------------------------------------------
%%% TEST FUNCTIONS
%%--------------------------------------------------------------------
all_(Node) ->
    meck:expect(leo_manager_api, get_system_status,
                fun() ->
                        ?STATE_STOP
                end),
    meck:expect(leo_manager_api, attach, 4, ok),
    meck:expect(leo_manager_api, attach, 5, ok),
    meck:expect(leo_manager_api, distribute_members, 1, ok),
    meck:expect(leo_manager_api, synchronize, 2, ok),

    meck:new(leo_storage_api, [non_strict]),
    meck:expect(leo_storage_api, register_in_monitor, fun(again) ->
                                                              ok
                                                      end),
    meck:new(leo_gateway_api, [non_strict]),
    meck:expect(leo_gateway_api, register_in_monitor, fun(again) ->
                                                              ok
                                                      end),

    meck:new(leo_manager_mnesia, [non_strict]),
    meck:expect(leo_manager_mnesia, get_gateway_nodes_all,
                fun() ->
                        {ok, [#node_state{node = Node, state=?STATE_RUNNING}]}
                end),
    meck:expect(leo_manager_mnesia, get_storage_nodes_all,
                fun() ->
                        {ok, [#node_state{node = Node, state=?STATE_RUNNING},
                              #node_state{node = 'storage_0@127.0.0.1',state=?STATE_DETACHED},
                              #node_state{node = 'storage_1@127.0.0.1',state=?STATE_SUSPEND},
                              #node_state{node = 'storage_2@127.0.0.1',state=?STATE_STOP},
                              #node_state{node = 'storage_3@127.0.0.1',state=?STATE_STOP}
                             ]}
                end),
    meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                fun('storage_1@127.0.0.1') -> {ok, #node_state{state = ?STATE_SUSPEND}};
                   ('storage_2@127.0.0.1') -> {ok, #node_state{state = ?STATE_STOP}};
                   ('storage_3@127.0.0.1') -> {ok, #node_state{state = ?STATE_STOP}};
                   ('storage_5@127.0.0.1') -> {ok, #node_state{state = ?STATE_ATTACHED}};
                   ('storage_6@127.0.0.1') -> {ok, #node_state{state = ?STATE_DETACHED}};
                   ('storage_7@127.0.0.1') -> {ok, #node_state{state = ?STATE_RESTARTED}};
                   ('storage_n@127.0.0.1') -> not_found;
                   (_) ->
                        {ok, #node_state{state = ?STATE_RUNNING}}
                end),
    meck:expect(leo_manager_mnesia, get_gateway_node_by_name,
                fun(_) ->
                        {ok, #node_state{state = ?STATE_RUNNING}}
                end),
    meck:expect(leo_manager_mnesia, update_storage_node_status,
                fun(_,_) ->
                        ok
                end),
    meck:expect(leo_manager_mnesia, get_system_config,
                fun() ->
                        {ok, #?SYSTEM_CONF{}}
                end),

    meck:new(leo_redundant_manager_api, [non_strict]),
    meck:expect(leo_redundant_manager_api, update_member_by_node, 3, ok),



    {ok, _Pid} = leo_manager_cluster_monitor:start_link(),
    timer:sleep(1000),

    Res0 = leo_manager_cluster_monitor:register(first, self(), Node, ?PERSISTENT_NODE),
    Res1 = leo_manager_cluster_monitor:register(first, self(), Node, ?PERSISTENT_NODE),
    Res2 = leo_manager_cluster_monitor:register(first, self(), 'gateway_0@127.0.0.1', ?WORKER_NODE),
    Res3 = leo_manager_cluster_monitor:register(first, self(), 'storage_1@127.0.0.1', ?PERSISTENT_NODE),
    Res4 = leo_manager_cluster_monitor:register(first, self(), 'storage_2@127.0.0.1', ?PERSISTENT_NODE),
    Res5 = leo_manager_cluster_monitor:register(first, self(), 'storage_3@127.0.0.1', ?PERSISTENT_NODE),
    Res6 = leo_manager_cluster_monitor:register(first, self(), 'storage_5@127.0.0.1', ?PERSISTENT_NODE),
    Res7 = leo_manager_cluster_monitor:register(first, self(), 'storage_6@127.0.0.1', ?PERSISTENT_NODE),
    Res8 = leo_manager_cluster_monitor:register(first, self(), 'storage_7@127.0.0.1', ?PERSISTENT_NODE),
    Res9 = leo_manager_cluster_monitor:register(first, self(), 'storage_n@127.0.0.1', ?PERSISTENT_NODE),
    ?debugVal(Res9),

    ?assertEqual({ok,ok,ok,ok,ok,ok,ok,ok,ok}, {Res0,Res1,Res2,Res3,Res4,Res5,Res6,Res7,Res8}),
    leo_manager_cluster_monitor:stop(),
    meck:unload(),
    ok.

-endif.
