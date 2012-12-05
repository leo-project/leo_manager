%%======================================================================
%% LeoFS Manager - TEST Case
%% @author yosuke hara
%% @doc
%% @end
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
    meck:new(leo_manager_api),
    meck:expect(leo_manager_api, get_nodes,
                fun() ->
                        {ok, [{storage, 'storage_0@127.0.0.1', ?STATE_DETACHED},
                              {storage, 'storage_1@127.0.0.1', ?STATE_SUSPEND},
                              {storage, 'storage_2@127.0.0.1', ?STATE_DOWNED},
                              {storage, 'storage_3@127.0.0.1', ?STATE_STOP},
                              {storage, Node,                  ?STATE_RUNNING},
                              {gateway, Node,                  ?STATE_RUNNING}
                             ]}
                end),
    meck:expect(leo_manager_api, get_system_status,
                fun() ->
                        ?STATE_STOP
                end),
    meck:expect(leo_manager_api, attach, 1, ok),
    meck:expect(leo_manager_api, distribute_members, 2, ok),

    meck:new(leo_storage_api),
    meck:expect(leo_storage_api, register_in_monitor, fun(again) ->
                                                              ok
                                                      end),
    meck:new(leo_gateway_api),
    meck:expect(leo_gateway_api, register_in_monitor, fun(again) ->
                                                              ok
                                                      end),

    meck:new(leo_manager_mnesia),
    meck:expect(leo_manager_mnesia, get_storage_node_by_name,
                fun('storage_1@127.0.0.1') -> {ok, [#node_state{state = ?STATE_SUSPEND}]};
                   ('storage_2@127.0.0.1') -> {ok, [#node_state{state = ?STATE_DOWNED}]};
                   ('storage_3@127.0.0.1') -> {ok, [#node_state{state = ?STATE_STOP}]};
                   ('storage_5@127.0.0.1') -> {ok, [#node_state{state = ?STATE_ATTACHED}]};
                   ('storage_6@127.0.0.1') -> {ok, [#node_state{state = ?STATE_DETACHED}]};
                   ('storage_7@127.0.0.1') -> {ok, [#node_state{state = ?STATE_RESTARTED}]};
                   ('storage_n@127.0.0.1') -> not_found;
                   (_) ->
                        {ok, [#node_state{state = ?STATE_RUNNING}]}
                end),
    meck:expect(leo_manager_mnesia, get_gateway_node_by_name,
                fun(_) ->
                        {ok, [#node_state{state = ?STATE_RUNNING}]}
                end),
    meck:expect(leo_manager_mnesia, update_storage_node_status,
                fun(_,_) ->
                        ok
                end),
    meck:expect(leo_manager_mnesia, get_system_config,
                fun() ->
                        {ok, #system_conf{}}
                end),

    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, update_member_by_node, 3, ok),



    {ok, _Pid} = leo_manager_cluster_monitor:start_link(),
    timer:sleep(1000),

    ?assertNotEqual([], meck:history(leo_storage_api)),
    ?assertNotEqual([], meck:history(leo_gateway_api)),

    Res0 = leo_manager_cluster_monitor:register(first, self(), Node, storage),
    Res1 = leo_manager_cluster_monitor:register(first, self(), Node, storage),
    Res2 = leo_manager_cluster_monitor:register(first, self(), 'gateway_0@127.0.0.1', gateway),
    Res3 = leo_manager_cluster_monitor:register(first, self(), 'storage_1@127.0.0.1', storage),
    Res4 = leo_manager_cluster_monitor:register(first, self(), 'storage_2@127.0.0.1', storage),
    Res5 = leo_manager_cluster_monitor:register(first, self(), 'storage_3@127.0.0.1', storage),
    Res6 = leo_manager_cluster_monitor:register(first, self(), 'storage_5@127.0.0.1', storage),
    Res7 = leo_manager_cluster_monitor:register(first, self(), 'storage_6@127.0.0.1', storage),
    Res8 = leo_manager_cluster_monitor:register(first, self(), 'storage_7@127.0.0.1', storage),
    Res9 = leo_manager_cluster_monitor:register(first, self(), 'storage_n@127.0.0.1', storage),
    ?debugVal(Res9),

    ?assertEqual({ok,ok,ok,ok,ok,ok,ok,ok,ok}, {Res0,Res1,Res2,Res3,Res4,Res5,Res6,Res7,Res8}),
    ok.

-endif.
