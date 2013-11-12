%%======================================================================
%%
%% Leo Manager
%%
%% Copyright (c) 2012 Rakuten, Inc.
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
%% ---------------------------------------------------------------------
%% Leo Manager - API
%% @doc
%% @end
%%======================================================================
-module(leo_manager_api).

-author('Yosuke Hara').

-include("leo_manager.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(API_STORAGE, leo_storage_api).
-define(API_GATEWAY, leo_gateway_api).

-define(TYPE_REBALANCE_TAKEOVER, 'takeover').
-define(TYPE_REBALANCE_REGULAR,  'regular').
-define(type_rebalance(_Ret),
        case _Ret of
            {?TYPE_REBALANCE_TAKEOVER, _} ->
                ?TYPE_REBALANCE_TAKEOVER;
            _ ->
                ?TYPE_REBALANCE_REGULAR
        end).

%% API
-export([get_system_config/0, get_system_status/0, get_members/0,
         update_manager_nodes/1,
         get_node_status/1, get_routing_table_chksum/0, get_nodes/0]).

-export([attach/1, attach/4, detach/1, suspend/1, resume/1,
         distribute_members/1, distribute_members/2,
         start/0, rebalance/0]).

-export([register/4, register/7,
         notify/3, notify/4, purge/1, remove/1,
         whereis/2, recover/3, compact/2, compact/4, stats/2,
         synchronize/1, synchronize/2, synchronize/3,
         set_endpoint/1, delete_bucket/2
        ]).

-type(system_status() :: ?STATE_RUNNING | ?STATE_STOP).

-define(ERROR_COULD_NOT_MODIFY_STORAGE_STATE, "Could not modify the storage status").
-define(ERROR_COULD_NOT_GET_GATEWAY,          "Could not get gateway nodes").
-define(ERROR_COULD_NOT_GET_RTABLE_CHKSUM,    "Could not get a routing talble checksum").
-define(ERROR_META_NOT_FOUND,                 "Metadata not found").

-record(rebalance_proc_info, {
          members_cur  = [] :: list(#member{}),
          members_prev = [] :: list(#member{}),
          system_conf  = [] :: list(tuple())
         }).


%%----------------------------------------------------------------------
%% API-Function(s) - retrieve system information.
%%----------------------------------------------------------------------
%% @doc Retrieve system configuration from mnesia(localdb).
%%
-spec(get_system_config() ->
             {ok, #system_conf{}} |
             atom() |
             {error, any()}).
get_system_config() ->
    leo_manager_mnesia:get_system_config().


-spec(get_system_status() ->
             system_status() | {error, any()}).
get_system_status() ->
    case leo_manager_mnesia:get_storage_nodes_by_status(?STATE_RUNNING) of
        not_found ->
            ?STATE_STOP;
        {ok, [_H|_]} ->
            ?STATE_RUNNING;
        Error ->
            Error
    end.


%% @doc Retrieve members from mnesia(localdb).
%%
-spec(get_members() ->
             {ok, list()}).
get_members() ->
    leo_redundant_manager_api:get_members().


%% @doc Retrieve cluster-node-status from each server.
%%
-spec(get_node_status(atom()) ->
             ok | {error, any()}).
get_node_status(Node_1) ->
    Node_2 = list_to_atom(Node_1),
    {Type, Mod} = case leo_manager_mnesia:get_gateway_node_by_name(Node_2) of
                      {ok, _} -> {?SERVER_TYPE_GATEWAY, ?API_GATEWAY};
                      _ ->
                          case leo_manager_mnesia:get_storage_node_by_name(Node_2) of
                              {ok, _} -> {?SERVER_TYPE_STORAGE, ?API_STORAGE};
                              _       -> {[], undefined}
                          end
                  end,

    case Mod of
        undefined ->
            {error, not_found};
        _ ->
            case rpc:call(Node_2, Mod, get_node_status, [], ?DEF_TIMEOUT) of
                {ok, Status} ->
                    {ok, {Type, Status}};
                {_, Cause} ->
                    {error, Cause};
                timeout = Cause ->
                    {error, Cause}
            end
    end.


%% @doc Retrieve ring checksums from redundant-manager.
%%
-spec(get_routing_table_chksum() ->
             {ok, any()} |
             {error, any()}).
get_routing_table_chksum() ->
    case leo_redundant_manager_api:checksum(?CHECKSUM_RING) of
        {ok, []} ->
            {error, ?ERROR_COULD_NOT_GET_RTABLE_CHKSUM};
        {ok, Chksums} ->
            {ok, Chksums}
    end.


%% @doc Retrieve list of cluster nodes from mnesia.
%%
-spec(get_nodes() ->
             {ok, list()}).
get_nodes() ->
    Nodes_0 = case catch leo_manager_mnesia:get_gateway_nodes_all() of
                  {ok, R1} ->
                      lists:map(fun(#node_state{node  = Node,
                                                state = State}) ->
                                        {gateway, Node, State}
                                end, R1);
                  _ ->
                      []
              end,

    Nodes_1 = case catch leo_manager_mnesia:get_storage_nodes_all() of
                  {ok, R2} ->
                      lists:map(fun(#node_state{node  = Node,
                                                state = State}) ->
                                        {storage, Node, State}
                                end, R2);
                  _Error ->
                      []
              end,
    {ok, Nodes_0 ++ Nodes_1}.


%%----------------------------------------------------------------------
%% API-Function(s) - Operate for the Cluster nodes.
%%----------------------------------------------------------------------
%% @doc Attach an storage-node into the cluster.
%%
-spec(attach(atom()) ->
             ok | {error, any()}).
attach(Node) ->
    attach(Node, [], [], ?DEF_NUMBER_OF_VNODES).

attach(Node,_L1, L2, NumOfVNodes) ->
    case leo_misc:node_existence(Node) of
        true ->
            Status = get_system_status(),
            attach_1(Status, Node,_L1, L2, leo_date:clock(), NumOfVNodes);
        false ->
            {error, ?ERROR_COULD_NOT_CONNECT}
    end.

attach_1(?STATE_RUNNING, Node,_L1, L2, Clock, NumOfVNodes) ->
    State = ?STATE_ATTACHED,
    case leo_redundant_manager_api:reserve(Node, State, L2, Clock, NumOfVNodes) of
        ok ->
            leo_manager_mnesia:update_storage_node_status(
              #node_state{node    = Node,
                          state   = State,
                          when_is = leo_date:now()});
        Error ->
            Error
    end;

attach_1(_, Node,_L1, L2, Clock, NumOfVNodes) ->
    case leo_redundant_manager_api:attach(Node, L2, Clock, NumOfVNodes) of
        ok ->
            leo_manager_mnesia:update_storage_node_status(
              #node_state{node    = Node,
                          state   = ?STATE_ATTACHED,
                          when_is = leo_date:now()});
        Error ->
            Error
    end.


%% @doc Suspend a node.
%%
-spec(suspend(string()) ->
             ok | {error, any()}).
suspend(Node) ->
    case leo_redundant_manager_api:has_member(Node) of
        true ->
            case leo_misc:node_existence(Node) of
                true ->
                    case leo_manager_mnesia:update_storage_node_status(
                           update_state, #node_state{node  = Node,
                                                     state = ?STATE_SUSPEND}) of
                        ok ->
                            Res = leo_redundant_manager_api:suspend(Node, leo_date:clock()),
                            distribute_members(Res, []);
                        Error ->
                            Error
                    end;
                false ->
                    {error, ?ERROR_COULD_NOT_CONNECT}
            end;
        false ->
            {error, ?ERROR_NODE_NOT_EXISTS}
    end.


%% @doc Remove a storage-node from the cluster.
%%
-spec(detach(string()) ->
             ok | {error, any()}).
detach(Node) ->
    case leo_redundant_manager_api:has_member(Node) of
        true ->
            case leo_redundant_manager_api:checksum(?CHECKSUM_RING) of
                {ok, {CurRingHash, PrevRingHash}} when CurRingHash =:= PrevRingHash ->
                    detach_1(Node);
                {ok, _Checksums} ->
                    {error, on_rebalances};
                Error ->
                    Error
            end;
        false ->
            {error, ?ERROR_NODE_NOT_EXISTS}
    end.

detach_1(Node) ->
    State = ?STATE_DETACHED,
    case leo_redundant_manager_api:reserve(Node, State, leo_date:clock()) of
        ok ->
            leo_manager_mnesia:update_storage_node_status(
              #node_state{node    = Node,
                          state   = State,
                          when_is = leo_date:now()});
        Error ->
            Error
    end.


%% @doc Resume a storage-node when its status is 'RUNNING' OR 'DOWNED'.
%%
-spec(resume(atom()) ->
             ok | {error, any()}).
resume(Node) ->
    case leo_redundant_manager_api:has_member(Node) of
        true ->
            Res = leo_misc:node_existence(Node),
            resume(is_alive, Res, Node);
        false ->
            {error, ?ERROR_NODE_NOT_EXISTS}
    end.

-spec(resume(is_alive | is_state | sync | distribute | last, any(), atom()) ->
             any() | {error, any()}).
resume(is_alive, false, _Node) ->
    {error, ?ERROR_COULD_NOT_CONNECT};
resume(is_alive, true,  Node) ->
    Res = leo_manager_mnesia:get_storage_node_by_name(Node),
    resume(is_state, Res, Node);


resume(is_state, {ok, [#node_state{state = State}|_]}, Node) when State == ?STATE_SUSPEND;
                                                                  State == ?STATE_RESTARTED ->
    Res = leo_redundant_manager_api:update_member_by_node(
            Node, leo_date:clock(), ?STATE_RUNNING),
    resume(sync, Res, Node);
resume(is_state, {ok, [#node_state{state = State}|_]},_Node) ->
    {error, atom_to_list(State)};
resume(is_state, Error, _Node) ->
    Error;


resume(sync, ok, Node) ->
    Res = case leo_redundant_manager_api:get_members(?VER_CUR) of
              {ok, MembersCur} ->
                  case leo_redundant_manager_api:get_members(?VER_PREV) of
                      {ok, MembersPrev} ->
                          synchronize(?CHECKSUM_RING, Node, [{?VER_CUR,  MembersCur },
                                                             {?VER_PREV, MembersPrev}]);
                      Error ->
                          Error
                  end;
              Error ->
                  Error
          end,
    case distribute_members(Res, Node) of
        ok ->
            resume(last, Res, Node);
        Reason ->
            Reason
    end;
resume(sync, Error, _Node) ->
    Error;

resume(last, ok, Node) ->
    leo_manager_mnesia:update_storage_node_status(#node_state{node = Node,
                                                              state =  ?STATE_RUNNING});
resume(last, Error, _) ->
    Error.


%% @doc Distribute members list to all nodes.
%% @private
distribute_members([]) ->
    ok;
distribute_members([_|_]= Nodes) ->
    %% Retrieve storage-nodes from mnesia
    case leo_redundant_manager_api:get_members() of
        {ok, Members} ->
            StorageNodes = lists:filter(
                             fun(N) ->
                                     lists:member(N, Nodes) /= true
                             end,  [_N || #member{node  = _N,
                                                  state = ?STATE_RUNNING} <- Members]),

            %% Retrieve gateway nodes, then merge them with storage-nodes
            %% they're destination nodes in order to update "members"
            DestNodes = case leo_manager_mnesia:get_gateway_nodes_all() of
                            {ok, List} ->
                                lists:merge(StorageNodes,
                                            [_N || #member{node  = _N,
                                                           state = ?STATE_RUNNING} <- List]);
                            _ ->
                                StorageNodes
                        end,

            case rpc:multicall(DestNodes, leo_redundant_manager_api, update_members,
                               [Members], ?DEF_TIMEOUT) of
                {_, []} ->
                    void;
                {_, BadNodes} ->
                    ?error("distribute_members/2", "bad-nodes:~p", [BadNodes])
            end,
            ok;
        Error ->
            Error
    end;

distribute_members(Node) when is_atom(Node) ->
    distribute_members(ok, Node).

-spec(distribute_members(ok, atom()) ->
             ok | {error, any()}).
distribute_members(ok, Node) ->
    distribute_members([Node]);
distribute_members(Error, _Node) ->
    Error.

%% @doc update manager nodes
%%
-spec(update_manager_nodes(list()) ->
             ok | {error, any()}).
update_manager_nodes(Managers) ->
    Ret = case leo_manager_mnesia:get_storage_nodes_all() of
              {ok, Members} ->
                  StorageNodes = [_N || #node_state{node = _N} <- Members],
                  case rpc:multicall(StorageNodes, leo_storage_api, update_manager_nodes,
                                     [Managers], ?DEF_TIMEOUT) of
                      {_, []} -> ok;
                      {_, BadNodes} ->
                          ?error("update_manager_nodes/1", "bad-nodes:~p", [BadNodes]),
                          {error, {badrpc, BadNodes}}
                  end;
              Error ->
                  Error
          end,
    update_manager_nodes(Managers, Ret).

update_manager_nodes(Managers, ok) ->
    case leo_manager_mnesia:get_gateway_nodes_all() of
        {ok, Members} ->
            Fun = fun(#node_state{node  = Node}, Acc) ->
                          [Node|Acc]
                  end,
            GatewayNodes = lists:foldl(Fun, [], Members),
            case rpc:multicall(GatewayNodes, leo_gateway_api, update_manager_nodes,
                               [Managers], ?DEF_TIMEOUT) of
                {_, []} -> ok;
                {_, BadNodes} ->
                    ?error("update_manager_nodes/2", "bad-nodes:~p", [BadNodes]),
                    {error, {badrpc, BadNodes}}
            end;
        Error ->
            Error
    end;
update_manager_nodes(_Managers, Error) ->
    Error.

%% @doc Launch the leo-storage, but exclude Gateway(s).
%%
-spec(start() ->
             ok | {error, any()}).
start() ->
    %% Create current and previous RING(routing-table)
    case leo_redundant_manager_api:create() of
        {ok, Members, _Chksums} ->
            %% Distribute members-list to all storage nodes.
            Nodes = lists:map(fun(#member{node = Node}) ->
                                      Node
                              end, Members),

            %% Retrieve system-configuration
            %% Then launch storage-cluster
            case leo_manager_mnesia:get_system_config() of
                {ok, SystemConf} ->
                    Res = rpc:multicall(Nodes, ?API_STORAGE, start,
                                        [Members, SystemConf], ?DEF_TIMEOUT),
                    start_1(Res);
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            ?error("start/0", "cause:~p", [Cause]),
            {error, Cause}
    end.

%% @doc Check results and update an object of node-status
%% @private
start_1({ResL0, BadNodes0}) ->
    case lists:foldl(fun({ok, {Node, Chksum}}, {Acc0,Acc1}) ->
                             {[{Node, Chksum}|Acc0], Acc1};
                        ({error, {ErrorNode, _Cause}}, {Acc0,Acc1}) ->
                             {Acc0, [ErrorNode|Acc1]}
                     end, {[],[]}, ResL0) of
        {ResL1, []} ->
            case BadNodes0 of
                [] ->
                    lists:foreach(
                      fun({Node, {RingHash0, RingHash1}}) ->
                              leo_manager_mnesia:update_storage_node_status(
                                update,
                                #node_state{node          = Node,
                                            state         = ?STATE_RUNNING,
                                            ring_hash_new = leo_hex:integer_to_hex(RingHash0, 8),
                                            ring_hash_old = leo_hex:integer_to_hex(RingHash1, 8),
                                            when_is       = leo_date:now()})
                      end, ResL1),
                    {ResL1, []};
                _ ->
                    {ResL1, BadNodes0}
            end;
        {ResL1, BadNodes1} ->
            {ResL1, BadNodes0 ++ BadNodes1}
    end.


%% @doc Do Rebalance which affect all storage-nodes in operation.
%% [process flow]
%%     1. Judge that "is exist attach-node OR detach-node" ?
%%     2. Create RING (redundant-manager).
%%     3. Distribute each storage node. (from manager to storages)
%%     4. Confirm callback.
%%
-spec(rebalance() ->
             ok | {error, any()}).
rebalance() ->
    case leo_redundant_manager_api:get_members() of
        {ok, Members_1} ->
            {State, PairOfTakeover, Nodes} =
                lists:foldl(
                  fun(#member{state = ?STATE_RUNNING },{false, AccT, AccN}) ->
                          {true, AccT, AccN};
                     (#member{node  = Node,
                              state = ?STATE_ATTACHED},{SoFar, AccT, [{?STATE_DETACHED,_}] = AccN}) ->
                          NS = {?STATE_ATTACHED, Node},
                          {SoFar, [NS|AccT], [NS|AccN]};
                     (#member{node  = Node,
                              state = ?STATE_ATTACHED},{SoFar, AccT, AccN}) ->
                          NS = {?STATE_ATTACHED, Node},
                          case AccT of
                              [] -> {SoFar, [NS|AccT], [NS|AccN]};
                              _  -> {SoFar, AccT,      [NS|AccN]}
                          end;
                     (#member{node  = Node,
                              state = ?STATE_DETACHED},{SoFar, AccT, [{?STATE_ATTACHED,_}] = AccN}) ->
                          NS = {?STATE_DETACHED, Node},
                          {SoFar, [NS|AccT], [NS|AccN]};
                     (#member{node  = Node,
                              state = ?STATE_DETACHED},{SoFar, AccT, AccN}) ->
                          NS = {?STATE_DETACHED, Node},
                          case AccT of
                              [] -> {SoFar, [NS|AccT], [NS|AccN]};
                              _  -> {SoFar, AccT,      [NS|AccN]}
                          end;
                     (_Member, SoFar) ->
                          SoFar
                  end, {false, [], []}, Members_1),

            Nodes_1 = case PairOfTakeover of
                          [_,_] ->
                              lists:delete(lists:keyfind(?STATE_DETACHED, 1, Nodes), Nodes);
                          _ ->
                              Nodes
                      end,

            case rebalance_1(State, PairOfTakeover, Nodes_1) of
                {ok, RetRebalance} ->
                    [{NodeState, _}|_] = Nodes_1,
                    Nodes_2 = [N || {_, N} <- Nodes_1],

                    case leo_redundant_manager_api:get_members() of
                        {ok, Members_2} ->
                            {ok, SystemConf} = leo_manager_mnesia:get_system_config(),
                            case rebalance_3(NodeState, Nodes_2,
                                             #rebalance_proc_info{members_cur = Members_2,
                                                                  system_conf = SystemConf}) of
                                ok ->
                                    _ = distribute_members(Nodes_2),
                                    rebalance_5(RetRebalance, []);
                                {error, Cause}->
                                    ?error("rebalance/0", "cause:~p", [Cause]),
                                    {error, Cause}
                            end;
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

%% @private
rebalance_1(false,_PairOfTakeover,_Nodes) ->
    {error, not_running};
rebalance_1(_State, [], []) ->
    {error, not_need_to_rebalance};

%% for takeover-operation
rebalance_1(true, [_,_] = PairOfTakeover,_Nodes) ->
    Ret1 = lists:keyfind(?STATE_DETACHED, 1, PairOfTakeover),
    Ret2 = lists:keyfind(?STATE_ATTACHED, 1, PairOfTakeover),

    case (Ret1 /= false andalso Ret2 /= false) of
        true ->
            case assign_nodes_to_ring([Ret1, Ret2]) of
                ok ->
                    {_, DetachedNode} = Ret1,
                    {_, AttachedNode} = Ret2,
                    rebalance_1_1(DetachedNode, AttachedNode);
                Error ->
                    Error
            end;
        false ->
            {error, ?ERROR_NOT_SATISFY_CONDITION}
    end;

%% for regular-case
rebalance_1(true,_PairOfTakeover, Nodes) ->
    case assign_nodes_to_ring(Nodes) of
        ok ->
            case is_allow_to_distribute_command() of
                {true, _} ->
                    case leo_redundant_manager_api:rebalance() of
                        {ok, List} ->
                            Tbl = leo_hashtable:new(),
                            rebalance_2(Tbl, List);
                        Error ->
                            Error
                    end;
                {false, _} ->
                    {error, ?ERROR_NOT_SATISFY_CONDITION}
            end;
        Error ->
            Error
    end.

%% @private
rebalance_1_1(DetachedNode, AttachedNode) ->
    case leo_redundant_manager_api:delete_member_by_node(DetachedNode) of
        ok ->
            case leo_manager_mnesia:delete_storage_node(DetachedNode) of
                ok ->
                    {ok, {takeover, AttachedNode}};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

%% @private
rebalance_2(Tbl, []) ->
    Ret = case leo_hashtable:all(Tbl) of
              []   -> {error, no_entry};
              List -> {ok, List}
          end,
    catch leo_hashtable:destroy(Tbl),
    Ret;
rebalance_2(Tbl, [Item|T]) ->
    %% Item: [{vnode_id, VNodeId0}, {src, SrcNode}, {dest, DestNode}]
    VNodeId  = leo_misc:get_value('vnode_id', Item),
    SrcNode  = leo_misc:get_value('src',      Item),
    DestNode = leo_misc:get_value('dest',     Item),
    case SrcNode of
        {error, no_entry} ->
            void;
        _ ->
            ok = leo_hashtable:append(Tbl, SrcNode, {VNodeId, DestNode})
    end,
    rebalance_2(Tbl, T).

%% @private
rebalance_3(_, [],
            #rebalance_proc_info{members_cur = Members} = RebalanceProcInfo) ->
    rebalance_4(Members, RebalanceProcInfo, []);

rebalance_3(?STATE_ATTACHED, [Node|Rest],
            #rebalance_proc_info{members_cur = Members,
                                 system_conf = SystemConf} = RebalanceProcInfo) ->
    %% send a launch-message to a remote storage node
    Ret = case rpc:call(Node, ?API_STORAGE, start,
                        [Members, SystemConf], ?DEF_TIMEOUT) of
              {ok, {_Node, {RingHash0, RingHash1}}} ->
                  case leo_manager_mnesia:update_storage_node_status(
                         update, #node_state{node          = Node,
                                             state         = ?STATE_RUNNING,
                                             ring_hash_new = leo_hex:integer_to_hex(RingHash0, 8),
                                             ring_hash_old = leo_hex:integer_to_hex(RingHash1, 8),
                                             when_is       = leo_date:now()}) of
                      ok ->
                          case leo_redundant_manager_api:update_member_by_node(
                                 Node, leo_date:clock(), ?STATE_RUNNING) of
                              ok ->
                                  ok;
                              Error ->
                                  Error
                          end;
                      Error ->
                          Error
                  end;
              {error, {_Node, Cause}} ->
                  {error, Cause};
              {_, Cause} ->
                  {error, Cause};
              timeout = Cause ->
                  {error, Cause}
          end,

    %% check that fail sending message
    case Ret of
        ok -> void;
        {error, _} ->
            %% @TODO >> enqueue
            ok
    end,
    rebalance_3(?STATE_ATTACHED, Rest, RebalanceProcInfo);

rebalance_3(?STATE_DETACHED, [Node|Rest], RebalanceProcInfo) ->
    ok = leo_redundant_manager_api:update_member_by_node(
           Node, leo_date:clock(), ?STATE_STOP),

    case leo_manager_mnesia:get_storage_node_by_name(Node) of
        {ok, [NodeInfo|_]} ->
            _ = leo_manager_mnesia:delete_storage_node(NodeInfo),
            rebalance_3(?STATE_DETACHED, Rest, RebalanceProcInfo);
        Error ->
            Error
    end.

%% @private
rebalance_4([],_,[]) ->
    ok;
rebalance_4([],_,Acc) ->
    {error, Acc};
rebalance_4([#member{node  = Node,
                     state = ?STATE_RUNNING}|T], RebalanceProcInfo, Acc) ->
    MemberCur  = RebalanceProcInfo#rebalance_proc_info.members_cur,
    MemberPrev = RebalanceProcInfo#rebalance_proc_info.members_prev,
    SystemConf = RebalanceProcInfo#rebalance_proc_info.system_conf,
    %% @TODO
    Options = [{n, SystemConf#system_conf.n},
               {r, SystemConf#system_conf.r},
               {w, SystemConf#system_conf.w},
               {d, SystemConf#system_conf.d},
               {bit_of_ring, SystemConf#system_conf.bit_of_ring},
               {level_1, SystemConf#system_conf.level_1},
               {level_2, SystemConf#system_conf.level_2}
              ],

    Acc_1 = case rpc:call(Node, leo_redundant_manager_api, synchronize,
                          [?SYNC_TARGET_BOTH, [{?VER_CUR,  MemberCur},
                                               {?VER_PREV, MemberPrev}],
                           Options], ?DEF_TIMEOUT) of
                %% @TODO
                %% Acc_1 = case rpc:call(Node, leo_redundant_manager_api, synchronize,
                %%                       [?SYNC_TARGET_BOTH, [{?VER_CUR,  MemberCur},
                %%                                            {?VER_PREV, MemberPrev}]], ?DEF_TIMEOUT) of
                {ok, Hashes} ->
                    {RingHashCur, RingHashPrev} = leo_misc:get_value(?CHECKSUM_RING, Hashes),
                    _ = leo_manager_mnesia:update_storage_node_status(
                          update_chksum,
                          #node_state{node = Node,
                                      ring_hash_new = leo_hex:integer_to_hex(RingHashCur,  8),
                                      ring_hash_old = leo_hex:integer_to_hex(RingHashPrev, 8)}),
                    Acc;
                {_, Cause} ->
                    [{Node, Cause}|Acc];
                timeout = Cause ->
                    [{Node, Cause}|Acc]
            end,
    rebalance_4(T, RebalanceProcInfo, Acc_1);

rebalance_4([_|T], RebalanceProcInfo, Acc) ->
    rebalance_4(T, RebalanceProcInfo, Acc).


%% @private
rebalance_5({takeover, Node},_) ->
    recover(?RECOVER_BY_NODE, Node, true);

rebalance_5([], []) ->
    ok;
rebalance_5([], Acc) ->
    {error, Acc};
rebalance_5([{Node, Info}|T], Acc) ->
    case rpc:call(Node, ?API_STORAGE, rebalance, [Info], ?DEF_TIMEOUT) of
        ok ->
            rebalance_5(T, Acc);
        {_, Cause}->
            ?error("rebalance_5/2", "node:~w, cause:~p", [Node, Cause]),
            rebalance_5(T, [{Node, Cause}|Acc]);
        timeout = Cause ->
            ?error("rebalance_5/2", "node:~w, cause:~p", [Node, Cause]),
            rebalance_5(T, [{Node, Cause}|Acc])
    end.


%% @private
assign_nodes_to_ring([]) ->
    ok;
assign_nodes_to_ring([{?STATE_ATTACHED, Node}|Rest]) ->
    case leo_redundant_manager_api:get_member_by_node(Node) of
        {ok, #member{grp_level_1 = L1,
                     grp_level_2 = L2,
                     num_of_vnodes = NumOfVNodes}} ->
            case leo_redundant_manager_api:attach(Node, L1, L2, NumOfVNodes) of
                ok ->
                    assign_nodes_to_ring(Rest);
                Error ->
                    Error
            end;
        Error ->
            Error
    end;
assign_nodes_to_ring([{?STATE_DETACHED, Node}|Rest]) ->
    case leo_redundant_manager_api:detach(Node) of
        ok ->
            assign_nodes_to_ring(Rest);
        Error ->
            Error
    end.


%%----------------------------------------------------------------------
%% API-Function(s) - for system maintenance.
%%----------------------------------------------------------------------
%% @doc Register Pid of storage-node and Pid of gateway-node into the manager-monitors.
%%
-spec(register(first | again, pid(), atom(), atom()) ->
             ok).
register(RequestedTimes, Pid, Node, Type) ->
    leo_manager_cluster_monitor:register(RequestedTimes, Pid, Node, Type).

-spec(register(first | again, pid(), atom(), atom(), string(), string(), pos_integer()) ->
             ok).
register(RequestedTimes, Pid, Node, Type, IdL1, IdL2, NumOfVNodes) ->
    leo_manager_cluster_monitor:register(RequestedTimes, Pid, Node, Type, IdL1, IdL2, NumOfVNodes).


%% @doc Notified "Synchronized" from cluster-nods.
%%
notify(synchronized,_VNodeId, Node) ->
    synchronize_1(?SYNC_TARGET_RING_PREV, Node);
notify(_,_,_) ->
    {error, ?ERROR_INVALID_ARGS}.

%% @doc Notified "Server Error" from cluster-nods.
%%
notify(error, DownedNode, NotifyNode, ?ERR_TYPE_NODE_DOWN) ->
    Ret1 = notify1(DownedNode),
    Ret2 = notify1(NotifyNode),
    {ok, {Ret1, Ret2}};

%% @doc Notified "Rebalance Progress" from cluster-nods.
%%
notify(rebalance, VNodeId, Node, TotalOfObjects) ->
    leo_manager_mnesia:update_rebalance_info(
      #rebalance_info{vnode_id = VNodeId,
                      node     = Node,
                      total_of_objects = TotalOfObjects,
                      when_is  = leo_date:now()});

%% @doc Notified "Server Launch" from cluster-nods.
%%
notify(launched, gateway, Node, Checksums0) ->
    case get_routing_table_chksum() of
        {ok, Checksums1} when Checksums0 == Checksums1 ->
            {RingHash0, RingHash1} = Checksums1,
            leo_manager_mnesia:update_gateway_node(
              #node_state{node          = Node,
                          state         = ?STATE_RUNNING,
                          ring_hash_new = leo_hex:integer_to_hex(RingHash0, 8),
                          ring_hash_old = leo_hex:integer_to_hex(RingHash1, 8),
                          when_is       = leo_date:now()});
        {ok, _} ->
            {error, ?ERR_TYPE_INCONSISTENT_HASH};
        Error ->
            Error
    end;
notify(_,_,_,_) ->
    {error, ?ERROR_INVALID_ARGS}.


notify1(TargetNode) ->
    case leo_manager_mnesia:get_storage_node_by_name(TargetNode) of
        {ok, [#node_state{state = State,
                          error = NumOfErrors}|_]} ->
            case (State == ?STATE_SUSPEND  orelse
                  State == ?STATE_ATTACHED orelse
                  State == ?STATE_DETACHED orelse
                  State == ?STATE_RESTARTED) of
                true ->
                    ok;
                false ->
                    %% STATE_RUNNING | STATE_STOP
                    case leo_misc:node_existence(TargetNode, (10 * 1000)) of
                        true when State == ?STATE_RUNNING ->
                            ok;
                        true when State /= ?STATE_RUNNING ->
                            notify2(?STATE_RUNNING, TargetNode);
                        false ->
                            notify1(?STATE_STOP, TargetNode, NumOfErrors)
                    end;
                _ ->
                    {error, ?ERROR_COULD_NOT_MODIFY_STORAGE_STATE}
            end;
        _Error ->
            {error, ?ERROR_COULD_NOT_MODIFY_STORAGE_STATE}
    end.


notify1(?STATE_STOP = State, Node, NumOfErrors) when NumOfErrors >= ?DEF_NUM_OF_ERROR_COUNT ->
    notify2(State, Node);

notify1(?STATE_STOP, Node,_NumOfErrors) ->
    case leo_manager_mnesia:update_storage_node_status(
           increment_error, #node_state{node = Node}) of
        ok ->
            ok;
        _Error ->
            {error, ?ERROR_COULD_NOT_MODIFY_STORAGE_STATE}
    end.


notify2(?STATE_RUNNING = State, Node) ->
    Ret = case rpc:call(Node, ?API_STORAGE, get_routing_table_chksum, [], ?DEF_TIMEOUT) of
              {ok, {RingHash0, RingHash1}} ->
                  case rpc:call(Node, ?API_STORAGE, register_in_monitor, [again], ?DEF_TIMEOUT) of
                      ok ->
                          leo_manager_mnesia:update_storage_node_status(
                            update, #node_state{node          = Node,
                                                state         = State,
                                                ring_hash_new = leo_hex:integer_to_hex(RingHash0, 8),
                                                ring_hash_old = leo_hex:integer_to_hex(RingHash1, 8),
                                                when_is       = leo_date:now()});
                      {_, Cause} ->
                          {error, Cause}
                  end;
              {_, Cause} ->
                  {error, Cause}
          end,
    notify3(Ret, ?STATE_RUNNING, Node);

notify2(State, Node) ->
    Ret = leo_manager_mnesia:update_storage_node_status(
            update_state, #node_state{node  = Node,
                                      state = State}),
    notify3(Ret, State, Node).


notify3(ok, State, Node) ->
    Clock = leo_date:clock(),

    case leo_redundant_manager_api:update_member_by_node(Node, Clock, State) of
        ok ->
            case get_nodes() of
                {ok, []} ->
                    ok;
                {ok, Nodes} ->
                    _ = rpc:multicall(Nodes, leo_redundant_manager_api,
                                      update_member_by_node,
                                      [Node, Clock, State], ?DEF_TIMEOUT),
                    ok
            end;
        _Error ->
            {error, ?ERROR_COULD_NOT_MODIFY_STORAGE_STATE}
    end;

notify3({error,_Cause},_State,_Node) ->
    {error, ?ERROR_COULD_NOT_MODIFY_STORAGE_STATE}.


%% @doc purge an object.
%%
-spec(purge(string()) -> ok).
purge(Path) ->
    case leo_manager_mnesia:get_gateway_nodes_all() of
        {ok, R1} ->
            Nodes = lists:foldl(fun(#node_state{node  = Node,
                                                state = ?STATE_RUNNING}, Acc) ->
                                        [Node|Acc];
                                   (_, Acc) ->
                                        Acc
                                end, [], R1),
            _ = rpc:multicall(Nodes, ?API_GATEWAY, purge, [Path], ?DEF_TIMEOUT),
            ok;
        _Error ->
            {error, ?ERROR_COULD_NOT_GET_GATEWAY}
    end.


%% @doc remove a gateway-node
%%
-spec(remove(atom()) -> ok | {error, any()}).
remove(Node) when is_atom(Node) ->
    remove_3(Node);
remove(Node) ->
    remove_1(Node).

%% @private
remove_1(Node) ->
    case string:tokens(Node, "@") of
        [_, IP] ->
            remove_2(Node, IP);
        _ ->
            {error, ?ERROR_INVALID_ARGS}
    end.

%% @private
remove_2(Node, IP) ->
    case string:tokens(IP, ".") of
        [_,_,_,_] ->
            remove_3(list_to_atom(Node));
        _ ->
            {error, ?ERROR_INVALID_ARGS}
    end.

%% @private
remove_3(Node) ->
    case leo_manager_mnesia:get_gateway_node_by_name(Node) of
        {ok, [#node_state{state = ?STATE_STOP} = NodeState|_]} ->
            remove_4(NodeState);
        {ok, _} ->
            {error, ?ERROR_STILL_RUNNING};
        _ ->
            {error, ?ERROR_INVALID_ARGS}
    end.

%% @private
remove_4(NodeState) ->
    case leo_manager_mnesia:delete_gateway_node(NodeState) of
        ok ->
            ok;
        _Error ->
            {error, ?ERROR_COULD_NOT_GET_GATEWAY}
    end.


%% @doc Retrieve assigned file information.
%%
-spec(whereis(list(), boolean()) ->
             {ok, any()} |
             {error, any()}).
whereis([Key|_], true) ->
    KeyBin = list_to_binary(Key),
    case leo_redundant_manager_api:get_redundancies_by_key(KeyBin) of
        {ok, #redundancies{id = AddrId, nodes = Redundancies}} ->
            whereis_1(AddrId, KeyBin, Redundancies, []);
        _ ->
            {error, ?ERROR_COULD_NOT_GET_RING}
    end;

whereis(_Key, false) ->
    {error, ?ERROR_COULD_NOT_GET_RING};

whereis(_Key, _HasRoutingTable) ->
    {error, ?ERROR_INVALID_ARGS}.

whereis_1(_, _, [],Acc) ->
    {ok, lists:reverse(Acc)};

whereis_1(AddrId, Key, [{Node, true }|T], Acc) ->
    NodeStr = atom_to_list(Node),
    RPCKey  = rpc:async_call(Node, leo_storage_handler_object,
                             head, [AddrId, Key]),
    Reply   = case rpc:nb_yield(RPCKey, ?DEF_TIMEOUT) of
                  {value, {ok, #metadata{addr_id   = AddrId,
                                         dsize     = DSize,
                                         cnumber   = ChunkedObjs,
                                         clock     = Clock,
                                         timestamp = Timestamp,
                                         checksum  = Checksum,
                                         del       = DelFlag}}} ->
                      {NodeStr, AddrId, DSize, ChunkedObjs, Clock, Timestamp, Checksum, DelFlag};
                  _ ->
                      {NodeStr, not_found}
              end,
    whereis_1(AddrId, Key, T, [Reply | Acc]);

whereis_1(AddrId, Key, [{Node, false}|T], Acc) ->
    whereis_1(AddrId, Key, T, [{atom_to_list(Node), not_found} | Acc]).


%% @doc Recover key/node
%%
-spec(recover(binary(), string(), boolean()) ->
             ok | {error, any()}).
recover(?RECOVER_BY_FILE, Key, true) ->
    Key1 = list_to_binary(Key),
    case leo_redundant_manager_api:get_redundancies_by_key(Key1) of
        {ok, #redundancies{nodes = Redundancies}} ->
            Nodes = [N || {N, _} <- Redundancies],
            case rpc:multicall(Nodes, ?API_STORAGE, synchronize,
                               [Key1, 'error_msg_replicate_data'], ?DEF_TIMEOUT) of
                {_, []} ->
                    ok;
                {_, BadNodes} ->
                    {error, BadNodes}
            end;
        _ ->
            {error, ?ERROR_COULD_NOT_GET_RING}
    end;
recover(?RECOVER_BY_NODE, Node, true) ->
    Node_1 = case is_atom(Node) of
                 true  -> Node;
                 false -> list_to_atom(Node)
             end,
    %% Check the target node and system-state
    case leo_misc:node_existence(Node_1) of
        true ->
            Ret = case leo_redundant_manager_api:get_member_by_node(Node_1) of
                      {ok, #member{state = ?STATE_RUNNING}} -> true;
                      _ -> false
                  end,
            recover_node_1(Ret, Node_1);
        false ->
            {error, ?ERROR_COULD_NOT_CONNECT}
    end;

recover(?RECOVER_BY_RING, Node, true) ->
    Node_1 = case is_atom(Node) of
                 true  -> Node;
                 false -> list_to_atom(Node)
             end,
    case leo_misc:node_existence(Node_1) of
        true ->
            %% Check during-rebalance?
            case leo_redundant_manager_api:checksum(?CHECKSUM_RING) of
                {ok, {CurRingHash, PrevRingHash}} when CurRingHash == PrevRingHash ->
                    %% Sync target-node's member/ring with manager
                    case leo_redundant_manager_api:get_members() of
                        {ok, Members} ->
                            synchronize(?CHECKSUM_RING, Node_1, Members);
                        Error ->
                            Error
                    end;
                _ ->
                    {error, ?ERROR_DURING_REBALANCE}
            end;
        false ->
            {error, ?ERROR_COULD_NOT_CONNECT}
    end;

recover(_,_,true) ->
    {error, ?ERROR_INVALID_ARGS};
recover(_,_,false) ->
    {error, ?ERROR_COULD_NOT_GET_RING}.

%% @doc Execute recovery of the target node
%%      Check conditions
%% @private
recover_node_1(true, Node) ->
    {Ret, Members}= is_allow_to_distribute_command(Node),
    recover_node_2(Ret, Members, Node);
recover_node_1(false, _) ->
    {error, ?ERROR_TARGET_NODE_NOT_RUNNING}.

%% @doc Execute recovery of the target node
%% @private
recover_node_2(true, Members, Node) ->
    case rpc:multicall(Members, ?API_STORAGE, synchronize,
                       [Node], ?DEF_TIMEOUT) of
        {_, []} ->
            ok;
        {_, BadNodes} ->
            ?warn("recover_node_3/3", "bad_nodes:~p", [BadNodes]),
            {error, BadNodes}
    end;
recover_node_2(false,_,_) ->
    {error, ?ERROR_NOT_SATISFY_CONDITION}.


%% @doc Do compact.
%%
-spec(compact(string(), string() | atom()) ->
             ok).
compact(Mode, Node) when is_list(Node) ->
    compact(Mode, list_to_atom(Node));
compact(Mode, Node) ->
    ModeAtom = case Mode of
                   ?COMPACT_SUSPEND -> suspend;
                   ?COMPACT_RESUME  -> resume;
                   ?COMPACT_STATUS  -> status;
                   _ -> {error, ?ERROR_INVALID_ARGS}
               end,

    case ModeAtom of
        {error, Cause} ->
            {error, Cause};
        _ ->
            case rpc:call(Node, ?API_STORAGE, compact, [ModeAtom], ?DEF_TIMEOUT) of
                ok ->
                    ok;
                {ok, Status} ->
                    {ok, Status};
                {_, Cause} ->
                    ?warn("compact/2", "cause:~p", [Cause]),
                    {error, ?ERROR_FAILED_COMPACTION}
            end
    end.


-spec(compact(atom(), string() | atom(), list(), integer()) ->
             ok | {error, any}).
compact(_, [], _NumOfTargets, _MaxProc) ->
    {error, not_found};
compact(?COMPACT_START, Node, NumOfTargets, MaxProc) when is_list(Node) ->
    compact(?COMPACT_START, list_to_atom(Node), NumOfTargets, MaxProc);
compact(?COMPACT_START, Node, NumOfTargets, MaxProc) ->
    case leo_misc:node_existence(Node) of
        true ->
            case rpc:call(Node, ?API_STORAGE, compact,
                          [start, NumOfTargets, MaxProc], ?DEF_TIMEOUT) of
                ok ->
                    ok;
                {_, Cause} ->
                    ?warn("compact/4", "cause:~p", [Cause]),
                    {error, ?ERROR_FAILED_COMPACTION}
            end;
        false ->
            {error, ?ERR_TYPE_NODE_DOWN}
    end;
compact(_,_,_,_) ->
    {error, ?ERROR_INVALID_ARGS}.


%% @doc get storage stats.
%%
-spec(stats(summary | detail, string() | atom()) ->
             {ok, list()} | {error, any}).
stats(_, []) ->
    {error, not_found};

stats(Mode, Node) when is_list(Node) ->
    stats(Mode, list_to_atom(Node));

stats(Mode, Node) ->
    case leo_manager_mnesia:get_storage_node_by_name(Node) of
        {ok, _} ->
            case leo_misc:node_existence(Node) of
                true ->
                    case rpc:call(Node, leo_object_storage_api, stats, [], ?DEF_TIMEOUT) of
                        not_found = Cause ->
                            {error, Cause};
                        {ok, []} ->
                            {error, not_found};
                        {ok, Result} ->
                            stats1(Mode, Result)
                    end;
                false ->
                    {error, ?ERR_TYPE_NODE_DOWN}
            end;
        _ ->
            {error, not_found}
    end.

stats1(summary, List) ->
    {ok, lists:foldl(
           fun({ok, #storage_stats{file_path  = _ObjPath,
                                   compaction_histories = Histories,
                                   total_sizes = TotalSize,
                                   active_sizes = ActiveSize,
                                   total_num  = Total,
                                   active_num = Active}},
               {SumTotal, SumActive, SumTotalSize, SumActiveSize, LatestStart, LatestEnd}) ->
                   {LatestStart1, LatestEnd1} =
                       case length(Histories) of
                           0 -> {LatestStart, LatestEnd};
                           _ ->
                               {StartComp, FinishComp} = hd(Histories),
                               {max(LatestStart, StartComp), max(LatestEnd, FinishComp)}
                       end,
                   {SumTotal + Total,
                    SumActive + Active,
                    SumTotalSize + TotalSize,
                    SumActiveSize + ActiveSize,
                    LatestStart1,
                    LatestEnd1};
              (_, Acc) ->
                   Acc
           end, {0, 0, 0, 0, 0, 0}, List)};
stats1(detail, List) ->
    {ok, List}.


%% @doc Synchronize Members and Ring (both New and Old).
%%
synchronize(Type) when Type == ?CHECKSUM_RING;
                       Type == ?CHECKSUM_MEMBER ->
    case leo_redundant_manager_api:get_members(?VER_CUR) of
        {ok, MembersCur} ->
            case leo_redundant_manager_api:get_members(?VER_PREV) of
                {ok, MembersPrev} ->
                    %% synchronize member and ring with remote-node(s)
                    lists:map(
                      fun(#member{node  = Node,
                                  state = ?STATE_RUNNING}) ->
                              synchronize(Type, Node, [{?VER_CUR,  MembersCur },
                                                       {?VER_PREV, MembersPrev}]);
                         (_) ->
                              ok
                      end, MembersCur);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

synchronize(Type, Node, MembersList) when Type == ?CHECKSUM_RING;
                                          Type == ?CHECKSUM_MEMBER ->
    {ok, SystemConf} = leo_manager_mnesia:get_system_config(),
    Options = [{n, SystemConf#system_conf.n},
               {r, SystemConf#system_conf.r},
               {w, SystemConf#system_conf.w},
               {d, SystemConf#system_conf.d},
               {bit_of_ring, SystemConf#system_conf.bit_of_ring},
               {level_1, SystemConf#system_conf.level_1},
               {level_2, SystemConf#system_conf.level_2}
              ],
    MembersCur  = leo_misc:get_value(?VER_CUR,  MembersList),
    MembersPrev = leo_misc:get_value(?VER_PREV, MembersList),

    case rpc:call(Node, leo_redundant_manager_api, synchronize,
                  [?SYNC_TARGET_BOTH, [{?VER_CUR,  MembersCur },
                                       {?VER_PREV, MembersPrev}], Options], ?DEF_TIMEOUT) of
        {ok, Hashes} ->
            {RingHashCur, RingHashPrev} = leo_misc:get_value(?CHECKSUM_RING, Hashes),
            leo_manager_mnesia:update_storage_node_status(
              update_chksum, #node_state{node          = Node,
                                         ring_hash_new = leo_hex:integer_to_hex(RingHashCur, 8),
                                         ring_hash_old = leo_hex:integer_to_hex(RingHashPrev,8)}),
            ok;
        {_, Cause} ->
            ?warn("synchronize/3", "cause:~p", [Cause]),
            {error, Cause};
        timeout = Cause ->
            ?warn("synchronize/3", "cause:~p", [Cause]),
            {error, Cause}
    end;
synchronize(_,_,_) ->
    ok.

%% @doc From manager-node
synchronize(?CHECKSUM_MEMBER, Node) when is_atom(Node) ->
    synchronize_1(?SYNC_TARGET_MEMBER, Node);

synchronize(?CHECKSUM_RING, Node) when is_atom(Node) ->
    synchronize_1(?SYNC_TARGET_RING_CUR,  Node),
    synchronize_1(?SYNC_TARGET_RING_PREV, Node),
    ok;


%% @doc From gateway and storage-node
synchronize(?CHECKSUM_MEMBER = Type, [{Node_1, Checksum_1},
                                      {Node_2, Checksum_2}]) ->
    Ret = case (Node_1 == node()) of
              true ->
                  case leo_manager_mnesia:get_storage_node_by_name(Node_2) of
                      {ok, [#node_state{state = ?STATE_STOP}|_]} ->
                          notify1(Node_2);
                      _ ->
                          not_match
                  end;
              false ->
                  not_match
          end,

    case Ret of
        not_match ->
            case leo_redundant_manager_api:checksum(Type) of
                {ok, LocalChecksum} ->
                    compare_local_chksum_with_remote_chksum(
                      ?SYNC_TARGET_MEMBER, Node_1, LocalChecksum, Checksum_1),
                    compare_local_chksum_with_remote_chksum(
                      ?SYNC_TARGET_MEMBER, Node_2, LocalChecksum, Checksum_2);
                Error ->
                    Error
            end;
        _ ->
            Ret
    end;

synchronize(?CHECKSUM_RING = Type, [{Node_1, {CurRingHash_1, PrevRingHash_1}},
                                    {Node_2, {CurRingHash_2, PrevRingHash_2}}]) ->
    case leo_redundant_manager_api:checksum(Type) of
        {ok, {LocalCurRingHash, LocalPrevRingHash}} ->
            %% copare manager-cur-ring-hash with remote cur-ring-hash
            _ = compare_local_chksum_with_remote_chksum(
                  ?SYNC_TARGET_RING_CUR,  Node_1, LocalCurRingHash,  CurRingHash_1),
            _ = compare_local_chksum_with_remote_chksum(
                  ?SYNC_TARGET_RING_CUR,  Node_2, LocalCurRingHash,  CurRingHash_2),

            %% copare manager-cur/prev-ring-hash/ with remote prev-ring-hash
            _ = compare_local_chksum_with_remote_chksum(
                  ?SYNC_TARGET_RING_PREV, Node_1, LocalCurRingHash, LocalPrevRingHash, PrevRingHash_1),
            _ = compare_local_chksum_with_remote_chksum(
                  ?SYNC_TARGET_RING_PREV, Node_2, LocalCurRingHash, LocalPrevRingHash, PrevRingHash_2);
        Error ->
            Error
    end.

%% @doc Synchronize members-list or rings
%% @private
-spec(synchronize_1(?SYNC_TARGET_MEMBER   |
                    ?SYNC_TARGET_RING_CUR |
                    ?SYNC_TARGET_RING_PREV, atom()) ->
             ok | {error, any()}).
synchronize_1(?SYNC_TARGET_MEMBER = Type, Node) ->
    case leo_redundant_manager_api:get_members(?VER_CUR) of
        {ok, MembersCur} ->
            case leo_redundant_manager_api:get_members(?VER_PREV) of
                {ok, MembersPrev} ->
                    case rpc:call(Node, leo_redundant_manager_api, synchronize,
                                  [Type, [{?VER_CUR,  MembersCur},
                                          {?VER_PREV, MembersPrev}]], ?DEF_TIMEOUT) of
                        {ok, _} ->
                            ok;
                        timeout = Cause ->
                            {error, Cause};
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end;

synchronize_1(Type, Node) when Type == ?SYNC_TARGET_RING_CUR;
                               Type == ?SYNC_TARGET_RING_PREV ->
    Ver = case Type of
              ?SYNC_TARGET_RING_CUR  -> ?VER_CUR;
              ?SYNC_TARGET_RING_PREV -> ?VER_PREV
          end,

    case leo_redundant_manager_api:get_members(Ver) of
        {ok, Members} ->
            case rpc:call(Node, leo_redundant_manager_api, synchronize,
                          [Type, [{Ver, Members}]], ?DEF_TIMEOUT) of
                {ok, Hashes} ->
                    synchronize_2(Node, Hashes);
                {_, Cause} ->
                    {error, Cause};
                timeout = Cause ->
                    {error, Cause}
            end;
        Error ->
            Error
    end;
synchronize_1(_,_) ->
    {error, ?ERROR_INVALID_ARGS}.


synchronize_2(Node, Hashes) ->
    {RingHashCur, RingHashPrev} = leo_misc:get_value(?CHECKSUM_RING, Hashes),

    case leo_manager_mnesia:get_gateway_node_by_name(Node) of
        {ok, [NodeState|_]} ->
            leo_manager_mnesia:update_gateway_node(
              NodeState#node_state{ring_hash_new = leo_hex:integer_to_hex(RingHashCur,  8),
                                   ring_hash_old = leo_hex:integer_to_hex(RingHashPrev, 8)});
        _ ->
            case leo_manager_mnesia:get_storage_node_by_name(Node) of
                {ok, _} ->
                    leo_manager_mnesia:update_storage_node_status(
                      update_chksum,
                      #node_state{node  = Node,
                                  ring_hash_new = leo_hex:integer_to_hex(RingHashCur,  8),
                                  ring_hash_old = leo_hex:integer_to_hex(RingHashPrev, 8)});
                _ ->
                    void
            end
    end,
    ok.



%% @doc Compare local-checksum with remote-checksum
%% @private
compare_local_chksum_with_remote_chksum(_,_, Checksum_1, Checksum_2)
  when Checksum_1 =:= Checksum_2 -> ok;
compare_local_chksum_with_remote_chksum(Type, Node, Checksum_1, Checksum_2)
  when Checksum_1 =/= Checksum_2 -> synchronize_1(Type, Node).

%% @private
compare_local_chksum_with_remote_chksum(_,_,RemoteChecksum,_,RemoteChecksum) ->
    ok;
compare_local_chksum_with_remote_chksum(_,_,_,RemoteChecksum,RemoteChecksum) ->
    ok;
compare_local_chksum_with_remote_chksum(Type, Node,_,_,_) ->
    synchronize_1(Type, Node).


%% @doc Insert an endpoint
%%
-spec(set_endpoint(binary()) ->
             ok | {error, any()}).
set_endpoint(Endpoint) ->
    case catch leo_manager_mnesia:get_gateway_nodes_all() of
        {ok, Nodes_0} ->
            case lists:flatten(lists:map(fun(#node_state{node  = Node,
                                                         state = ?STATE_RUNNING}) ->
                                                 Node;
                                            (_) ->
                                                 []
                                         end, Nodes_0)) of
                [] ->
                    ok;
                Nodes_1 ->
                    case rpc:multicall(Nodes_1, ?API_GATEWAY, set_endpoint,
                                       [Endpoint], ?DEF_TIMEOUT) of
                        {_, []} ->
                            ok;
                        {_, BadNodes} ->
                            {error, BadNodes}
                    end
            end;
        not_found ->
            ok;
        Error ->
            Error
    end.


%% @doc Remove a bucket from storage-cluster and manager
%%
-spec(delete_bucket(binary(), binary()) ->
             ok | {error, any()}).
delete_bucket(AccessKey, Bucket) ->
    AccessKeyBin = case is_binary(AccessKey) of
                       true  -> AccessKey;
                       false -> list_to_binary(AccessKey)
                   end,
    BucketBin    = case is_binary(Bucket) of
                       true  -> Bucket;
                       false -> list_to_binary(Bucket)
                   end,

    %% Check during-rebalance
    case leo_redundant_manager_api:checksum(?CHECKSUM_RING) of
        {ok, {CurRingHash, PrevRingHash}} when CurRingHash == PrevRingHash ->
            %% Check preconditions
            case is_allow_to_distribute_command() of
                {true, _}->
                    case leo_s3_bucket:head(AccessKeyBin, BucketBin) of
                        ok ->
                            delete_bucket_1(AccessKeyBin, BucketBin);
                        not_found ->
                            {error, not_found};
                        {error, _} ->
                            {error, ?ERROR_INVALID_ARGS}
                    end;
                _ ->
                    {error, ?ERROR_NOT_SATISFY_CONDITION}
            end;
        _ ->
            {error, ?ERROR_DURING_REBALANCE}
    end.

delete_bucket_1(AccessKeyBin, BucketBin) ->
    case leo_redundant_manager_api:get_members_by_status(?STATE_RUNNING) of
        {ok, Members} ->
            Nodes = lists:map(fun(#member{node = Node}) ->

                                      Node
                              end, Members),
            case rpc:multicall(Nodes, leo_storage_handler_directory,
                               delete_objects_in_parent_dir,
                               [BucketBin], ?DEF_TIMEOUT) of
                {_, []} -> void;
                {_, BadNodes} ->
                    ?error("start/0", "bad-nodes:~p", [BadNodes])
            end,
            delete_bucket_2(AccessKeyBin, BucketBin);
        {error, Cause} ->
            {error, Cause}
    end.

delete_bucket_2(AccessKeyBin, BucketBin) ->
    case leo_s3_bucket:delete(AccessKeyBin, BucketBin) of
        ok ->
            ok;
        {error, badarg} ->
            {error, ?ERROR_INVALID_BUCKET_FORMAT};
        {error, _Cause} ->
            {error, ?ERROR_COULD_NOT_STORE}
    end.


%% @doc Is allow distribute to a command
%% @private
-spec(is_allow_to_distribute_command() ->
             boolean()).
is_allow_to_distribute_command() ->
    is_allow_to_distribute_command([]).

-spec(is_allow_to_distribute_command(atom()) ->
             boolean()).
is_allow_to_distribute_command(Node) ->
    {ok, SystemConf} = leo_manager_mnesia:get_system_config(),
    {ok, Members_1}   = leo_redundant_manager_api:get_members(),
    {Total, Active, Members_2} =
        lists:foldl(fun(#member{node = N}, Acc) when N == Node ->
                            Acc;
                       (#member{state = ?STATE_DETACHED}, Acc) ->
                            Acc;
                       (#member{state = ?STATE_ATTACHED}, Acc) ->
                            Acc;
                       (#member{state = ?STATE_RUNNING,
                                node  = N}, {Num1,Num2,M}) ->
                            {Num1+1, Num2+1, [N|M]};
                       (_, {Num1,Num2,M}) ->
                            {Num1+1, Num2, M}
                    end, {0,0,[]}, Members_1),

    NVal = SystemConf#system_conf.n,
    Diff = case (SystemConf#system_conf.n < 3) of
               true  -> 0;
               false ->
                   NVal - (NVal - 1)
           end,
    Ret  = case ((Total - Active) =< Diff) of
               true ->
                   case rpc:multicall(Members_2, erlang, node, [], ?DEF_TIMEOUT) of
                       {_, []} -> true;
                       _ -> false
                   end;
               false ->
                   false
           end,
    {Ret, Members_2}.

