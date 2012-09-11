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


%% API
-export([get_system_config/0, get_system_status/0, get_members/0,
         get_node_status/1, get_routing_table_chksum/0, get_nodes/0]).

-export([attach/1, detach/1, suspend/1, resume/1,
         start/0, rebalance/0]).

-export([register/4, notify/3, notify/4, purge/1,
         whereis/2, compact/1, stats/2, synchronize/1, synchronize/2, synchronize/3]).

-type(system_status() :: ?STATE_RUNNING | ?STATE_STOP).

-define(ERROR_COULD_NOT_GET_RTABLE_CHKSUM, "could not get a routing talble checksum").
-define(ERROR_META_NOT_FOUND,              "metadata not found").

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
get_node_status(Node0) ->
    Node1 = list_to_atom(Node0),
    Mod   = case leo_manager_mnesia:get_gateway_node_by_name(Node1) of
                {ok, _} -> leo_gateway_api;
                _ ->
                    case leo_manager_mnesia:get_storage_node_by_name(Node1) of
                        {ok, _} -> leo_storage_api;
                        _       -> undefined
                    end
            end,

    case Mod of
        undefined ->
            {error, not_found};
        _ ->
            case rpc:call(Node1, Mod, get_node_status, [], ?DEF_TIMEOUT) of
                {ok, Status} ->
                    {ok, Status};
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
    case leo_redundant_manager_api:checksum(ring) of
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
    Nodes0 = case catch leo_manager_mnesia:get_gateway_nodes_all() of
                 {ok, R1} ->
                     lists:map(fun(#node_state{node  = Node,
                                               state = State}) ->
                                       {gateway, Node, State}
                               end, R1);
                 _ ->
                     []
             end,

    Nodes1 = case catch leo_manager_mnesia:get_storage_nodes_all() of
                 {ok, R2} ->
                     lists:map(fun(#node_state{node  = Node,
                                               state = State}) ->
                                       {storage, Node, State}
                               end, R2);
                 _Error ->
                     []
             end,
    {ok, Nodes0 ++ Nodes1}.


get_nodes(Node) ->
    {ok, Nodes0} = get_nodes(),
    Res = lists:foldl(fun({_, N, _}, Acc) when Node == N ->
                              Acc;
                         ({_, N, _}, Acc) ->
                              [N|Acc]
                      end, [], Nodes0),
    {ok, Res}.


%%----------------------------------------------------------------------
%% API-Function(s) - Operate for the Cluster nodes.
%%----------------------------------------------------------------------
%% @doc Attach an storage-node into the cluster.
%%
-spec(attach(atom()) ->
             ok | {error, any()}).
attach(Node) ->
    case leo_misc:node_existence(Node) of
        true ->
            case leo_redundant_manager_api:attach(Node) of
                ok ->
                    leo_manager_mnesia:update_storage_node_status(
                      #node_state{node    = Node,
                                  state   = ?STATE_ATTACHED,
                                  when_is = leo_date:now()});
                Error ->
                    Error
            end;
        false ->
            {error, ?ERROR_COULD_NOT_CONNECT}
    end.


%% @doc Suspend a node.
%%
-spec(suspend(string()) ->
             ok | {error, any()}).
suspend(Node) ->
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
                    case leo_redundant_manager_api:detach(Node) of
                        ok ->
                            Res = leo_manager_mnesia:update_storage_node_status(
                                    #node_state{node    = Node,
                                                state   = ?STATE_DETACHED,
                                                when_is = leo_date:now()}),
                            distribute_members(Res, Node);
                        Error ->
                            Error
                    end;
                {ok, _Checksums} ->
                    {error, on_rebalances};
                Error ->
                    Error
            end;
        false ->
            {error, not_fouund}
    end.


%% @doc Resume a storage-node when its status is 'RUNNING' OR 'DOWNED'.
%%
-spec(resume(atom()) ->
             ok | {error, any()}).
resume(Node) ->
    Res = leo_misc:node_existence(Node),
    resume(is_alive, Res, Node).

-spec(resume(is_alive | is_state | sync | distribute | last, any(), atom()) ->
             any() | {error, any()}).
resume(is_alive, false, _Node) ->
    {error, ?ERROR_COULD_NOT_CONNECT};
resume(is_alive, true,  Node) ->
    Res = leo_manager_mnesia:get_storage_node_by_name(Node),
    resume(is_state, Res, Node);


resume(is_state, {ok, [#node_state{state = State}|_]}, Node) when State == ?STATE_SUSPEND;
                                                                  State == ?STATE_RESTARTED ->
    Res = leo_redundant_manager_api:update_member_by_node(Node, leo_date:clock(), ?STATE_RUNNING),
    resume(sync, Res, Node);
resume(is_state, {ok, [#node_state{state = State}|_]},_Node) ->
    {error, atom_to_list(State)};
resume(is_state, Error, _Node) ->
    Error;


resume(sync, ok, Node) ->
    Res = case leo_redundant_manager_api:get_members() of
              {ok, Members} ->
                  synchronize(?CHECKSUM_RING, Node, Members);
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
distribute_members([Node|Rest]) ->
    _ = distribute_members(ok, Node),
    distribute_members(Rest);

distribute_members(Node) when is_atom(Node) ->
    distribute_members(ok, Node).

-spec(distribute_members(ok, atom()) ->
             ok | {error, any()}).
distribute_members(ok, Node0) ->
    case leo_redundant_manager_api:get_members() of
        {ok, Members} ->
            Fun = fun(#member{node  = Node1,
                              state = ?STATE_RUNNING}, Acc) when Node0 =/= Node1 ->
                          [Node1|Acc];
                     (_, Acc) ->
                          Acc
                  end,
            StorageNodes = lists:foldl(Fun, [], Members),
            DestNodes    = case leo_manager_mnesia:get_gateway_nodes_all() of
                               {ok, GatewayNodes} ->
                                   lists:foldl(fun(#node_state{node = Node2}, Acc) ->
                                                       [Node2|Acc]
                                               end, StorageNodes, GatewayNodes);
                               _ ->
                                   StorageNodes
                           end,

            case rpc:multicall(DestNodes, leo_redundant_manager_api, update_members,
                               [Members], ?DEF_TIMEOUT) of
                {_, []      } ->
                    void;
                {_, BadNodes} ->
                    ?warn("resume/3", "bad_nodes:~p", [BadNodes]);
                _ ->
                    void
            end,
            ok;
        Error ->
            Error
    end;
distribute_members(Error, _Node) ->
    Error.


%% @doc Launch the leo-storage, but exclude Gateway(s).
%%
-spec(start() ->
             ok | {error, any()}).
start() ->
    case leo_redundant_manager_api:create() of
        {ok, Members, _Chksums} ->
            %% Distribute members-list to all storage nodes.
            Nodes = lists:map(fun(#member{node = Node}) ->
                                      Node
                              end, Members),
            {ok, SystemConf}   = leo_manager_mnesia:get_system_config(),
            {ResL0, BadNodes0} = rpc:multicall(
                                   Nodes, leo_storage_api, start, [Members, SystemConf], infinity),

            %% Update an object of node-status.
            case lists:foldl(fun({ok, {Node, Chksum}}, {Acc0,Acc1}) ->
                                     {[{Node, Chksum}|Acc0], Acc1};
                                ({error, {ErrorNode, _Cause}}, {Acc0,Acc1}) ->
                                     {Acc0, [ErrorNode|Acc1]}
                             end, {[],[]}, ResL0) of
                {ResL1, BadNodes1} ->
                    lists:foreach(
                      fun({Node, {RingHash0, RingHash1}}) ->
                              leo_manager_mnesia:update_storage_node_status(
                                update, #node_state{node          = Node,
                                                    state         = ?STATE_RUNNING,
                                                    ring_hash_new = leo_hex:integer_to_hex(RingHash0),
                                                    ring_hash_old = leo_hex:integer_to_hex(RingHash1),
                                                    when_is       = leo_date:now()})
                      end, ResL1),
                    {ResL1, BadNodes0 ++ BadNodes1}
            end;
        {error, Cause} ->
            ?error("start/0", "cause:~p", [Cause]),
            {error, Cause}
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
        {ok, Members} ->
            {State, Nodes} = lists:foldl(
                               fun(#member{node =_Node, state = ?STATE_RUNNING },{false, Acc}) ->
                                       {true, Acc};
                                  (#member{node = Node, state = ?STATE_ATTACHED},{SoFar, Acc}) ->
                                       {SoFar, [{attached, Node}|Acc]};
                                  (#member{node = Node, state = ?STATE_DETACHED},{SoFar, Acc}) ->
                                       {SoFar, [{detached, Node}|Acc]};
                                  (_Member, SoFar) ->
                                       SoFar
                               end, {false, []}, Members),

            case rebalance1(State, Nodes) of
                {ok, List} ->
                    [{NodeState, _}|_] = Nodes,
                    Ns = [N || {_, N} <- Nodes],

                    case leo_redundant_manager_api:get_members() of
                        {ok, Members} ->
                            case rebalance3(NodeState, Ns, Members) of
                                ok ->
                                    _ = distribute_members(Ns),
                                    rebalance5(List, []);
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

rebalance1(false, _Nodes) ->
    {error, not_running};
rebalance1(_State, []) ->
    {error, not_need_to_rebalance};
rebalance1(true, _Nodes) ->
    case leo_redundant_manager_api:rebalance() of
        {ok, List} ->
            Tbl = leo_hashtable:new(),
            rebalance2(Tbl, List);
        Error ->
            Error
    end.

rebalance2(Tbl, []) ->
    case leo_hashtable:all(Tbl) of
        [] ->
            {error, no_entry};
        List ->
            {ok, List}
    end;
rebalance2(Tbl, [Item|T]) ->
    %% Item: [{vnode_id, VNodeId0}, {src, SrcNode}, {dest, DestNode}]
    VNodeId  = proplists:get_value('vnode_id', Item),
    SrcNode  = proplists:get_value('src',      Item),
    DestNode = proplists:get_value('dest',     Item),

    ok = leo_hashtable:append(Tbl, SrcNode, {VNodeId, DestNode}),
    rebalance2(Tbl, T).

rebalance3(?STATE_ATTACHED, [], Members) ->
    rebalance4(Members, Members, []);

rebalance3(?STATE_ATTACHED, [Node|Rest], Members) ->
    %% New Attached-node change Ring.cur, Ring.prev and Members
    {ok, SystemConf} = leo_manager_mnesia:get_system_config(),

    case rpc:call(Node, leo_storage_api, start, [Members, SystemConf], ?DEF_TIMEOUT) of
        {ok, {_Node, {RingHash0, RingHash1}}} ->
            case leo_manager_mnesia:update_storage_node_status(
                   update, #node_state{node          = Node,
                                       state         = ?STATE_RUNNING,
                                       ring_hash_new = leo_hex:integer_to_hex(RingHash0),
                                       ring_hash_old = leo_hex:integer_to_hex(RingHash1),
                                       when_is       = leo_date:now()}) of
                ok ->
                    case leo_redundant_manager_api:update_member_by_node(
                           Node, leo_date:clock(), ?STATE_RUNNING) of
                        ok ->
                            rebalance3(?STATE_ATTACHED, Rest, Members);
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
            %% Error ->
            %%     Error
    end;

rebalance3(?STATE_DETACHED, Node, Members) ->
    _ = rpc:call(Node, leo_storage_api, stop, [], ?DEF_TIMEOUT),
    {ok, Ring} = leo_redundant_manager_api:get_ring(?SYNC_MODE_CUR_RING),
    ok = leo_redundant_manager_api:update_member_by_node(Node, leo_date:clock(), ?STATE_DETACHED),
    _ = leo_redundant_manager_api:synchronize(?SYNC_MODE_PREV_RING, Ring),
    rebalance4(Members, Members, []).


rebalance4(_Members, [], []) ->
    ok;
rebalance4(_Members, [], Errors) ->
    {error, Errors};
rebalance4(Members, [#member{node  = Node,
                             state = ?STATE_RUNNING}|T], Errors0) ->
    %% already-started node -> ring(cur) + member
    %%
    {ok, Ring}    = leo_redundant_manager_api:get_ring(?SYNC_MODE_CUR_RING),
    ObjectOfRings = lists:foldl(fun(#member{state = ?STATE_ATTACHED}, null) ->
                                        ?SYNC_MODE_CUR_RING;
                                   (#member{state = ?STATE_DETACHED}, null) ->
                                        [?SYNC_MODE_CUR_RING, ?SYNC_MODE_PREV_RING];
                                   (_, Acc) ->
                                        Acc
                                end, null, Members),
    Errors1 =
        case rpc:call(Node, leo_redundant_manager_api, synchronize,
                      [ObjectOfRings, Ring], ?DEF_TIMEOUT) of
            {ok, {RingHash0, RingHash1}} ->
                _ = leo_manager_mnesia:update_storage_node_status(
                      update_chksum, #node_state{node  = Node,
                                                 ring_hash_new = leo_hex:integer_to_hex(RingHash0),
                                                 ring_hash_old = leo_hex:integer_to_hex(RingHash1)}),
                Errors0;
            {_, Cause} ->
                [{Node, Cause}|Errors0];
            timeout = Cause ->
                [{Node, Cause}|Errors0]
        end,
    rebalance4(Members, T, Errors1);

rebalance4(Members, [_|T], Errors0) ->
    rebalance4(Members, T, Errors0).


rebalance5([], []) ->
    ok;
rebalance5([], Errors0) ->
    {error, Errors0};
rebalance5([{Node, Info}|T], Errors0) ->
    case rpc:call(Node, leo_storage_api, rebalance, [Info], ?DEF_TIMEOUT) of
        ok ->
            rebalance5(T, Errors0);
        {_, Cause}->
            ?error("rebalance5/2", "node:~w, cause:~p", [Node, Cause]),
            rebalance5(T, [{Node, Cause}|Errors0]);
        timeout = Cause ->
            ?error("rebalance5/2", "node:~w, cause:~p", [Node, Cause]),
            rebalance5(T, [{Node, Cause}|Errors0])
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


%% @doc Notified "Server Error", "Server Launch" and "Rebalance Progress" from cluster-nods.
%%
-spec(notify(error | launched | rebalance | synchronized, atom(), ?ERR_TYPE_NODE_DOWN) ->
             ok | {error, any()}).
notify(error, Node, ?ERR_TYPE_NODE_DOWN) ->
    case leo_manager_mnesia:get_storage_node_by_name(Node) of
        {ok, [#node_state{state = State,
                          error = NumOfErrors}|_]} ->
            case (State == ?STATE_SUSPEND  orelse
                  State == ?STATE_DETACHED) of
                true ->
                    ok;
                false ->
                    case leo_redundant_manager_api:get_members_count() of
                        {error, Cause} ->
                            {error, Cause};
                        Size when Size >= ?DEF_NUM_OF_ERROR_COUNT ->
                            notify1(error, Node, NumOfErrors, ?DEF_NUM_OF_ERROR_COUNT);
                        Size ->
                            notify1(error, Node, NumOfErrors, Size)
                    end;
                _ ->
                    {error, "could not modify storage status"}
            end;
        _Error ->
            {error, "could not modify storage status"}
    end;

notify(synchronized, VNodeId, Node) ->
    ok = leo_redundant_manager_api:adjust(VNodeId),
    Res = synchronize1(?SYNC_MODE_PREV_RING, Node),
    Res;

notify(_,_,_) ->
    {error, badarg}.

notify(rebalance, VNodeId, Node, TotalOfObjects) ->
    leo_manager_mnesia:update_rebalance_info(#rebalance_info{vnode_id = VNodeId,
                                                             node     = Node,
                                                             total_of_objects = TotalOfObjects,
                                                             when_is  = leo_date:now()});
notify(launched, gateway, Node, Checksums0) ->
    case get_routing_table_chksum() of
        {ok, Checksums1} when Checksums0 == Checksums1 ->
            {RingHash0, RingHash1} = Checksums1,
            leo_manager_mnesia:update_gateway_node(#node_state{node          = Node,
                                                               state         = ?STATE_RUNNING,
                                                               ring_hash_new = leo_hex:integer_to_hex(RingHash0),
                                                               ring_hash_old = leo_hex:integer_to_hex(RingHash1),
                                                               when_is       = leo_date:now()});
        {ok, _} ->
            {error, ?ERR_TYPE_INCONSISTENT_HASH};
        Error ->
            Error
    end;
notify(_,_,_,_) ->
    {error, badarg}.


notify1(error, Node, NumOfErrors, Thresholds) when NumOfErrors >= Thresholds ->
    State = ?STATE_STOP,
    Cause = "could not modify storage status",

    case leo_manager_mnesia:update_storage_node_status(
           update_state, #node_state{node  = Node,
                                     state = State}) of
        ok ->
            Clock = leo_date:clock(),
            case leo_redundant_manager_api:update_member_by_node(Node, Clock, State) of
                ok ->
                    notify2(error, Node, Clock, State);
                _ ->
                    {error, Cause}
            end;
        _ ->
            {error, Cause}
    end;

notify1(error, Node,_NumOfErrors,_Thresholds) ->
    case leo_manager_mnesia:update_storage_node_status(increment_error, #node_state{node = Node}) of
        ok ->
            ok;
        _ ->
            {error, "could not modify storage status"}
    end.

notify2(error, Node, Clock, State) ->
    case get_nodes(Node) of
        {ok, []} ->
            ok;
        {ok, Nodes} ->
            _Res = rpc:multicall(Nodes, leo_redundant_manager_api,
                                 update_member_by_node, [Node, Clock, State], ?DEF_TIMEOUT),
            ok
    end.


%% @doc Retrieve assigned file information.
%%
-spec(whereis(list(), boolean()) ->
             {ok, any()} |
             {error, any()}).
whereis([Key|_], true) ->
    case leo_redundant_manager_api:get_redundancies_by_key(Key) of
        {ok, #redundancies{id = AddrId, nodes = Redundancies}} ->
            whereis1(AddrId, Key, Redundancies, []);
        undefined ->
            {error, ?ERROR_META_NOT_FOUND}
    end;

whereis(_Key, false) ->
    {error, ?ERROR_COULD_NOT_GET_RING};

whereis(_Key, _HasRoutingTable) ->
    {error, badarith}.

whereis1(_, _, [],Acc) ->
    {ok, lists:reverse(Acc)};

whereis1(AddrId, Key, [{Node, true }|T], Acc) ->
    NodeStr = atom_to_list(Node),
    RPCKey  = rpc:async_call(Node, leo_storage_handler_object,
                             head, [AddrId, Key]),
    Reply   = case rpc:nb_yield(RPCKey, ?DEF_TIMEOUT) of
                  {value, {ok, #metadata{addr_id   = AddrId,
                                         dsize     = DSize,
                                         clock     = Clock,
                                         timestamp = Timestamp,
                                         checksum  = Checksum,
                                         del       = DelFlag}}} ->
                      {NodeStr, AddrId, DSize, Clock, Timestamp, Checksum, DelFlag};
                  _ ->
                      {NodeStr, not_found}
              end,
    whereis1(AddrId, Key, T, [Reply | Acc]);

whereis1(AddrId, Key, [{Node, false}|T], Acc) ->
    whereis1(AddrId, Key, T, [{atom_to_list(Node), not_found} | Acc]).


%% @doc Do compact.
%%
-spec(compact(string() | atom()) ->
             {ok, list()} | {error, any}).
compact([]) ->
    {error, not_found};

compact(Node) when is_list(Node) ->
    compact(list_to_atom(Node));

compact(Node) ->
    case leo_misc:node_existence(Node) of
        true ->
            case rpc:call(Node, leo_object_storage_api, compact, [], infinity) of
                Result when is_list(Result) ->
                    {ok, Result};
                {error, _} ->
                    {error, ?ERROR_FAILED_COMPACTION}
            end;
        false ->
            {error, ?ERR_TYPE_NODE_DOWN}
    end.


%% @doc get storage stats.
%%
-spec(stats(summary | detail, string() | atom()) ->
             {ok, list()} | {error, any}).
stats(_, []) ->
    {error, not_found};

stats(Mode, Node) when is_list(Node) ->
    stats(Mode, list_to_atom(Node));

stats(Mode, Node) ->
    case leo_misc:node_existence(Node) of
        true ->
            case rpc:call(Node, leo_object_storage_api, stats, [], infinity) of
                not_found = Cause ->
                    {error, Cause};
                {ok, []} ->
                    {error, not_found};
                {ok, Result} ->
                    stats1(Mode, Result)
            end;
        false ->
            {error, ?ERR_TYPE_NODE_DOWN}
    end.

stats1(summary, List) ->
    {ok, lists:foldl(fun({ok, #storage_stats{total_sizes = FileSize,
                                             total_num   = ObjTotal}}, {CurSize, CurTotal}) ->
                             {CurSize + FileSize, CurTotal + ObjTotal}
                     end, {0,0}, List)};
stats1(detail, List) ->
    {ok, List}.

%% @doc Synchronize Members and Ring (both New and Old).
%%
synchronize(Type) when Type == ?CHECKSUM_RING;
                       Type == ?CHECKSUM_MEMBER ->
    case leo_redundant_manager_api:get_members() of
        {ok, Members} ->
            lists:map(fun(#member{node  = Node,
                                  state = ?STATE_RUNNING}) ->
                              synchronize(Type, Node, Members);
                         (_) ->
                              ok
                      end, Members);
        Error ->
            Error
    end.

synchronize(Type, Node, Members) when Type == ?CHECKSUM_RING;
                                      Type == ?CHECKSUM_MEMBER ->
    {ok, SystemConf} = leo_manager_mnesia:get_system_config(),
    Options = [{n, SystemConf#system_conf.n},
               {r, SystemConf#system_conf.r},
               {w, SystemConf#system_conf.w},
               {d, SystemConf#system_conf.d},
               {bit_of_ring, SystemConf#system_conf.bit_of_ring}],

    case rpc:call(Node, leo_redundant_manager_api, synchronize,
                  [?SYNC_MODE_BOTH, Members, Options], ?DEF_TIMEOUT) of
        {ok, _Members, Chksums} ->
            {RingHash0, RingHash1} = proplists:get_value(?CHECKSUM_RING, Chksums),

            leo_manager_mnesia:update_storage_node_status(
              update_chksum, #node_state{node          = Node,
                                         ring_hash_new = leo_hex:integer_to_hex(RingHash0),
                                         ring_hash_old = leo_hex:integer_to_hex(RingHash1)
                                        }),
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
%%
synchronize(?CHECKSUM_MEMBER, Node) when is_atom(Node) ->
    synchronize1(?SYNC_MODE_MEMBERS, Node);

synchronize(?CHECKSUM_RING, Node) when is_atom(Node) ->
    synchronize1(?SYNC_MODE_CUR_RING,  Node),
    synchronize1(?SYNC_MODE_PREV_RING, Node);

%% @doc From gateway and storage-node
%%
synchronize(?CHECKSUM_MEMBER = Type, [{Node0, Checksum0},
                                      {Node1, Checksum1}]) ->
    case leo_redundant_manager_api:checksum(Type) of
        {ok, LocalChecksum} ->
            compare_local_chksum_with_remote_chksum(?SYNC_MODE_MEMBERS, Node0, LocalChecksum, Checksum0),
            compare_local_chksum_with_remote_chksum(?SYNC_MODE_MEMBERS, Node1, LocalChecksum, Checksum1);
        Error ->
            Error
    end;

synchronize(?CHECKSUM_RING = Type, [{Node0, {CurRingHash0, PrevRingHash0}},
                                    {Node1, {CurRingHash1, PrevRingHash1}}]) ->
    case leo_redundant_manager_api:checksum(Type) of
        {ok, {LocalCurRingHash, LocalPrevRingHash}} ->
            %% copare manager-cur-ring-hash with remote cur-ring-hash
            _ = compare_local_chksum_with_remote_chksum(
                  ?SYNC_MODE_CUR_RING,  Node0, LocalCurRingHash,  CurRingHash0),
            _ = compare_local_chksum_with_remote_chksum(
                  ?SYNC_MODE_CUR_RING,  Node1, LocalCurRingHash,  CurRingHash1),

            %% copare manager-cur/prev-ring-hash/ with remote prev-ring-hash
            _ = compare_local_chksum_with_remote_chksum(
                  ?SYNC_MODE_PREV_RING, Node0, LocalCurRingHash, LocalPrevRingHash, PrevRingHash0),
            _ = compare_local_chksum_with_remote_chksum(
                  ?SYNC_MODE_PREV_RING, Node1, LocalCurRingHash, LocalPrevRingHash, PrevRingHash1);
        Error ->
            Error
    end.

%% @doc Synchronize members-list or rings
%% @private
-spec(synchronize1(?SYNC_MODE_MEMBERS|?SYNC_MODE_CUR_RING|?SYNC_MODE_PREV_RING, atom()) ->
             ok | {error, any()}).
synchronize1(?SYNC_MODE_MEMBERS = Type, Node) ->
    case leo_redundant_manager_api:get_members(?VER_CURRENT) of
        {ok, Members} ->
            case rpc:call(Node, leo_redundant_manager_api, synchronize, [Type, Members], ?DEF_TIMEOUT) of
                {ok, _} ->
                    ok;
                {_, Cause} ->
                    {error, Cause};
                timeout = Cause ->
                    {error, Cause}
            end;
        Error ->
            Error
    end;

synchronize1(Type, Node) when Type == ?SYNC_MODE_CUR_RING;
                              Type == ?SYNC_MODE_PREV_RING ->
    case leo_redundant_manager_api:get_ring(Type) of
        {ok, Ring} ->
            case rpc:call(Node, leo_redundant_manager_api, synchronize,
                          [Type, Ring], ?DEF_TIMEOUT) of
                {ok, {RingHash0, RingHash1}} ->
                    case leo_manager_mnesia:get_gateway_node_by_name(Node) of
                        {ok, [NodeState|_]} ->
                            _ = leo_manager_mnesia:update_gateway_node(
                                  NodeState#node_state{ring_hash_new = leo_hex:integer_to_hex(RingHash0),
                                                       ring_hash_old = leo_hex:integer_to_hex(RingHash1)});
                        _ ->
                            case leo_manager_mnesia:get_storage_node_by_name(Node) of
                                {ok, _} ->
                                    _ = leo_manager_mnesia:update_storage_node_status(
                                          update_chksum, #node_state{node  = Node,
                                                                     ring_hash_new = leo_hex:integer_to_hex(RingHash0),
                                                                     ring_hash_old = leo_hex:integer_to_hex(RingHash1)});
                                _ ->
                                    void
                            end
                    end,
                    ok;
                {_, Cause} ->
                    {error, Cause};
                timeout = Cause ->
                    {error, Cause}
            end;
        Error ->
            Error
    end;
synchronize1(_,_) ->
    {error, badarg}.


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
            rpc:multicall(Nodes, leo_gateway_api, purge, [Path], ?DEF_TIMEOUT),
            ok;
        _ ->
            {error, "could not get gateway-nodes"}
    end.

%% @doc
%% @private
compare_local_chksum_with_remote_chksum(_Type,_Node, Checksum0, Checksum1) when Checksum0 =:= Checksum1 ->
    ok;
compare_local_chksum_with_remote_chksum( Type, Node, Checksum0, Checksum1) when Checksum0 =/= Checksum1 ->
    synchronize1(Type, Node).

compare_local_chksum_with_remote_chksum(_Type,_Node, Checksum0, Checksum1, RemoteChecksum)
  when Checksum0 =:= RemoteChecksum orelse
       Checksum1 =:= RemoteChecksum ->
    ok;
compare_local_chksum_with_remote_chksum( Type, Node,_Checksum0,_Checksum1,_RemoteChecksum) ->
    synchronize1(Type, Node).

