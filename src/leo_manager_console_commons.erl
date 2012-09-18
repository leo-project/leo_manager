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
%% LeoFS Manager - Console Commons
%% @doc
%% @end
%%======================================================================
-module(leo_manager_console_commons).

-author('Yosuke Hara').

-include("leo_manager.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([version/0, status/1, status/2, start/1,
         detach/2, suspend/2, resume/2, rebalance/1
        ]).


%%----------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------
%% @doc Retrieve version of the system
%%
-spec(version() ->
             {ok, string() | list()}).
version() ->
    case application:get_key(leo_manager, vsn) of
        {ok, Version} ->
            {ok, Version};
        _ ->
            {ok, []}
    end.


%% @doc Retrieve state of each node
%%
-spec(status(binary(), binary()) ->
             {ok, any()} | {error, any()}).
status(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),
    Token = string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER),

    case (erlang:length(Token) == 0) of
        true ->
            status(node_list);
        false ->
            [Node|_] = Token,
            status({node_state, Node})
    end.

status(node_list) ->
    {ok, SystemConf} = leo_manager_mnesia:get_system_config(),
    Version = case application:get_key(leo_manager, vsn) of
                  {ok, Vsn} -> Vsn;
                  undefined -> []
              end,
    {ok, {RingHash0, RingHash1}} = leo_redundant_manager_api:checksum(ring),

    S1 = case leo_manager_mnesia:get_storage_nodes_all() of
             {ok, R1} ->
                 lists:map(fun(N) ->
                                   {"S",
                                    atom_to_list(N#node_state.node),
                                    atom_to_list(N#node_state.state),
                                    N#node_state.ring_hash_new,
                                    N#node_state.ring_hash_old,
                                    N#node_state.when_is}
                           end, R1);
             _ ->
                 []
         end,
    S2 = case leo_manager_mnesia:get_gateway_nodes_all() of
             {ok, R2} ->
                 lists:map(fun(N) ->
                                   {"G",
                                    atom_to_list(N#node_state.node),
                                    atom_to_list(N#node_state.state),
                                    N#node_state.ring_hash_new,
                                    N#node_state.ring_hash_old,
                                    N#node_state.when_is}
                           end, R2);
             _ ->
                 []
         end,
    {ok, {node_list, [{system_config, SystemConf},
                      {version,       Version},
                      {ring_hash,     [RingHash0, RingHash1]},
                      {nodes,         S1 ++ S2}
                     ]}};

status({node_state, Node}) ->
    case leo_manager_api:get_node_status(Node) of
        {ok, State} ->
            {ok, State};
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Launch the storage cluster
%%
-spec(start(binary()) ->
             ok | {error, any()}).
start(CmdBody) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case leo_manager_api:get_system_status() of
        ?STATE_STOP ->
            {ok, SystemConf} = leo_manager_mnesia:get_system_config(),

            case leo_manager_mnesia:get_storage_nodes_by_status(?STATE_ATTACHED) of
                {ok, Nodes} when length(Nodes) >= SystemConf#system_conf.n ->
                    case leo_manager_api:start() of
                        {error, Cause} ->
                            {error, Cause};
                        {_ResL, []} ->
                            ok;
                        {_ResL, BadNodes} ->
                            {error, {bad_nodes, lists:foldl(fun(Node, Acc) ->
                                                                    Acc ++ [Node]
                                                            end, [], BadNodes)}}
                    end;
                {ok, Nodes} when length(Nodes) < SystemConf#system_conf.n ->
                    {error, "Attached nodes less than # of replicas"};
                Error ->
                    Error
            end;
        ?STATE_RUNNING ->
            {error, "System already started"}
    end.


%% @doc Detach a storage-node
%%
-spec(detach(binary(), binary()) ->
             ok | {error, {atom(), string()}} | {error, any()}).
detach(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),
    {ok, SystemConf} = leo_manager_mnesia:get_system_config(),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [] ->
            {error, ?ERROR_NO_NODE_SPECIFIED};
        [Node|_] ->
            NodeAtom = list_to_atom(Node),

            case leo_manager_mnesia:get_storage_node_by_name(NodeAtom) of
                {ok, [#node_state{state = ?STATE_ATTACHED} = NodeState|_]} ->
                    ok = leo_manager_mnesia:delete_storage_node(NodeState),
                    ok = leo_manager_cluster_monitor:demonitor(NodeAtom),
                    ok;
                _ ->
                    case leo_manager_mnesia:get_storage_nodes_by_status(?STATE_RUNNING) of
                        {ok, Nodes} when length(Nodes) >= SystemConf#system_conf.n ->
                            case leo_manager_api:detach(NodeAtom) of
                                ok ->
                                    ok;
                                {error, _} ->
                                    {error, {Node, ?ERROR_COULD_NOT_DETACH_NODE}}
                            end;
                        {ok, Nodes} when length(Nodes) =< SystemConf#system_conf.n ->
                            {error, "Attached nodes less than # of replicas"};
                        _Error ->
                            {error, "Could not get node-status"}
                    end
            end
    end.


%% @doc Suspend a storage-node
%%
-spec(suspend(binary(), binary()) ->
             ok | {error, any()}).
suspend(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [] ->
            {error, ?ERROR_NO_NODE_SPECIFIED};
        [Node|_] ->
            case leo_manager_api:suspend(list_to_atom(Node)) of
                ok ->
                    ok;
                {error, Cause} ->
                    {error, Cause}
            end
    end.


%% @doc Resume a storage-node
%%
-spec(resume(binary(), binary()) ->
             ok | {error, any()}).
resume(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [] ->
            {error, ?ERROR_NO_NODE_SPECIFIED};
        [Node|_] ->
            case leo_manager_api:resume(list_to_atom(Node)) of
                ok ->
                    ok;
                {error, Cause} ->
                    {error, Cause}
            end
    end.


%% @doc Rebalance the storage cluster
%%
-spec(rebalance(binary()) ->
             ok | {error, any()}).
rebalance(CmdBody) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case leo_redundant_manager_api:checksum(?CHECKSUM_RING) of
        {ok, {CurRingHash, PrevRingHash}} when CurRingHash =/= PrevRingHash ->
            case leo_manager_api:rebalance() of
                ok ->
                    ok;
                _Other ->
                    {error, "Fail rebalance"}
            end;
        _Other ->
            {error, "Could not launch the storage"}
    end.


