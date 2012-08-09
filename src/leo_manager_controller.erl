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
%% LeoFS Manager - Controller.
%% @doc
%% @end
%%======================================================================
-module(leo_manager_controller).

-author('Yosuke Hara').

-include("leo_manager.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/1, stop/0]).
-export([init/1, handle_call/3]).

-define(ERROR_COULD_NOT_ATTACH_NODE, "could not attach a node").
-define(ERROR_COULD_NOT_DETACH_NODE, "could not detach a node").
-define(ERROR_COMMAND_NOT_FOUND,     "command not exist").
-define(ERROR_NO_NODE_SPECIFIED,     "no node specified").
-define(ERROR_NO_PATH_SPECIFIED,     "no path specified").


%%----------------------------------------------------------------------
%%
%%----------------------------------------------------------------------
start_link(Params) ->
    tcp_server:start_link(?MODULE, [], Params).

stop() ->
    tcp_server:stop().

%%----------------------------------------------------------------------
%% Callback function(s)
%%----------------------------------------------------------------------
init(_Args) ->
    {ok, []}.


%%----------------------------------------------------------------------
%% Operation-1
%%----------------------------------------------------------------------
handle_call(_Socket, <<?HELP>>, State) ->
    Commands = []
        ++ io_lib:format("[Whole System]\r\n", [])
        ++ io_lib:format("~s\r\n",["version"])
        ++ io_lib:format("~s\r\n",["status"])
        ++ io_lib:format("~s\r\n",["history"])
        ++ io_lib:format("~s\r\n",["quit"])
        ++ io_lib:format("\r\n",  [])

        ++ io_lib:format("[Cluster]\r\n", [])
        ++ io_lib:format("~s\r\n",["detach ${NODE}"])
        ++ io_lib:format("~s\r\n",["suspend ${NODE}"])
        ++ io_lib:format("~s\r\n",["resume ${NODE}"])
        ++ io_lib:format("~s\r\n",["start"])
        ++ io_lib:format("~s\r\n",["rebalance"])
        ++ io_lib:format("~s\r\n",["whereis ${PATH}"])
        ++ io_lib:format("\r\n",  [])

        ++ io_lib:format("[Storage]\r\n", [])
        ++ io_lib:format("~s\r\n",["du ${NODE}"])
        ++ io_lib:format("~s\r\n",["compact ${NODE}"])
        ++ io_lib:format("\r\n",  [])

        ++ io_lib:format("[Gateway]\r\n", [])
        ++ io_lib:format("~s\r\n",["purge ${PATH}"])
        ++ io_lib:format("\r\n",  [])

        ++ io_lib:format("[S3]\r\n", [])
        ++ io_lib:format("~s\r\n",["s3-gen-key ${USER-ID}"])
        ++ io_lib:format("~s\r\n",["s3-set-endpoint ${ENDPOINT}"])
        ++ io_lib:format("~s\r\n",["s3-delete-endpoint ${ENDPOINT}"])
        ++ io_lib:format("~s\r\n",["s3-get-endpoints"])
        ++ io_lib:format("\r\n",  [])
        ++ ?CRLF,
    {reply, Commands, State};


handle_call(_Socket, <<?VERSION, _Option/binary>>, State) ->
    Reply = case application:get_key(leo_manager, vsn) of
                {ok, Version} ->
                    io_lib:format("~s\r\n",[Version]);
                _ ->
                    []
            end,
    {reply, Reply, State};


handle_call(_Socket, <<?STATUS, Option/binary>> = Command, State) ->
    _ = leo_manager_mnesia:insert_history(Command),
    {ok, SystemConf} = leo_manager_mnesia:get_system_config(),
    Token = string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER),
    Reply = case (erlang:length(Token) == 0) of
                true ->
                    format_cluster_node_list(SystemConf);
                false ->
                    [Node|_] = Token,
                    case leo_manager_api:get_cluster_node_status(Node) of
                        {ok, Status} ->
                            format_cluster_node_state(Status);
                        {error, Cause} ->
                            io_lib:format("[ERROR] ~s\r\n", [Cause])
                    end
            end,
    {reply, Reply, State};


handle_call(_Socket, <<?DETACH_SERVER, Option/binary>> = Command, State) ->
    _ = leo_manager_mnesia:insert_history(Command),
    {ok, SystemConf} = leo_manager_mnesia:get_system_config(),

    Reply =
        case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
            [] ->
                io_lib:format("[ERROR] ~s\r\n",[?ERROR_NO_NODE_SPECIFIED]);
            [Node|_] ->
                NodeAtom = list_to_atom(Node),

                case leo_manager_mnesia:get_storage_node_by_name(NodeAtom) of
                    {ok, [#node_state{state = ?STATE_ATTACHED} = NodeState|_]} ->
                        ok = leo_manager_cluster_monitor:demonitor(NodeAtom),
                        ok = leo_manager_mnesia:delete_storage_node(NodeState),
                        ?OK;
                    _ ->
                        case leo_manager_mnesia:get_storage_nodes_by_status(?STATE_RUNNING) of
                            {ok, Nodes} when length(Nodes) > SystemConf#system_conf.n ->
                                case leo_manager_api:detach(NodeAtom) of
                                    ok ->
                                        ?OK;
                                    {error, _} ->
                                        io_lib:format("[ERROR] ~s - ~s\r\n", [?ERROR_COULD_NOT_DETACH_NODE, Node])
                                end;
                            {ok, Nodes} when length(Nodes) =< SystemConf#system_conf.n ->
                                io_lib:format("[ERROR] ~s\r\n",["Attached nodes less than # of replicas"]);
                            _Error ->
                                io_lib:format("[ERROR] ~s\r\n",["Could not get node-status"])
                        end
                end
        end,
    {reply, Reply, State};


handle_call(_Socket, <<?SUSPEND, Option/binary>> = Command, State) ->
    _ = leo_manager_mnesia:insert_history(Command),

    Reply = case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
                [] ->
                    io_lib:format("[ERROR] ~s\r\n",[?ERROR_NO_NODE_SPECIFIED]);
                [Node|_] ->
                    case leo_manager_api:suspend(list_to_atom(Node)) of
                        ok ->
                            ?OK;
                        {error, Cause} ->
                            io_lib:format("[ERROR] ~s\r\n",[Cause])
                    end
            end,
    {reply, Reply, State};


handle_call(_Socket, <<?RESUME, Option/binary>> = Command, State) ->
    _ = leo_manager_mnesia:insert_history(Command),

    Reply = case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
                [] ->
                    io_lib:format("[ERROR] ~s\r\n",[?ERROR_NO_NODE_SPECIFIED]);
                [Node|_] ->
                    case leo_manager_api:resume(list_to_atom(Node)) of
                        ok ->
                            ?OK;
                        {error, Cause} ->
                            io_lib:format("[ERROR] ~s\r\n",[Cause])
                    end
            end,
    {reply, Reply, State};


handle_call(_Socket, <<?START, _Option/binary>> = Command, State) ->
    _ = leo_manager_mnesia:insert_history(Command),

    Reply = case leo_manager_api:get_system_status() of
                ?STATE_STOP ->
                    {ok, SystemConf} = leo_manager_mnesia:get_system_config(),

                    case leo_manager_mnesia:get_storage_nodes_by_status(?STATE_ATTACHED) of
                        {ok, Nodes} when length(Nodes) >= SystemConf#system_conf.n ->
                            case leo_manager_api:start() of
                                {error, Cause} ->
                                    io_lib:format("[ERROR] ~s\r\n",[Cause]);
                                {_ResL, []} ->
                                    ?OK;
                                {_ResL, BadNodes} ->
                                    lists:foldl(fun(Node, Acc) ->
                                                        Acc ++ io_lib:format("[ERROR] ~w\r\n", [Node])
                                                end, [], BadNodes)
                            end;
                        {ok, Nodes} when length(Nodes) < SystemConf#system_conf.n ->
                            io_lib:format("[ERROR] ~s\r\n",["Attached nodes less than # of replicas"]);
                        _Error ->
                            io_lib:format("[ERROR] ~s\r\n",["Could not get node-status"])
                    end;
                ?STATE_RUNNING ->
                    io_lib:format("[ERROR] ~s\r\n",["System already started"])
            end,
    {reply, Reply, State};


handle_call(_Socket, <<?REBALANCE, _Option/binary>> = Command, State) ->
    _ = leo_manager_mnesia:insert_history(Command),

    Reply = case leo_redundant_manager_api:checksum(?CHECKSUM_RING) of
                {ok, {CurRingHash, PrevRingHash}} when CurRingHash =/= PrevRingHash ->
                    case leo_manager_api:rebalance() of
                        ok ->
                            ?OK;
                        _Other ->
                            io_lib:format("[ERROR] ~s\r\n",["fail rebalance"])
                    end;
                _Other ->
                    io_lib:format("[ERROR] ~s\r\n",["could not start"])
            end,
    {reply, Reply, State};


%%----------------------------------------------------------------------
%% Operation-2
%%----------------------------------------------------------------------
handle_call(_Socket, <<?STORAGE_STATS, Option/binary>> = Command, State) ->
    _ = leo_manager_mnesia:insert_history(Command),

    {Reply, NewState} =
        case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
            [] ->
                {io_lib:format("[ERROR] ~s\r\n",[?ERROR_NO_NODE_SPECIFIED]), State};
            Tokens ->
                Res = case length(Tokens) of
                          1 -> {summary, lists:nth(1, Tokens)};
                          2 -> {list_to_atom(lists:nth(1, Tokens)),  lists:nth(2, Tokens)};
                          _ -> {error, badarg}
                      end,

                case Res of
                    {error, _Cause} ->
                        {io_lib:format("[ERROR] ~s\r\n",[?ERROR_COMMAND_NOT_FOUND]), State};
                    {Option1, Node1} ->
                        case leo_manager_api:stats(Option1, Node1) of
                            {ok, StatsList} ->
                                {format_stats_list(Option1, StatsList), State};
                            {error, Cause} ->
                                {io_lib:format("[ERROR] ~s\r\n",[Cause]), State}
                        end
                end
        end,
    {reply, Reply, NewState};


handle_call(_Socket, <<?COMPACT, Option/binary>> = Command, State) ->
    _ = leo_manager_mnesia:insert_history(Command),

    {Reply, NewState} =
        case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
            [] ->
                {io_lib:format("[ERROR] ~s\r\n",[?ERROR_NO_NODE_SPECIFIED]),State};
            [Node|_] ->
                case leo_manager_api:compact(Node) of
                    {ok, _} ->
                        {?OK, State};
                    {error, Cause} ->
                        {io_lib:format("[ERROR] ~s\r\n",[Cause]), State}
                end
        end,
    {reply, Reply, NewState};


handle_call(_Socket, <<?S3_GEN_KEY, Option/binary>> = Command, State) ->
    _ = leo_manager_mnesia:insert_history(Command),

    {Reply, NewState} =
        case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
            [] ->
                {io_lib:format("[ERROR] ~s\r\n",["no user specified"]),State};
            [UserId|_] ->
                case leo_s3_auth:gen_key(UserId) of
                    {ok, Keys} ->
                        AccessKeyId     = proplists:get_value(access_key_id,     Keys),
                        SecretAccessKey = proplists:get_value(secret_access_key, Keys),
                        {io_lib:format("access-key-id: ~s\r\n"
                                       ++ "secret-access-key: ~s\r\n\r\n",
                                       [AccessKeyId, SecretAccessKey]), State};
                    {error, Cause} ->
                        {io_lib:format("[ERROR] ~s\r\n",[Cause]), State}
                end
        end,
    {reply, Reply, NewState};

handle_call(_Socket, <<?S3_SET_ENDPOINT, Option/binary>> = Command, State) ->
    _ = leo_manager_mnesia:insert_history(Command),

    {Reply, NewState} =
        case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
            [] ->
                {io_lib:format("[ERROR] ~s\r\n",[?ERROR_COMMAND_NOT_FOUND]),State};
            [EndPoint|_] ->
                case leo_s3_endpoint:set_endpoint(EndPoint) of
                    ok ->
                        {?OK, State};
                    {error, Cause} ->
                        {io_lib:format("[ERROR] ~s\r\n",[Cause]), State}
                end
        end,
    {reply, Reply, NewState};

handle_call(_Socket, <<?S3_DEL_ENDPOINT, Option/binary>> = Command, State) ->
    _ = leo_manager_mnesia:insert_history(Command),

    {Reply, NewState} =
        case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
            [] ->
                {io_lib:format("[ERROR] ~s\r\n",[?ERROR_COMMAND_NOT_FOUND]),State};
            [EndPoint|_] ->
                case leo_s3_endpoint:delete_endpoint(EndPoint) of
                    ok ->
                        {?OK, State};
                    {error, Cause} ->
                        {io_lib:format("[ERROR] ~s\r\n",[Cause]), State}
                end
        end,
    {reply, Reply, NewState};

handle_call(_Socket, <<?S3_GET_ENDPOINTS, _Option/binary>> = Command, State) ->
    _ = leo_manager_mnesia:insert_history(Command),

    {Reply, NewState} =
        case leo_s3_endpoint:get_endpoints() of
            {ok, EndPoints} ->
                {format_endpoint_list(EndPoints), State};
            not_found ->
                {io_lib:format("not found\r\n", []), State};
            {error, Cause} ->
                {io_lib:format("[ERROR] ~s\r\n",[Cause]), State}
        end,
    {reply, Reply, NewState};


handle_call(_Socket, <<?WHEREIS, Option/binary>> = Command, State) ->
    _ = leo_manager_mnesia:insert_history(Command),

    Reply = case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
                [] ->
                    io_lib:format("[ERROR] ~s\r\n",[?ERROR_NO_PATH_SPECIFIED]);
                Key ->
                    HasRoutingTable = (leo_redundant_manager_api:checksum(ring) >= 0),

                    case catch leo_manager_api:whereis(Key, HasRoutingTable) of
                        {ok, AssignedInfo} ->
                            format_where_is(AssignedInfo);
                        {error, Cause} ->
                            io_lib:format("[ERROR] ~s\r\n", [Cause]);
                        _ ->
                            io_lib:format("[ERROR] ~s\r\n", [?ERROR_COMMAND_NOT_FOUND])
                    end
            end,
    {reply, Reply, State};

handle_call(_Socket, <<?PURGE, Option/binary>> = Command, State) ->
    _ = leo_manager_mnesia:insert_history(Command),
    Reply = case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
                [] ->
                    io_lib:format("[ERROR] ~s\r\n",[?ERROR_NO_PATH_SPECIFIED]);
                [Key|_] ->
                    case catch leo_manager_api:purge(Key) of
                        ok ->
                            ?OK;
                        {error, Cause} ->
                            io_lib:format("[ERROR] ~s\r\n", [Cause]);
                        _ ->
                            io_lib:format("[ERROR] ~s\r\n", [?ERROR_COMMAND_NOT_FOUND])
                    end
            end,
    {reply, Reply, State};


handle_call(_Socket, <<?HISTORY, _Option/binary>>, State) ->
    Reply = case leo_manager_mnesia:get_histories_all() of
                {ok, Histories} ->
                    format_history_list(Histories) ++ "\r\n";
                {error, Cause} ->
                    io_lib:format("[ERROR] ~p\r\n", [Cause])
            end,
    {reply, Reply, State};


handle_call(_Socket, <<?QUIT>>, State) ->
    {close, <<?BYE>>, State};


handle_call(_Socket, <<?CRLF>>, State) ->
    {reply, "", State};


handle_call(_Socket, _Data, State) ->
    Reply = io_lib:format("[ERROR] ~s\r\n",[?ERROR_COMMAND_NOT_FOUND]),
    {reply, Reply, State}.

%%----------------------------------------------------------------------
%% Inner function(s)
%%----------------------------------------------------------------------
format_cluster_node_list(SystemConf) ->
    FormattedSystemConf =
        case is_record(SystemConf, system_conf) of
            true ->
                Version = case application:get_key(leo_manager, vsn) of
                              {ok, Vsn} -> Vsn;
                              undefined -> []
                          end,
                {ok, {RingHash0, RingHash1}} = leo_redundant_manager_api:checksum(ring),

                io_lib:format("[system config]\r\n"               ++
                                  "             version : ~s\r\n"     ++
                                  " # of replicas       : ~w\r\n"     ++
                                  " # of successes of R : ~w\r\n"     ++
                                  " # of successes of W : ~w\r\n"     ++
                                  " # of successes of D : ~w\r\n"     ++
                                  "           ring size : 2^~w\r\n"   ++
                                  "    ring hash (cur)  : ~s\r\n"     ++
                                  "    ring hash (prev) : ~s\r\n\r\n" ++
                                  "[node(s) state]\r\n",
                              [Version,
                               SystemConf#system_conf.n,
                               SystemConf#system_conf.r,
                               SystemConf#system_conf.w,
                               SystemConf#system_conf.d,
                               SystemConf#system_conf.bit_of_ring,
                               leo_hex:integer_to_hex(RingHash0),
                               leo_hex:integer_to_hex(RingHash1)
                              ]);
            false ->
                []
        end,

    S1 = case leo_manager_mnesia:get_storage_nodes_all() of
             {ok, R1} ->
                 lists:map(fun(N) ->
                                   {atom_to_list(N#node_state.node),
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
                                   {atom_to_list(N#node_state.node),
                                    atom_to_list(N#node_state.state),
                                    N#node_state.ring_hash_new,
                                    N#node_state.ring_hash_old,
                                    N#node_state.when_is}
                           end, R2);
             _ ->
                 []
         end,
    format_system_conf_with_node_state(FormattedSystemConf, S1 ++ S2).


format_cluster_node_state(State) ->
    ObjStoragePath = State#cluster_node_status.avs,
    Directories    = State#cluster_node_status.dirs,
    RingHashes     = State#cluster_node_status.ring_checksum,
    Statistics     = State#cluster_node_status.statistics,

    io_lib:format("[config]\r\n" ++
                      "            version : ~s\r\n" ++
                      "  obj-container-dir : ~s"     ++
                      "            log-dir : ~s\r\n" ++
                      "         mnesia-dir : ~s\r\n" ++
                      "  ring state (cur)  : ~w\r\n" ++
                      "  ring state (prev) : ~w\r\n" ++
                      "\r\n[erlang-vm status]\r\n"   ++
                      "    total mem usage : ~w\r\n" ++
                      "   system mem usage : ~w\r\n" ++
                      "    procs mem usage : ~w\r\n" ++
                      "      ets mem usage : ~w\r\n" ++
                      "    # of procs      : ~w\r\n\r\n",
                  [State#cluster_node_status.version,
                   ObjStoragePath,
                   proplists:get_value('log',              Directories, []),
                   proplists:get_value('mnesia',           Directories, []),
                   proplists:get_value('ring_cur',         RingHashes,  []),
                   proplists:get_value('ring_prev',        RingHashes,  []),
                   proplists:get_value('total_mem_usage',  Statistics, 0),
                   proplists:get_value('system_mem_usage', Statistics, 0),
                   proplists:get_value('proc_mem_usage',   Statistics, 0),
                   proplists:get_value('ets_mem_usage',    Statistics, 0),
                   proplists:get_value('num_of_procs',     Statistics, 0)
                  ]).


format_system_conf_with_node_state(FormattedSystemConf, Nodes) ->
    CellColumns =
        [{"node",28},{"state",12},{"ring (cur)",14},{"ring (prev)",14},{"when",28},{"END",0}],
    LenPerCol = lists:map(fun(C)->{_, Len} = C, Len end, CellColumns),
    Fun1 = fun(Col, {Type,Str}) ->
                   {Name, Len} = Col,
                   case Name of
                       "END" when Type =:= title -> {Type, Str ++ ?CRLF};
                       _ when Type =:= title andalso
                              Name =:= "node"-> {Type, " " ++ Str ++ string:left(Name, Len, $ )};
                       _ when Type =:= title -> {Type,        Str ++ string:left(Name, Len, $ )}
                   end
           end,
    {_, Header2} = lists:foldl(Fun1, {title,[]}, CellColumns),
    Sepalator = lists:foldl(
                  fun(N, L) -> L ++ N  end,
                  [], lists:duplicate(lists:sum(LenPerCol), "-")) ++ ?CRLF,

    Fun2 = fun(N, List) ->
                   {Alias, State, RingHash0, RingHash1, When} = N,
                   FormattedDate = leo_utils:date_format(When),

                   Ret = " " ++ string:left(Alias,         lists:nth(1,LenPerCol), $ )
                       ++ string:left(State,         lists:nth(2,LenPerCol), $ )
                       ++ string:left(RingHash0,     lists:nth(3,LenPerCol), $ )
                       ++ string:left(RingHash1,     lists:nth(4,LenPerCol), $ )
                       ++ string:left(FormattedDate, lists:nth(5,LenPerCol), $ )
                       ++ ?CRLF,
                   List ++ [Ret]
           end,
    _FormattedList =
        lists:foldl(Fun2, [FormattedSystemConf, Sepalator, Header2, Sepalator], Nodes) ++ ?CRLF.


format_where_is(AssignedInfo) ->
    CellColumns =
        [{"del?",5}, {"node",28},{"ring address",36},{"size",8},{"checksum",12},{"clock",14},{"when",28},{"END",0}],
    LenPerCol = lists:map(fun(C)->{_, Len} = C, Len end, CellColumns),
    Fun1 = fun(Col, {Type,Str}) ->
                   {Name, Len} = Col,
                   case Name of
                       "END" when Type =:= title -> {Type, Str ++ ?CRLF};
                       _ when Type =:= title andalso
                              Name =:= "node"-> {Type, " " ++ Str ++ string:left(Name, Len, $ )};
                       _ when Type =:= title -> {Type,        Str ++ string:left(Name, Len, $ )}
                   end
           end,
    {_, Header2} = lists:foldl(Fun1, {title,[]}, CellColumns),
    Sepalator = lists:foldl(
                  fun(N, L) -> L ++ N  end,
                  [], lists:duplicate(lists:sum(LenPerCol), "-")) ++ ?CRLF,

    Fun2 = fun(N, List) ->
                   Ret = case N of
                             {Node, not_found} ->
                                 " " ++ string:left("", lists:nth(1,LenPerCol))
                                     ++ Node
                                     ++ ?CRLF;

                             {Node, VNodeId, DSize, Clock, Timestamp, Checksum, DelFlag} ->
                                 FormattedDate = leo_utils:date_format(Timestamp),
                                 DelStr = case DelFlag of
                                              0 -> " ";
                                              _ -> "*"
                                          end,

                                 " " ++ string:left(DelStr,                            lists:nth(1,LenPerCol))
                                     ++ string:left(Node,                              lists:nth(2,LenPerCol))
                                     ++ string:left(leo_hex:integer_to_hex(VNodeId),   lists:nth(3,LenPerCol))
                                     ++ string:left(dsize(DSize),                      lists:nth(4,LenPerCol))
                                     ++ string:left(
                                          string:sub_string(
                                            leo_hex:integer_to_hex(Checksum), 1, 10),  lists:nth(5,LenPerCol))
                                     ++ string:left(leo_hex:integer_to_hex(Clock),     lists:nth(6,LenPerCol))
                                     ++ string:left(FormattedDate,                     lists:nth(7,LenPerCol))
                                     ++ ?CRLF
                         end,
                   List ++ [Ret]
           end,
    _FormattedList =
        lists:foldl(Fun2, [Sepalator, Header2, Sepalator], AssignedInfo) ++ ?CRLF.


format_stats_list(summary, {FileSize, Total, Active}) ->
    io_lib:format(
      "              file size: ~w\r\n"
      ++ " number of total object: ~w\r\n"
      ++ "number of active object: ~w\r\n\r\n",
      [FileSize, Total, Active]);

format_stats_list(detail, StatsList) when is_list(StatsList) ->
    Fun = fun(Stats, Acc) ->
                  case Stats of
                      {ok, #storage_stats{file_path   = FilePath,
                                          total_sizes = FileSize,
                                          total_num   = ObjTotal,
                                          active_num  = ObjActive}} ->
                          Acc ++ io_lib:format("              file path: ~s\r\n"
                                               ++  "              file size: ~w\r\n"
                                               ++  " number of total object: ~w\r\n"
                                               ++  "number of active object: ~w\r\n\r\n",
                                               [FilePath, FileSize, ObjTotal, ObjActive]);
                      _Error ->
                          Acc
                  end
          end,
    lists:foldl(Fun, "[du(storage stats)]\r\n", StatsList).



format_history_list(Histories) ->
    Fun = fun(#history{id      = Id,
                       command = Command,
                       created = Created}, Acc) ->
                  Acc ++ io_lib:format("~s | ~s | ~s\r\n",
                                       [string:left(integer_to_list(Id), 4), leo_utils:date_format(Created), Command])
          end,
    lists:foldl(Fun, "[Histories]\r\n", Histories).


format_endpoint_list(EndPoints) ->
    Fun = fun({endpoint, EP, Created}, Acc) ->
                  Acc ++ io_lib:format("~s | ~s\r\n",
                                       [leo_utils:date_format(Created), EP])
          end,
    lists:foldl(Fun, "[EndPoints]\r\n", EndPoints).


%% @doc Retrieve data-size w/unit.
%% @private
-define(FILE_KB,       1024).
-define(FILE_MB,    1048586).
-define(FILE_GB, 1073741824).

dsize(Size) when Size =< ?FILE_KB -> integer_to_list(Size) ++ "B";
dsize(Size) when Size  > ?FILE_KB -> integer_to_list(erlang:round(Size / ?FILE_KB)) ++ "K";
dsize(Size) when Size  > ?FILE_MB -> integer_to_list(erlang:round(Size / ?FILE_MB)) ++ "M";
dsize(Size) when Size  > ?FILE_GB -> integer_to_list(erlang:round(Size / ?FILE_GB)) ++ "G".

