%%======================================================================
%%
%% Leo Manager
%%
%% Copyright (c) 2012-2014 Rakuten, Inc.
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
%% LeoFS Manager - CUI Formatter
%% @doc
%% @end
%%======================================================================
-module(leo_manager_formatter_text).

-author('Yosuke Hara').

-include("leo_manager.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("leo_s3_libs/include/leo_s3_user.hrl").
-include_lib("leo_s3_libs/include/leo_s3_bucket.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([ok/0, error/1, error/2, help/1, version/1,
         bad_nodes/1, system_info_and_nodes_stat/1, node_stat/2,
         compact_status/1, du/2, credential/2, users/1, endpoints/1,
         buckets/1, bucket_by_access_key/1,
         acls/1, cluster_status/1,
         whereis/1, histories/1,
         authorized/0, user_id/0, password/0
        ]).

%% @doc Format 'ok'
%%
-spec(ok() ->
             string()).
ok() ->
    ?OK.

%% @doc Format 'error'
%%
-spec(error(string()) ->
             string()).
error(Cause) when is_list(Cause) ->
    io_lib:format("[ERROR] ~s\r\n", [Cause]);
error(Cause) ->
    io_lib:format("[ERROR] ~p\r\n", [Cause]).


%% @doc Format 'error'
%%
-spec(error(atom() | string(), string()) ->
             string()).
error(Node, Cause) when is_atom(Node) ->
    io_lib:format("[ERROR] node:~w, ~s\r\n", [Node, Cause]);
error(Node, Cause) ->
    io_lib:format("[ERROR] node:~s, ~s\r\n", [Node, Cause]).


%% @doc Format 'help'
%%
-spec(help(undefined | atom()) ->
             string()).
help(PluginMod) ->
    Help_1 = lists:append([help("[Cluster Operation]\r\n",
                                [?CMD_DETACH,
                                 ?CMD_SUSPEND,
                                 ?CMD_RESUME,
                                 ?CMD_START,
                                 ?CMD_REBALANCE,
                                 ?CMD_WHEREIS,
                                 ?CMD_RECOVER], []),
                           help("[Storage Maintenance]\r\n",
                                [?CMD_DU,
                                 ?CMD_COMPACT], []),
                           help("[Gateway Maintenance]\r\n",
                                [?CMD_PURGE,
                                 ?CMD_REMOVE], []),
                           help("[Manager Maintenance]\r\n",
                                [?CMD_UPDATE_MANAGERS,
                                 ?CMD_BACKUP_MNESIA,
                                 ?CMD_RESTORE_MNESIA], []),
                           help("[S3-related Maintenance]\r\n",
                                [?CMD_CREATE_USER,
                                 ?CMD_DELETE_USER,
                                 ?CMD_UPDATE_USER_ROLE,
                                 ?CMD_UPDATE_USER_PW,
                                 ?CMD_GET_USERS,
                                 ?CMD_ADD_ENDPOINT,
                                 ?CMD_DEL_ENDPOINT,
                                 ?CMD_GET_ENDPOINTS,
                                 ?CMD_ADD_BUCKET,
                                 ?CMD_DELETE_BUCKET,
                                 ?CMD_GET_BUCKETS,
                                 ?CMD_GET_BUCKET_BY_ACCESS_KEY,
                                 ?CMD_CHANGE_BUCKET_OWNER,
                                 ?CMD_UPDATE_ACL], []),
                           help("[Multi-DC Replication]\r\n",
                                [?CMD_JOIN_CLUSTER,
                                 ?CMD_REMOVE_CLUSTER,
                                 ?CMD_CLUSTER_STAT
                                ], [])]),
    Help_2 = case PluginMod of
                 undefined -> [];
                 _ ->
                     PluginMod:help()
             end,
    Help_3 = lists:append([help("[Misc]\r\n",
                                [?CMD_VERSION,
                                 ?CMD_STATUS,
                                 ?CMD_HISTORY,
                                 ?CMD_DUMP_RING,
                                 ?CMD_QUIT], [])]),
    lists:append([Help_1, Help_2, Help_3]).


%% @private
help(_, [], []) ->
    [];
help(Header, [], Acc) ->
    lists:append([[Header], Acc, [?CRLF]]);
help(Header, [Command|Rest], Acc) ->
    case get_formatted_help(Command) of
        [] ->
            help(Header, Rest, Acc);
        Res ->
            Acc1 = lists:append([Acc, [Res]]),
            help(Header, Rest, Acc1)
    end.

%% @private
get_formatted_help(Command) ->
    case leo_manager_mnesia:get_available_command_by_name(Command) of
        {ok, [#cmd_state{help = Help}|_]} ->
            io_lib:format("~s\r\n", [Help]);
        _ ->
            []
    end.


%% Format 'version'
%%
-spec(version(string()) ->
             string()).
version(Version) ->
    lists:append([Version, ?CRLF]).


%% @doc Format 'bad nodes'
%%
-spec(bad_nodes(list()) ->
             string()).
bad_nodes(BadNodes) ->
    lists:foldl(fun(Node, Acc) ->
                        Acc ++ io_lib:format("[ERROR] ~w\r\n", [Node])
                end, [], BadNodes).


%% @doc Format a cluster-node list
%%
-spec(system_info_and_nodes_stat(list()) ->
             string()).
system_info_and_nodes_stat(Props) ->
    SystemConf = leo_misc:get_value('system_config', Props),
    Version    = leo_misc:get_value('version',       Props),
    [RH0, RH1] = leo_misc:get_value('ring_hash',     Props),
    Nodes      = leo_misc:get_value('nodes',         Props),

    %% Output format:
    %% [System config]
    %%                 System version : 1.0.0
    %%                     Cluster Id : leofs-1
    %%                          DC Id : dc-1
    %%                 Total replicas : 3
    %%            # of successes of R : 1
    %%            # of successes of W : 2
    %%            # of successes of D : 1
    %%  # of Rack-awareness replicas  : 0
    %%                      ring size : 2^128
    %%              Current ring hash : 41e0c107
    %%                 Prev ring hash : 41e0c107
    %% [Multi DC replication settings]
    %%        # of destination of DCs : 0
    %%        # of replicas to a DC   : 0
    FormattedSystemConf =
        io_lib:format(lists:append(["[System config]\r\n",
                                    "                System version : ~s\r\n",
                                    "                    Cluster Id : ~s\r\n",
                                    "                         DC Id : ~s\r\n",
                                    "                Total replicas : ~w\r\n",
                                    "           # of successes of R : ~w\r\n",
                                    "           # of successes of W : ~w\r\n",
                                    "           # of successes of D : ~w\r\n",
                                    " # of DC-awareness replicas    : ~w\r\n",
                                    "                     ring size : 2^~w\r\n",
                                    "             Current ring hash : ~s\r\n",
                                    "                Prev ring hash : ~s\r\n",
                                    "[Multi DC replication settings]\r\n",
                                    "         max # of joinable DCs : ~w\r\n",
                                    "            # of replicas a DC : ~w\r\n\r\n",
                                    "[Node(s) state]\r\n"]),
                      [Version,
                       SystemConf#?SYSTEM_CONF.cluster_id,
                       SystemConf#?SYSTEM_CONF.dc_id,
                       SystemConf#?SYSTEM_CONF.n,
                       SystemConf#?SYSTEM_CONF.r,
                       SystemConf#?SYSTEM_CONF.w,
                       SystemConf#?SYSTEM_CONF.d,
                       SystemConf#?SYSTEM_CONF.num_of_rack_replicas,
                       SystemConf#?SYSTEM_CONF.bit_of_ring,
                       leo_hex:integer_to_hex(RH0, 8),
                       leo_hex:integer_to_hex(RH1, 8),
                       SystemConf#?SYSTEM_CONF.max_mdc_targets,
                       SystemConf#?SYSTEM_CONF.num_of_dc_replicas
                      ]),
    system_conf_with_node_stat(FormattedSystemConf, Nodes).


%% @doc Format a system-configuration w/node-state
%%
-spec(system_conf_with_node_stat(string(), list()) ->
             string()).
system_conf_with_node_stat(FormattedSystemConf, Nodes) ->
    Col1Len = lists:foldl(fun({_,N,_,_,_,_}, Acc) ->
                                  Len = length(N),
                                  case (Len > Acc) of
                                      true  -> Len;
                                      false -> Acc
                                  end
                          end, 0, Nodes) + 5,
    CellColumns = [{"type",          6},
                   {"node",    Col1Len},
                   {"state",        12},
                   {"current ring", 14},
                   {"prev ring",    14},
                   {"updated at",   28},
                   {'$end',          0}],
    LenPerCol = lists:map(fun({_, Len}) -> Len end, CellColumns),


    Fun1 = fun(Col, Str) ->
                   case Col of
                       {'$end',_Len      } -> lists:append([Str, ?CRLF]);
                       {"type", Len      } -> lists:append([Str, lists:duplicate(Len + 1, "-"), "+"]);
                       {"node", Len      } -> lists:append([Str, lists:duplicate(Len + 2, "-"), "+"]);
                       {"updated at", Len} -> lists:append([Str, lists:duplicate(Len + 0, "-")]);
                       {_Other, Len      } -> lists:append([Str, lists:duplicate(Len + 2, "-"), "+"])
                   end
           end,
    Header1 = lists:foldl(Fun1, [], CellColumns),

    Fun2 = fun(Col, Str) ->
                   {Name, _} = Col,
                   case Col of
                       {'$end',_Len      } -> lists:append([Str, ?CRLF]);
                       {"updated at", Len} -> lists:append([Str, string:centre(Name, Len, $ )]);
                       {_Other, Len      } -> lists:append([Str, string:centre(Name, Len, $ ), ?SEPARATOR])
                   end
           end,
    Header2 = lists:foldl(Fun2, [], CellColumns),

    Fun3 = fun(N, List) ->
                   {Type, Alias, State, RingHash0, RingHash1, When} = N,
                   FormattedDate = leo_date:date_format(When),
                   Ret = lists:append([string:centre(Type,    lists:nth(1,LenPerCol)), ?SEPARATOR,
                                       string:left(Alias,     lists:nth(2,LenPerCol)), ?SEPARATOR,
                                       string:left(State,     lists:nth(3,LenPerCol)), ?SEPARATOR,
                                       string:left(RingHash0, lists:nth(4,LenPerCol)), ?SEPARATOR,
                                       string:left(RingHash1, lists:nth(5,LenPerCol)), ?SEPARATOR,
                                       FormattedDate,
                                       ?CRLF]),
                   List ++ [Ret]
           end,
    lists:foldl(Fun3, [FormattedSystemConf, Header1, Header2, Header1], Nodes) ++ ?CRLF.


%% @doc Format a cluster node state
%%
-spec(node_stat(string(), [tuple()]) ->
             string()).
node_stat(?SERVER_TYPE_GATEWAY, State) ->
    Version      = leo_misc:get_value('version',       State, []),
    Directories  = leo_misc:get_value('dirs',          State, []),
    HttpConf     = leo_misc:get_value('http_conf',     State, []),
    RingHashes   = leo_misc:get_value('ring_checksum', State, []),
    Statistics   = leo_misc:get_value('statistics',    State, []),

    MaxChunkedObjs = leo_misc:get_value('max_chunked_objs', HttpConf, 0),
    ChunkedObjLen  = leo_misc:get_value('chunked_obj_len',  HttpConf, 0),
    MaxObjLen = MaxChunkedObjs * ChunkedObjLen,

    io_lib:format(lists:append(["[config-1]\r\n",
                                "                      version : ~s\r\n",
                                "                      log dir : ~s\r\n",
                                "[config-2]\r\n",
                                "  -- http-server-related --\r\n",
                                "          using api [s3|rest] : ~w\r\n",
                                "               listening port : ~w\r\n",
                                "           listening ssl port : ~w\r\n",
                                "               # of_acceptors : ~w\r\n",
                                "  -- cache-related --\r\n",
                                "      http cache [true|false] : ~w\r\n",
                                "           # of cache_workers : ~w\r\n",
                                "                 cache expire : ~w\r\n",
                                "        cache max content len : ~w\r\n",
                                "           ram cache capacity : ~w\r\n",
                                "       disc cache capacity    : ~w\r\n",
                                "       disc cache threshold   : ~w\r\n",
                                "       disc cache data dir    : ~s\r\n",
                                "       disc cache journal dir : ~s\r\n",
                                "  -- large-object-related --\r\n",
                                "        max # of chunked objs : ~w\r\n",
                                "          chunk object length : ~w\r\n",
                                "            max object length : ~w\r\n",
                                "   reading_chunked_obj_length : ~w\r\n",
                                "    threshold of chunk length : ~w\r\n",
                                "\r\n[status-1: ring]\r\n",
                                "            ring state (cur)  : ~s\r\n",
                                "            ring state (prev) : ~s\r\n",
                                "\r\n[status-2: erlang-vm]\r\n",
                                "                   vm version : ~s\r\n",
                                "              total mem usage : ~w\r\n",
                                "             system mem usage : ~w\r\n",
                                "              procs mem usage : ~w\r\n",
                                "                ets mem usage : ~w\r\n",
                                "                        procs : ~w/~w\r\n",
                                "                  kernel_poll : ~w\r\n",
                                "             thread_pool_size : ~w\r\n\r\n"]),
                  [
                   %% config-1 [2]
                   Version,
                   leo_misc:get_value('log', Directories, []),
                   %% config-2 [17]
                   leo_misc:get_value('handler',                  HttpConf, ''),
                   leo_misc:get_value('port',                     HttpConf, 0),
                   leo_misc:get_value('ssl_port',                 HttpConf, 0),
                   leo_misc:get_value('num_of_acceptors',         HttpConf, 0),
                   leo_misc:get_value('http_cache',               HttpConf, ''),
                   leo_misc:get_value('cache_workers',            HttpConf, 0),
                   leo_misc:get_value('cache_expire',             HttpConf, 0),
                   leo_misc:get_value('cache_max_content_len',    HttpConf, 0),
                   leo_misc:get_value('cache_ram_capacity',       HttpConf, 0),
                   leo_misc:get_value('cache_disc_capacity',      HttpConf, 0),
                   leo_misc:get_value('cache_disc_threshold_len', HttpConf, 0),
                   leo_misc:get_value('cache_disc_dir_data',      HttpConf, ""),
                   leo_misc:get_value('cache_disc_dir_journal',   HttpConf, ""),

                   %% large-object
                   MaxChunkedObjs,
                   ChunkedObjLen,
                   MaxObjLen,
                   leo_misc:get_value('reading_chunked_obj_len',  HttpConf, 0),
                   leo_misc:get_value('threshold_of_chunk_len',   HttpConf, 0),
                   %% status-1 [2]
                   leo_hex:integer_to_hex(leo_misc:get_value('ring_cur',  RingHashes, 0), 8),
                   leo_hex:integer_to_hex(leo_misc:get_value('ring_prev', RingHashes, 0), 8),
                   %% status-2 [8]
                   leo_misc:get_value('vm_version',       Statistics, []),
                   leo_misc:get_value('total_mem_usage',  Statistics, 0),
                   leo_misc:get_value('system_mem_usage', Statistics, 0),
                   leo_misc:get_value('proc_mem_usage',   Statistics, 0),
                   leo_misc:get_value('ets_mem_usage',    Statistics, 0),
                   leo_misc:get_value('num_of_procs',     Statistics, 0),
                   leo_misc:get_value('process_limit',    Statistics, 0),
                   leo_misc:get_value('kernel_poll',      Statistics, false),
                   leo_misc:get_value('thread_pool_size', Statistics, 0)
                  ]);

node_stat(?SERVER_TYPE_STORAGE, State) ->
    Version      = leo_misc:get_value('version',       State, []),
    NumOfVNodes  = leo_misc:get_value('num_of_vnodes', State, []),
    GrpLevel2    = leo_misc:get_value('grp_level_2',   State, []),
    Directories  = leo_misc:get_value('dirs',          State, []),
    RingHashes   = leo_misc:get_value('ring_checksum', State, []),
    Statistics   = leo_misc:get_value('statistics',    State, []),
    ObjContainer = leo_misc:get_value('avs',           State, []),
    CustomItems  = leo_misc:get_value('storage', Statistics, []),

    io_lib:format(lists:append(["[config]\r\n",
                                "            version : ~s\r\n",
                                "        # of vnodes : ~w\r\n",
                                "      group level-1 :   \r\n",
                                "      group level-2 : ~s\r\n",
                                "      obj-container : ~p\r\n",
                                "            log dir : ~s\r\n",
                                "\r\n[status-1: ring]\r\n",
                                "  ring state (cur)  : ~s\r\n",
                                "  ring state (prev) : ~s\r\n",
                                "\r\n[status-2: erlang-vm]\r\n",
                                "         vm version : ~s\r\n",
                                "    total mem usage : ~w\r\n",
                                "   system mem usage : ~w\r\n",
                                "    procs mem usage : ~w\r\n",
                                "      ets mem usage : ~w\r\n",
                                "              procs : ~w/~w\r\n",
                                "        kernel_poll : ~w\r\n",
                                "   thread_pool_size : ~w\r\n",
                                "\r\n[status-3: # of msgs]\r\n",
                                "   replication msgs : ~w\r\n",
                                "    vnode-sync msgs : ~w\r\n",
                                "     rebalance msgs : ~w\r\n",
                                "\r\n"]),
                  [Version,
                   NumOfVNodes,
                   GrpLevel2,
                   ObjContainer,
                   leo_misc:get_value('log', Directories, []),
                   leo_hex:integer_to_hex(leo_misc:get_value('ring_cur',  RingHashes, 0), 8),
                   leo_hex:integer_to_hex(leo_misc:get_value('ring_prev', RingHashes, 0), 8),
                   leo_misc:get_value('vm_version',       Statistics, []),
                   leo_misc:get_value('total_mem_usage',  Statistics, 0),
                   leo_misc:get_value('system_mem_usage', Statistics, 0),
                   leo_misc:get_value('proc_mem_usage',   Statistics, 0),
                   leo_misc:get_value('ets_mem_usage',    Statistics, 0),
                   leo_misc:get_value('num_of_procs',     Statistics, 0),
                   leo_misc:get_value('process_limit',    Statistics, 0),
                   leo_misc:get_value('kernel_poll',      Statistics, false),
                   leo_misc:get_value('thread_pool_size', Statistics, 0),
                   leo_misc:get_value('num_of_replication_msg', CustomItems, 0),
                   leo_misc:get_value('num_of_sync_vnode_msg',  CustomItems, 0),
                   leo_misc:get_value('num_of_rebalance_msg',   CustomItems, 0)
                  ]).


%% @doc Status of compaction
%%
-spec(compact_status(#compaction_stats{}) ->
             string()).
compact_status(#compaction_stats{status = Status,
                                 total_num_of_targets    = TotalNumOfTargets,
                                 num_of_pending_targets  = Targets1,
                                 num_of_ongoing_targets  = Targets2,
                                 num_of_reserved_targets = Targets3,
                                 latest_exec_datetime    = LatestExecDate}) ->
    Date = case LatestExecDate of
               0 -> ?NULL_DATETIME;
               _ -> leo_date:date_format(LatestExecDate)
           end,

    io_lib:format(lists:append(["        current status: ~w\r\n",
                                " last compaction start: ~s\r\n",
                                "         total targets: ~w\r\n",
                                "  # of pending targets: ~w\r\n",
                                "  # of ongoing targets: ~w\r\n",
                                "  # of out of targets : ~w\r\n",
                                "\r\n"]),
                  [Status, Date, TotalNumOfTargets, Targets1, Targets2, Targets3]).


%% @doc Format storge stats-list
%%
-spec(du(summary | detail, {integer(), integer(), integer(), integer(), integer(), integer()} | list()) ->
             string()).
du(summary, {TotalNum, ActiveNum, TotalSize, ActiveSize, LastStart, LastEnd}) ->
    Fun = fun(0) -> ?NULL_DATETIME;
             (D) -> leo_date:date_format(D)
          end,
    Ratio = ?ratio_of_active_size(ActiveSize, TotalSize),

    io_lib:format(lists:append([" active number of objects: ~w\r\n",
                                "  total number of objects: ~w\r\n",
                                "   active size of objects: ~w\r\n",
                                "    total size of objects: ~w\r\n",
                                "     ratio of active size: ~w%\r\n",
                                "    last compaction start: ~s\r\n",
                                "      last compaction end: ~s\r\n\r\n"]),
                  [ActiveNum, TotalNum,
                   ActiveSize, TotalSize, Ratio,
                   Fun(LastStart), Fun(LastEnd)]);

du(detail, StatsList) when is_list(StatsList) ->
    Fun = fun({ok, #storage_stats{file_path   = FilePath,
                                  compaction_histories = Histories,
                                  total_sizes = TotalSize,
                                  active_sizes = ActiveSize,
                                  total_num  = Total,
                                  active_num = Active}}, Acc) ->
                  {LatestStart1, LatestEnd1} =
                      case length(Histories) of
                          0 -> {?NULL_DATETIME, ?NULL_DATETIME};
                          _ ->
                              {StartComp, FinishComp} = hd(Histories),
                              {leo_date:date_format(StartComp),
                               leo_date:date_format(FinishComp)}
                      end,
                  Ratio = ?ratio_of_active_size(ActiveSize, TotalSize),

                  lists:append([Acc,
                                io_lib:format(
                                  lists:append(["              file path: ~s\r\n",
                                                " active number of objects: ~w\r\n",
                                                "  total number of objects: ~w\r\n",
                                                "   active size of objects: ~w\r\n"
                                                "    total size of objects: ~w\r\n",
                                                "     ratio of active size: ~w%\r\n",
                                                "    last compaction start: ~s\r\n"
                                                "      last compaction end: ~s\r\n\r\n"]),
                                  [FilePath,
                                   Active,
                                   Total,
                                   ActiveSize,
                                   TotalSize,
                                   Ratio,
                                   LatestStart1,
                                   LatestEnd1])]);
             (_Error, Acc) ->
                  Acc
          end,
    lists:append([lists:foldl(Fun, "[du(storage stats)]\r\n", StatsList), "\r\n"]);
du(_, _) ->
    [].


%% @doc Format s3-gen-key result
%%
-spec(credential(string(), string()) ->
             string()).
credential(AccessKeyId, SecretAccessKey) ->
    io_lib:format("  access-key-id: ~s\r\n  secret-access-key: ~s\r\n\r\n",
                  [AccessKeyId, SecretAccessKey]).


%% @doc Format s3-users result
%%
-spec(users(list()) ->
             string()).
users(Owners) ->
    Col1Len = lists:foldl(fun(User, Acc) ->
                                  UserId = leo_misc:get_value(user_id, User),
                                  Len = length(UserId),
                                  case (Len > Acc) of
                                      true  -> Len;
                                      false -> Acc
                                  end
                          end, 7, Owners),
    Col2Len = 7,
    Col3Len = 22,
    Col4Len = 26,

    Header = lists:append([string:left("user_id",       Col1Len), " | ",
                           string:left("role_id",       Col2Len), " | ",
                           string:left("access_key_id", Col3Len), " | ",
                           string:left("created_at",    Col4Len), "\r\n",
                           lists:duplicate(Col1Len, "-"), "-+-",
                           lists:duplicate(Col2Len, "-"), "-+-",
                           lists:duplicate(Col3Len, "-"), "-+-",
                           lists:duplicate(Col4Len, "-"), "\r\n"]),
    Fun = fun(User, Acc) ->
                  UserId      = leo_misc:get_value(user_id,       User),
                  RoleId      = leo_misc:get_value(role_id,       User),
                  AccessKeyId = leo_misc:get_value(access_key_id, User),
                  CreatedAt   = leo_misc:get_value(created_at,    User),

                  RoleIdStr      = integer_to_list(RoleId),
                  AccessKeyIdStr = binary_to_list(AccessKeyId),

                  Acc ++ io_lib:format("~s | ~s | ~s | ~s\r\n",
                                       [string:left(UserId,         Col1Len),
                                        string:left(RoleIdStr,      Col2Len),
                                        string:left(AccessKeyIdStr, Col3Len),
                                        leo_date:date_format(CreatedAt)])
          end,
    lists:append([lists:foldl(Fun, Header, Owners), "\r\n"]).


%% @doc Format a endpoint list
%%
-spec(endpoints(list(tuple())) ->
             string()).
endpoints(EndPoints) ->
    Col1Len = lists:foldl(fun({_, EP, _}, Acc) ->
                                  Len = byte_size(EP),
                                  case (Len > Acc) of
                                      true  -> Len;
                                      false -> Acc
                                  end
                          end, 8, EndPoints),
    Col2Len = 26,

    Header = lists:append([string:left("endpoint", Col1Len),
                           " | ",
                           string:left("created at", Col2Len),
                           "\r\n",
                           lists:duplicate(Col1Len, "-"),
                           "-+-", lists:duplicate(Col2Len, "-"),
                           "\r\n"]),
    Fun = fun({endpoint, EP, Created}, Acc) ->
                  EndpointStr = binary_to_list(EP),
                  Acc ++ io_lib:format("~s | ~s\r\n",
                                       [string:left(EndpointStr,Col1Len),
                                        leo_date:date_format(Created)])
          end,
    lists:append([lists:foldl(Fun, Header, EndPoints), "\r\n"]).


%% @doc Format a bucket list
%%
-spec(buckets(list(tuple())) ->
             string()).
buckets(Buckets) ->
    Col1MinLen = 12,  %% cluster-id
    Col2MinLen = 8,  %% bucket-name
    Col3MinLen = 6,  %% owner
    Col4MinLen = 12, %% permissions

    {Col1Len, Col2Len,
     Col3Len, Col4Len} =
        lists:foldl(fun(#bucket_dto{name  = Bucket,
                                    owner = #user_credential{user_id= Owner},
                                    acls  = Permissions,
                                    cluster_id = ClusterId,
                                    created_at = _CreatedAt},
                        {C1, C2, C3, C4}) ->
                            ClusterIdStr = atom_to_list(ClusterId),
                            BucketStr = binary_to_list(Bucket),
                            OwnerStr  = binary_to_list(Owner),
                            PermissionsStr = leo_s3_bucket:aclinfo2str(Permissions),
                            Len1 = length(ClusterIdStr),
                            Len2 = length(BucketStr),
                            Len3 = length(OwnerStr),
                            Len4 = length(PermissionsStr),


                            {case (Len1 > C1) of
                                 true  -> Len1;
                                 false -> C1
                             end,
                             case (Len2 > C2) of
                                 true  -> Len2;
                                 false -> C2
                             end,
                             case (Len3 > C3) of
                                 true  -> Len3;
                                 false -> C3
                             end,
                             case (Len4 > C4) of
                                 true  -> Len4;
                                 false -> C4
                             end
                            }
                    end, {Col1MinLen, Col2MinLen,
                          Col3MinLen, Col4MinLen}, Buckets),
    Col5Len = 26, %% created at

    Header = lists:append(
               [
                string:left("cluster id",  Col1Len), " | ",
                string:left("bucket",      Col2Len), " | ",
                string:left("owner",       Col3Len), " | ",
                string:left("permissions", Col4Len), " | ",
                string:left("created at",  Col5Len), "\r\n",

                lists:duplicate(Col1Len, "-"), "-+-",
                lists:duplicate(Col2Len, "-"), "-+-",
                lists:duplicate(Col3Len, "-"), "-+-",
                lists:duplicate(Col4Len, "-"), "-+-",
                lists:duplicate(Col5Len, "-"), "\r\n"
               ]),

    Fun = fun(#bucket_dto{name = Bucket,
                          owner = #user_credential{user_id= Owner},
                          acls  = Permissions1,
                          cluster_id = ClusterId,
                          created_at = Created1}, Acc) ->
                  ClusterIdStr = atom_to_list(ClusterId),
                  BucketStr = binary_to_list(Bucket),
                  OwnerStr  = binary_to_list(Owner),
                  PermissionsStr = leo_s3_bucket:aclinfo2str(Permissions1),
                  Created2  = case (Created1 > 0) of
                                  true  -> leo_date:date_format(Created1);
                                  false -> []
                              end,

                  Acc ++ io_lib:format("~s | ~s | ~s | ~s | ~s\r\n",
                                       [
                                        string:left(ClusterIdStr,   Col1Len),
                                        string:left(BucketStr,      Col2Len),
                                        string:left(OwnerStr,       Col3Len),
                                        string:left(PermissionsStr, Col4Len),
                                        Created2])
          end,
    lists:append([lists:foldl(Fun, Header, Buckets), "\r\n"]).


%% @doc Format a bucket list
%%
-spec(bucket_by_access_key(list(#?BUCKET{})) ->
             string()).
bucket_by_access_key(Buckets) ->
    Col1MinLen = 8,
    Col2MinLen = 12,
    {Col1Len, Col2Len} =
        lists:foldl(fun(#?BUCKET{name = Bucket,
                                 acls = Permissions}, {C1, C2}) ->
                            BucketStr = binary_to_list(Bucket),
                            PermissionsStr = leo_s3_bucket:aclinfo2str(Permissions),
                            Len1 = length(BucketStr),
                            Len2 = length(PermissionsStr),
                            {case (Len1 > C1) of
                                 true  -> Len1;
                                 false -> C1
                             end,
                             case (Len2 > C2) of
                                 true  -> Len2;
                                 false -> C2
                             end
                            }
                    end, {Col1MinLen, Col2MinLen}, Buckets),
    Col3Len = 26,
    Header = lists:append(
               [string:left("bucket",      Col1Len), " | ",
                string:left("permissions", Col2Len), " | ",
                string:left("created at",  Col3Len), "\r\n",

                lists:duplicate(Col1Len, "-"), "-+-",
                lists:duplicate(Col2Len, "-"), "-+-",
                lists:duplicate(Col3Len, "-"), "\r\n"]),

    Fun = fun(#?BUCKET{name = Bucket1,
                       acls = Permissions1,
                       created_at = Created1}, Acc) ->
                  BucketStr = binary_to_list(Bucket1),
                  PermissionsStr = leo_s3_bucket:aclinfo2str(Permissions1),
                  Created2  = case (Created1 > 0) of
                                  true  -> leo_date:date_format(Created1);
                                  false -> []
                              end,

                  Acc ++ io_lib:format("~s | ~s | ~s\r\n",
                                       [string:left(BucketStr,      Col1Len),
                                        string:left(PermissionsStr, Col2Len),
                                        Created2])
          end,
    lists:append([lists:foldl(Fun, Header, Buckets), "\r\n"]).


%% @doc Format a acl list
%%
-spec(acls(acls()) ->
             string()).
acls(ACLs) ->
    Col1Len = lists:foldl(fun(#bucket_acl_info{user_id = User} = _ACL, C1) ->
                                  UserStr = binary_to_list(User),
                                  Len = length(UserStr),
                                  case (Len > C1) of
                                      true  -> Len;
                                      false -> C1
                                  end
                          end, 14, ACLs),
    Col2Len = 24, % @todo to be calcurated
    Header = lists:append(
               [string:left("access_key_id", Col1Len), " | ",
                string:left("permissions",   Col2Len), "\r\n",
                lists:duplicate(Col1Len, "-"), "-+-",
                lists:duplicate(Col2Len, "-"), "\r\n"]),

    Fun = fun(#bucket_acl_info{user_id = User, permissions = Permissions} = _ACL, Acc) ->
                  UserStr = binary_to_list(User),
                  FormatStr = case length(Permissions) of
                                  0 ->
                                      "~s | private\r\n";
                                  1 ->
                                      "~s | ~s\r\n";
                                  N ->
                                      lists:flatten(lists:append(["~s | ~s",
                                                                  lists:duplicate(N-1, ", ~s"),
                                                                  "\r\n"]))
                              end,
                  Acc ++ io_lib:format(FormatStr,
                                       [string:left(UserStr, Col1Len)] ++ Permissions)
          end,
    lists:append([lists:foldl(Fun, Header, ACLs), "\r\n"]).


%% @doc Cluster statuses
-spec(cluster_status(list()) ->
             string()).
cluster_status(Stats) ->
    Col1Min = 10, %% cluster-id
    Col1Len = lists:foldl(fun(N, Acc) ->

                                  Len = length(atom_to_list(leo_misc:get_value('cluster_id', N))),
                                  case (Len > Acc) of
                                      true  -> Len;
                                      false -> Acc
                                  end
                          end, Col1Min, Stats),
    Col2Min = 10, %% dc-id
    Col2Len = lists:foldl(fun(N, Acc) ->
                                  Len = length(atom_to_list(leo_misc:get_value('dc_id', N))),
                                  case (Len > Acc) of
                                      true  -> Len;
                                      false -> Acc
                                  end
                          end, Col2Min, Stats),
    Col3Len = 12, %% status
    Col4Len = 14, %% # of storages
    Col5Len = 28, %% updated-at

    Header = lists:append(
               [string:centre("cluster id",    Col1Len), " | ",
                string:centre("dc id",         Col2Len), " | ",
                string:centre("status",        Col3Len), " | ",
                string:centre("# of storages", Col4Len), " | ",
                string:centre("updated at",    Col5Len), "\r\n",

                lists:duplicate(Col1Len, "-"), "-+-",
                lists:duplicate(Col2Len, "-"), "-+-",
                lists:duplicate(Col3Len, "-"), "-+-",
                lists:duplicate(Col4Len, "-"), "-+-",
                lists:duplicate(Col5Len, "-"), "\r\n"]),

    Fun = fun(Items, Acc) ->
                  ClusterId_1 = leo_misc:get_value('cluster_id', Items),
                  ClusterId_2 = case is_atom(ClusterId_1) of
                                    true  -> atom_to_list(ClusterId_1);
                                    false -> ClusterId_1
                                end,
                  DCId_1 = leo_misc:get_value('dc_id', Items),
                  DCId_2 = case is_atom(DCId_1) of
                               true  -> atom_to_list(DCId_1);
                               false -> DCId_1
                           end,
                  Status    = atom_to_list(leo_misc:get_value('status', Items)),
                  Storages  = integer_to_list(leo_misc:get_value('members', Items)),
                  UpdatedAt = leo_misc:get_value('updated_at', Items),
                  UpdatedAt_1 = case (UpdatedAt > 0) of
                                    true  -> leo_date:date_format(UpdatedAt);
                                    false -> []
                                end,
                  Acc ++ io_lib:format("~s | ~s | ~s | ~s | ~s\r\n",
                                       [string:left(ClusterId_2, Col1Len),
                                        string:left(DCId_2, Col2Len),
                                        string:left(Status, Col3Len),
                                        string:right(Storages, Col4Len),
                                        UpdatedAt_1])
          end,
    lists:append([lists:foldl(Fun, Header, Stats), "\r\n"]).


%% @doc Format an assigned file
%%
-spec(whereis(list()) ->
             string()).
whereis(AssignedInfo) ->
    Col2Len = lists:foldl(fun(N, Acc) ->
                                  Len = length(element(1,N)),
                                  case (Len > Acc) of
                                      true  -> Len;
                                      false -> Acc
                                  end
                          end, 0, AssignedInfo) + 5,
    CellColumns = [{"del?",          6},
                   {"node",    Col2Len},
                   {"ring address", 36},
                   {"size",         10},
                   {"checksum",     12},
                   {"# of chunks",  14},
                   {"clock",        14},
                   {"when",         28},
                   {'$end',          0}],

    LenPerCol = lists:map(fun({_, Len})-> Len end, CellColumns),

    Fun1 = fun(Col, Str) ->
                   case Col of
                       {'$end',_Len} -> lists:append([Str, ?CRLF]);
                       {"del?", Len} -> lists:append([Str, lists:duplicate(Len + 1, "-"), "+"]);
                       {"node", Len} -> lists:append([Str, lists:duplicate(Len + 2, "-"), "+"]);
                       {"when", Len} -> lists:append([Str, lists:duplicate(Len + 0, "-")]);
                       {_Other, Len} -> lists:append([Str, lists:duplicate(Len + 2, "-"), "+"])
                   end
           end,
    Header1 = lists:foldl(Fun1, [], CellColumns),

    Fun2 = fun(Col, Str) ->
                   {Name, _} = Col,
                   case Col of
                       {'$end', _  } -> lists:append([Str, ?CRLF]);
                       {"when", Len} -> lists:append([Str, string:centre(Name, Len, $ )]);
                       {_Other, Len} -> lists:append([Str, string:centre(Name, Len, $ ), ?SEPARATOR])
                   end
           end,
    Header2 = lists:foldl(Fun2, [], CellColumns),


    Fun3 = fun(N, List) ->
                   Ret = case N of
                             {Node, not_found} ->
                                 %% lists:append([string:left([], lists:nth(1,LenPerCol)), ?SEPARATOR,
                                 %%               Node, ?CRLF]);
                                 lists:append([string:left([],   lists:nth(1,LenPerCol)), ?SEPARATOR,
                                               string:left(Node, lists:nth(2,LenPerCol)), ?SEPARATOR,
                                               string:left([],   lists:nth(3,LenPerCol)), ?SEPARATOR,
                                               string:left([],   lists:nth(4,LenPerCol)), ?SEPARATOR,
                                               string:left([],   lists:nth(5,LenPerCol)), ?SEPARATOR,
                                               string:left([],   lists:nth(6,LenPerCol)), ?SEPARATOR,
                                               string:left([],   lists:nth(7,LenPerCol)), ?SEPARATOR,
                                               ?CRLF]);
                             {Node, VNodeId, DSize, ChunkedObjs, Clock, Timestamp, Checksum, DelFlag} ->
                                 FormattedDate = leo_date:date_format(Timestamp),
                                 DelStr = case DelFlag of
                                              0 -> ?SPACE;
                                              _ -> "*"
                                          end,
                                 lists:append([string:centre(DelStr,                           lists:nth(1,LenPerCol)), ?SEPARATOR,
                                               string:left(Node,                               lists:nth(2,LenPerCol)), ?SEPARATOR,
                                               string:left(leo_hex:integer_to_hex(VNodeId, 8), lists:nth(3,LenPerCol)), ?SEPARATOR,
                                               string:right(leo_file:dsize(DSize),             lists:nth(4,LenPerCol)), ?SEPARATOR,
                                               string:right(string:sub_string(leo_hex:integer_to_hex(Checksum, 8), 1, 10),
                                                            lists:nth(5,LenPerCol)), ?SEPARATOR,
                                               string:right(integer_to_list(ChunkedObjs),      lists:nth(6,LenPerCol)), ?SEPARATOR,
                                               string:left(leo_hex:integer_to_hex(Clock, 8),   lists:nth(7,LenPerCol)), ?SEPARATOR,
                                               FormattedDate,
                                               ?CRLF])
                         end,
                   List ++ [Ret]
           end,
    lists:foldl(Fun3, [Header1, Header2, Header1], AssignedInfo) ++ ?CRLF.


%% @doc Format a history list
%%
-spec(histories(list(#history{})) ->
             string()).
histories(Histories) ->
    Fun = fun(#history{id      = Id,
                       command = Command,
                       created = Created}, Acc) ->
                  Acc ++ io_lib:format("~s | ~s | ~s\r\n",
                                       [string:left(integer_to_list(Id), 4),
                                        leo_date:date_format(Created),
                                        Command])
          end,
    lists:foldl(Fun, "[Histories]\r\n", Histories).


authorized() ->
    io_lib:format("OK\r\n\r\n", []).

user_id() ->
    io_lib:format("~s\r\n",["user-id:"]).

password() ->
    io_lib:format("~s\r\n",["password:"]).


%%----------------------------------------------------------------------
%% Inner function(s)
%%----------------------------------------------------------------------
