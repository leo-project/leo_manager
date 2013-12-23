%%======================================================================
%%
%% Leo Manager
%%
%% Copyright (c) 2012-2013 Rakuten, Inc.
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
-include_lib("leo_s3_libs/include/leo_s3_user.hrl").
-include_lib("leo_s3_libs/include/leo_s3_bucket.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([ok/0, error/1, error/2, help/0, version/1,
         bad_nodes/1, system_info_and_nodes_stat/1, node_stat/2,
         compact_status/1, du/2, credential/2, users/1, endpoints/1, buckets/1,
         acls/1, whereis/1, histories/1,
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
-spec(help() ->
             string()).
help() ->
    lists:append([help("[Cluster Operation]\r\n",
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
                        ?CMD_CHANGE_BUCKET_OWNER,
                        ?CMD_UPDATE_ACL,
                        ?CMD_GET_ACL], []),
                  help("[Misc]\r\n",
                       [?CMD_VERSION,
                        ?CMD_STATUS,
                        ?CMD_HISTORY,
                        ?CMD_QUIT], [])]).

%% @private
help(Command) ->
    case leo_manager_mnesia:get_available_command_by_name(Command) of
        {ok, [#cmd_state{help = Help}|_]} ->
            io_lib:format("~s\r\n",[Help]);
        _ ->
            []
    end.
help(_, [], []) ->
    [];
help(Header, [], Acc) ->
    lists:append([[Header], Acc, [?CRLF]]);
help(Header, [Command|Rest], Acc) ->
    case help(Command) of
        [] ->
            help(Header, Rest, Acc);
        Res ->
            Acc1 = lists:append([Acc, [Res]]),
            help(Header, Rest, Acc1)
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

    FormattedSystemConf =
        io_lib:format(lists:append(["[System config]\r\n",
                                    "                system version : ~s\r\n",
                                    "                total replicas : ~w\r\n",
                                    "           # of successes of R : ~w\r\n",
                                    "           # of successes of W : ~w\r\n",
                                    "           # of successes of D : ~w\r\n",
                                    " # of DC-awareness replicas    : ~w\r\n",
                                    " # of Rack-awareness replicas  : ~w\r\n",
                                    "                     ring size : 2^~w\r\n",
                                    "              ring hash (cur)  : ~s\r\n",
                                    "              ring hash (prev) : ~s\r\n\r\n",
                                    "[Node(s) state]\r\n"]),
                      [Version,
                       SystemConf#system_conf.n,
                       SystemConf#system_conf.r,
                       SystemConf#system_conf.w,
                       SystemConf#system_conf.d,
                       SystemConf#system_conf.level_1,
                       SystemConf#system_conf.level_2,
                       SystemConf#system_conf.bit_of_ring,
                       leo_hex:integer_to_hex(RH0, 8),
                       leo_hex:integer_to_hex(RH1, 8)
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
    CellColumns = [{"type",        5},
                   {"node",  Col1Len},
                   {"state",      12},
                   {"ring (cur)", 14},
                   {"ring (prev)",14},
                   {"when",       28},
                   {"END",         0}],
    LenPerCol = lists:map(fun({_, Len}) -> Len end, CellColumns),

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
                   {Type, Alias, State, RingHash0, RingHash1, When} = N,
                   FormattedDate = leo_date:date_format(When),
                   Ret = lists:append([" ",
                                       string:left(Type,          lists:nth(1,LenPerCol)),
                                       string:left(Alias,         lists:nth(2,LenPerCol)),
                                       string:left(State,         lists:nth(3,LenPerCol)),
                                       string:left(RingHash0,     lists:nth(4,LenPerCol)),
                                       string:left(RingHash1,     lists:nth(5,LenPerCol)),
                                       FormattedDate,
                                       ?CRLF]),
                   List ++ [Ret]
           end,
    _FormattedList =
        lists:foldl(Fun2, [FormattedSystemConf, Sepalator, Header2, Sepalator], Nodes) ++ ?CRLF.


%% @doc Format a cluster node state
%%
-spec(node_stat(string(), #cluster_node_status{}) ->
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
    Fun = fun(Stats, Acc) ->
                  case Stats of
                      {ok, #storage_stats{file_path   = FilePath,
                                          compaction_histories = Histories,
                                          total_sizes = TotalSize,
                                          active_sizes = ActiveSize,
                                          total_num  = Total,
                                          active_num = Active}} ->
                          {LatestStart1, LatestEnd1} = case length(Histories) of
                                                           0 -> {?NULL_DATETIME, ?NULL_DATETIME};
                                                           _ ->
                                                               {StartComp, FinishComp} = hd(Histories),
                                                               {leo_date:date_format(StartComp), leo_date:date_format(FinishComp)}
                                                       end,
                          Ratio = ?ratio_of_active_size(ActiveSize, TotalSize),

                          lists:append([Acc, io_lib:format(
                                               lists:append(["              file path: ~s\r\n",
                                                             " active number of objects: ~w\r\n",
                                                             "  total number of objects: ~w\r\n",
                                                             "   active size of objects: ~w\r\n"
                                                             "    total size of objects: ~w\r\n",
                                                             "     ratio of active size: ~w%\r\n",
                                                             "    last compaction start: ~s\r\n"
                                                             "      last compaction end: ~s\r\n\r\n"]),
                                               [FilePath, Active, Total,
                                                ActiveSize, TotalSize, Ratio,
                                                LatestStart1, LatestEnd1])]);
                      _Error ->
                          Acc
                  end
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

    Header = lists:append([string:left("endpoint", Col1Len), " | ", string:left("created at", Col2Len), "\r\n",
                           lists:duplicate(Col1Len, "-"),    "-+-", lists:duplicate(Col2Len, "-"),      "\r\n"]),
    Fun = fun({endpoint, EP, Created}, Acc) ->
                  EndpointStr = binary_to_list(EP),
                  Acc ++ io_lib:format("~s | ~s\r\n",
                                       [string:left(EndpointStr,Col1Len), leo_date:date_format(Created)])
          end,
    lists:append([lists:foldl(Fun, Header, EndPoints), "\r\n"]).


%% @doc Format a bucket list
%%
-spec(buckets(list(tuple())) ->
             string()).
buckets(Buckets) ->
    {Col1Len, Col2Len} = lists:foldl(fun({Bucket, #user_credential{user_id= Owner}, _}, {C1, C2}) ->
                                             BucketStr = binary_to_list(Bucket),
                                             Len1 = length(BucketStr),
                                             Len2 = length(Owner),

                                             {case (Len1 > C1) of
                                                  true  -> Len1;
                                                  false -> C1
                                              end,
                                              case (Len2 > C2) of
                                                  true  -> Len2;
                                                  false -> C2
                                              end}
                                     end, {8, 6}, Buckets),
    Col3Len = 26,
    Header = lists:append(
               [string:left("bucket",     Col1Len), " | ",
                string:left("owner",      Col2Len), " | ",
                string:left("created at", Col3Len), "\r\n",

                lists:duplicate(Col1Len, "-"), "-+-",
                lists:duplicate(Col2Len, "-"), "-+-",
                lists:duplicate(Col3Len, "-"), "\r\n"]),

    Fun = fun({Bucket, #user_credential{user_id= Owner}, Created1}, Acc) ->
                  BucketStr = binary_to_list(Bucket),
                  Created2  = case (Created1 > 0) of
                                  true  -> leo_date:date_format(Created1);
                                  false -> []
                              end,

                  Acc ++ io_lib:format("~s | ~s | ~s\r\n",
                                       [string:left(BucketStr, Col1Len),
                                        string:left(Owner,     Col2Len),
                                        Created2])
          end,
    lists:append([lists:foldl(Fun, Header, Buckets), "\r\n"]).


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
    CellColumns = [{"del?",          5},
                   {"node",    Col2Len},
                   {"ring address", 36},
                   {"size",         10},
                   {"checksum",     12},
                   {"# of chunks",  14},
                   {"clock",        14},
                   {"when",         28},
                   {"END",           0}],

    LenPerCol = lists:map(fun({_, Len})-> Len end, CellColumns),
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
                                 lists:append([" ",
                                               string:left("", lists:nth(1,LenPerCol)),
                                               Node,
                                               ?CRLF]);
                             {Node, VNodeId, DSize, ChunkedObjs, Clock, Timestamp, Checksum, DelFlag} ->
                                 FormattedDate = leo_date:date_format(Timestamp),
                                 DelStr = case DelFlag of
                                              0 -> " ";
                                              _ -> "*"
                                          end,
                                 lists:append([" ",
                                               string:left(DelStr,                              lists:nth(1,LenPerCol)),
                                               string:left(Node,                                lists:nth(2,LenPerCol)),
                                               string:left(leo_hex:integer_to_hex(VNodeId, 8),  lists:nth(3,LenPerCol)),
                                               string:left(leo_file:dsize(DSize),               lists:nth(4,LenPerCol)),
                                               string:left(string:sub_string(leo_hex:integer_to_hex(Checksum, 8), 1, 10),
                                                           lists:nth(5,LenPerCol)),
                                               string:left(integer_to_list(ChunkedObjs),        lists:nth(6,LenPerCol)),
                                               string:left(leo_hex:integer_to_hex(Clock, 8),    lists:nth(7,LenPerCol)),
                                               FormattedDate,
                                               ?CRLF])
                         end,
                   List ++ [Ret]
           end,
    _FormattedList =
        lists:foldl(Fun2, [Sepalator, Header2, Sepalator], AssignedInfo) ++ ?CRLF.


%% @doc Format a history list
%%
-spec(histories(list(#history{})) ->
             string()).
histories(Histories) ->
    Fun = fun(#history{id      = Id,
                       command = Command,
                       created = Created}, Acc) ->
                  Acc ++ io_lib:format("~s | ~s | ~s\r\n",
                                       [string:left(integer_to_list(Id), 4), leo_date:date_format(Created), Command])
          end,
    lists:foldl(Fun, "[Histories]\r\n", Histories).


authorized() ->
    io_lib:format("OK\r\n\r\n", []).

user_id() ->
    io_lib:format("~s\r\n",["user-id:"]).

password() ->
    io_lib:format("~s\r\n",["password:"]).

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
                                      lists:flatten(lists:append(["~s | ~s", lists:duplicate(N-1, ", ~s"), "\r\n"]))
                              end,
                  Acc ++ io_lib:format(FormatStr,
                                       [string:left(UserStr, Col1Len)] ++ Permissions)
          end,
    lists:append([lists:foldl(Fun, Header, ACLs), "\r\n"]).


%%----------------------------------------------------------------------
%% Inner function(s)
%%----------------------------------------------------------------------

