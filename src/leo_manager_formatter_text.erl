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
-include_lib("eunit/include/eunit.hrl").

-export([ok/0, error/1, error/2, help/0, version/1,
         bad_nodes/1, system_info_and_nodes_stat/1, node_stat/2,
         compact_status/1, du/2, s3_credential/2, s3_users/1, endpoints/1, buckets/1,
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
    lists:append([help("[Cluster]\r\n",
                       [?CMD_DETACH,
                        ?CMD_SUSPEND,
                        ?CMD_RESUME,
                        ?CMD_START,
                        ?CMD_REBALANCE,
                        ?CMD_WHEREIS], []),
                  help("[Storage]\r\n",
                       [?CMD_DU,
                        ?CMD_COMPACT], []),
                  help("[Gateway]\r\n",
                       [?CMD_PURGE], []),
                  help("[S3-API related]\r\n",
                       [?CMD_CREATE_USER,
                        ?CMD_DELETE_USER,
                        ?CMD_GET_USERS,
                        ?CMD_SET_ENDPOINT,
                        ?CMD_GET_ENDPOINTS,
                        ?CMD_DEL_ENDPOINT,
                        ?CMD_ADD_BUCKET,
                        ?CMD_GET_BUCKETS], []),
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
        io_lib:format(lists:append(["[system config]\r\n",
                                    "             version : ~s\r\n",
                                    " # of replicas       : ~w\r\n",
                                    " # of successes of R : ~w\r\n",
                                    " # of successes of W : ~w\r\n",
                                    " # of successes of D : ~w\r\n",
                                    "           ring size : 2^~w\r\n",
                                    "    ring hash (cur)  : ~s\r\n",
                                    "    ring hash (prev) : ~s\r\n\r\n",
                                    "[node(s) state]\r\n"]),
                      [Version,
                       SystemConf#system_conf.n,
                       SystemConf#system_conf.r,
                       SystemConf#system_conf.w,
                       SystemConf#system_conf.d,
                       SystemConf#system_conf.bit_of_ring,
                       leo_hex:integer_to_hex(RH0),
                       leo_hex:integer_to_hex(RH1)
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
    ObjContainer = State#cluster_node_status.avs,
    Directories  = State#cluster_node_status.dirs,
    RingHashes   = State#cluster_node_status.ring_checksum,
    Statistics   = State#cluster_node_status.statistics,

    io_lib:format(lists:append(["[config]\r\n",
                                "            version : ~s\r\n",
                                "      obj-container : ~p\r\n",
                                "            log-dir : ~s\r\n",
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
                                "   thread_pool_size : ~w\r\n\r\n"]),
                  [State#cluster_node_status.version,
                   ObjContainer,
                   leo_misc:get_value('log', Directories, []),
                   leo_hex:integer_to_hex(leo_misc:get_value('ring_cur',  RingHashes, 0)),
                   leo_hex:integer_to_hex(leo_misc:get_value('ring_prev', RingHashes, 0)),
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
    ObjContainer = State#cluster_node_status.avs,
    Directories  = State#cluster_node_status.dirs,
    RingHashes   = State#cluster_node_status.ring_checksum,
    Statistics   = State#cluster_node_status.statistics,
    CustomItems  = leo_misc:get_value('storage', Statistics, []),

    io_lib:format(lists:append(["[config]\r\n",
                                "            version : ~s\r\n",
                                "      obj-container : ~p\r\n",
                                "            log-dir : ~s\r\n",
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
                  [State#cluster_node_status.version,
                   ObjContainer,
                   leo_misc:get_value('log', Directories, []),
                   leo_hex:integer_to_hex(leo_misc:get_value('ring_cur',  RingHashes, 0)),
                   leo_hex:integer_to_hex(leo_misc:get_value('ring_prev', RingHashes, 0)),
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


%% @doc Format storge stats-list
%%
-spec(du(summary | detail, {integer(), integer(), integer(), integer(), integer(), integer()} | list()) ->
             string()).
du(summary, {TotalNum, ActiveNum, TotalSize, ActiveSize, LastStart, LastEnd}) ->
    StartStr = case LastStart of
                   0 -> ?NULL_DATETIME;
                   _ -> leo_date:date_format(LastStart)
               end,
    EndStr = case LastEnd of
                 0 -> ?NULL_DATETIME;
                 _ -> leo_date:date_format(LastEnd)
             end,
    io_lib:format(lists:append([" active number of objects: ~w\r\n",
                                "  total number of objects: ~w\r\n",
                                "   active size of objects: ~w\r\n",
                                "    total size of objects: ~w\r\n",
                                "    last compaction start: ~s\r\n",
                                "      last compaction end: ~s\r\n\r\n"]),
                  [ActiveNum,
                   TotalNum,
                   ActiveSize,
                   TotalSize,
                   StartStr,
                   EndStr]);

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
                          Acc ++ io_lib:format(lists:append(["              file path: ~s\r\n",
                                                             " active number of objects: ~w\r\n",
                                                             "  total number of objects: ~w\r\n",
                                                             "   active size of objects: ~w\r\n"
                                                             "    total size of objects: ~w\r\n",
                                                             "    last compaction start: ~s\r\n"
                                                             "      last compaction end: ~s\r\n\r\n"]),
                                               [FilePath,
                                                Active,
                                                Total,
                                                ActiveSize,
                                                TotalSize,
                                                LatestStart1,
                                                LatestEnd1]);
                      _Error ->
                          Acc
                  end
          end,
    lists:append([lists:foldl(Fun, "[du(storage stats)]\r\n", StatsList), "\r\n"]);

du(_, _) ->
    [].

atomlist_to_string(AtomList, Sep) ->
    lists:foldl(fun(Atom, Acc) ->
                        List = atom_to_list(Atom),
                        case Acc of
                            [] ->
                                List;
                            _ ->
                                Acc ++ Sep ++ List
                        end
                end, [], AtomList).

compact_status({RestPids, InProgPids, LastStart}) ->
    StrRest = atomlist_to_string(RestPids, ", "),
    StrProg = atomlist_to_string(InProgPids, ", "),
    StrLast = case LastStart of
                  0 -> ?NULL_DATETIME;
                  _ -> leo_date:date_format(LastStart)
              end,
    io_lib:format(lists:append([" last compaction start: ~s\r\n",
                                "     rest of jobs(pid): ~s\r\n",
                                "  ongoing of jobs(pid): ~s\r\n\r\n"]),
                  [StrLast, StrRest, StrProg]).

%% @doc Format s3-gen-key result
%%
-spec(s3_credential(string(), string()) ->
             string()).
s3_credential(AccessKeyId, SecretAccessKey) ->
    io_lib:format("  access-key-id: ~s\r\n  secret-access-key: ~s\r\n\r\n",
                  [AccessKeyId, SecretAccessKey]).


%% @doc Format s3-users result
%%
-spec(s3_users(list()) ->
             string()).
s3_users(Owners) ->
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
                                               string:left(DelStr,                            lists:nth(1,LenPerCol)),
                                               string:left(Node,                              lists:nth(2,LenPerCol)),
                                               string:left(leo_hex:integer_to_hex(VNodeId),   lists:nth(3,LenPerCol)),
                                               string:left(leo_file:dsize(DSize),             lists:nth(4,LenPerCol)),
                                               string:left(string:sub_string(leo_hex:integer_to_hex(Checksum), 1, 10),
                                                           lists:nth(5,LenPerCol)),
                                               string:left(integer_to_list(ChunkedObjs),      lists:nth(6,LenPerCol)),
                                               string:left(leo_hex:integer_to_hex(Clock),     lists:nth(7,LenPerCol)),
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


%%----------------------------------------------------------------------
%% Inner function(s)
%%----------------------------------------------------------------------

