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
%% LeoFS Manager - JSON Formatter
%% @doc
%% @end
%%======================================================================
-module(leo_manager_formatter_json).

-author('Yosuke Hara').

-include("leo_manager.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("leo_s3_libs/include/leo_s3_user.hrl").
-include_lib("leo_s3_libs/include/leo_s3_bucket.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([ok/0, error/1, error/2, help/0, version/1, login/2,
         bad_nodes/1, system_info_and_nodes_stat/1, node_stat/2,
         compact_status/1, du/2, credential/2, users/1, endpoints/1,
         buckets/1, bucket_by_access_key/1,
         acls/1, cluster_status/1,
         whereis/1, histories/1,
         authorized/0, user_id/0, password/0
        ]).

-define(output_ok(),           gen_json({[{result, <<"OK">>}]})).
-define(output_error_1(Cause), gen_json({[{error, list_to_binary(Cause)}]})).


%% @doc Format 'ok'
%%
-spec(ok() ->
             string()).
ok() ->
    gen_json({[{result, <<"OK">>}]}).


%% @doc Format 'error'
%%
-spec(error(string()) ->
             string()).
error(not_found)  ->
    gen_json({[{error,<<"not found">>}]});
error(nodedown)  ->
    gen_json({[{error,<<"node down">>}]});
error(Cause) when is_list(Cause) ->
    gen_json({[{error, list_to_binary(Cause)}]});
error(Cause) when is_atom(Cause) ->
    gen_json({[{error, list_to_binary(atom_to_list(Cause))}]}).


%% @doc Format 'error'
%%
-spec(error(atom() | string(), string()) ->
             string()).
error(Node, Cause) when is_atom(Node) ->
    gen_json({[{error,
                {[{<<"node">>,  list_to_binary(atom_to_list(Node))},
                  {<<"cause">>, list_to_binary(Cause)}]}
               }]});
error(Node, Cause) ->
    gen_json({[{error,
                {[{<<"node">>,  list_to_binary(Node)},
                  {<<"cause">>, list_to_binary(Cause)}]}
               }]}).


%% @doc Format 'help'
%%
-spec(help() ->
             string()).
help() ->
    [].


%% Format 'version'
%%
-spec(version(string()) ->
             string()).
version(Version) ->
    gen_json({[{result, list_to_binary(Version)}]}).


%% Format 'version'
%%
-spec(login(#?S3_USER{}, list(tuple())) ->
             string()).
login(User, Credential) ->
    gen_json({[{<<"user">>,
                {[{<<"id">>,            list_to_binary(User#?S3_USER.id)},
                  {<<"role_id">>,       User#?S3_USER.role_id},
                  {<<"access_key_id">>, leo_misc:get_value('access_key_id',     Credential)},
                  {<<"secret_key">>,    leo_misc:get_value('secret_access_key', Credential)},
                  {<<"created_at">>,    list_to_binary(leo_date:date_format(User#?S3_USER.created_at))}
                 ]}}
              ]}).


%% @doc Format 'bad nodes'
%%
-spec(bad_nodes(list()) ->
             string()).
bad_nodes(BadNodes) ->
    Cause = lists:foldl(fun(Node, [] ) ->        io_lib:format("~w",  [Node]);
                           (Node, Acc) -> Acc ++ io_lib:format(",~w", [Node])
                        end, [], BadNodes),
    ?MODULE:error(Cause).


%% @doc Format a cluster-node list
%%
-spec(system_info_and_nodes_stat(list()) ->
             string()).
system_info_and_nodes_stat(Props) ->
    SystemConf = leo_misc:get_value('system_config', Props),
    Version    = leo_misc:get_value('version',       Props),
    [RH0, RH1] = leo_misc:get_value('ring_hash',     Props),
    Nodes      = leo_misc:get_value('nodes',         Props),

    NodeInfo = case Nodes of
                   [] -> [];
                   _  ->
                       lists:map(
                         fun({Type, NodeName, NodeState, RingHash0, RingHash1, When}) ->
                                 NewRingHash0 = case is_integer(RingHash0) of
                                                    true  -> integer_to_list(RingHash0);
                                                    false -> RingHash0
                                                end,
                                 NewRingHash1 = case is_integer(RingHash1) of
                                                    true  -> integer_to_list(RingHash1);
                                                    false -> RingHash1
                                                end,
                                 {[{<<"type">>,      list_to_binary(Type)},
                                   {<<"node">>,      list_to_binary(NodeName)},
                                   {<<"state">>,     list_to_binary(NodeState)},
                                   {<<"ring_cur">>,  list_to_binary(NewRingHash0)},
                                   {<<"ring_prev">>, list_to_binary(NewRingHash1)},
                                   {<<"when">>,      list_to_binary(leo_date:date_format(When))}
                                  ]}
                         end, Nodes)
               end,

    ClusterId_1 = SystemConf#?SYSTEM_CONF.cluster_id,
    ClusterId_2 = case is_atom(ClusterId_1) of
                      true  -> atom_to_list(ClusterId_1);
                      false -> ClusterId_1
                  end,
    DCId_1 = SystemConf#?SYSTEM_CONF.dc_id,
    DCId_2 = case is_atom(DCId_1) of
                 true  -> atom_to_list(DCId_1);
                 false -> DCId_1
             end,

    gen_json({[{<<"system_info">>,
                {[{<<"version">>,    list_to_binary(Version)},
                  {<<"cluster_id">>, list_to_binary(ClusterId_2)},
                  {<<"dc_id">>,      list_to_binary(DCId_2)},
                  {<<"n">>, SystemConf#?SYSTEM_CONF.n},
                  {<<"r">>, SystemConf#?SYSTEM_CONF.r},
                  {<<"w">>, SystemConf#?SYSTEM_CONF.w},
                  {<<"d">>, SystemConf#?SYSTEM_CONF.d},
                  {<<"dc_awareness_replicas">>,   SystemConf#?SYSTEM_CONF.num_of_dc_replicas},
                  {<<"rack_awareness_replicas">>, SystemConf#?SYSTEM_CONF.num_of_rack_replicas},
                  {<<"ring_size">>,      SystemConf#?SYSTEM_CONF.bit_of_ring},
                  {<<"ring_hash_cur">>,  RH0},
                  {<<"ring_hash_prev">>, RH1},
                  {<<"max_mdc_targets">>,SystemConf#?SYSTEM_CONF.max_mdc_targets}
                 ]}},
               {<<"node_list">>, NodeInfo}
              ]}).


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

    gen_json({[{<<"node_stat">>,
                {[
                  %% config-1
                  {<<"version">>,          list_to_binary(Version)},
                  {<<"log_dir">>,          list_to_binary(leo_misc:get_value('log', Directories, []))},
                  %% config-2
                  {<<"ring_cur">>,         list_to_binary(leo_hex:integer_to_hex(leo_misc:get_value('ring_cur',  RingHashes, 0), 8))},
                  {<<"ring_prev">>,        list_to_binary(leo_hex:integer_to_hex(leo_misc:get_value('ring_prev', RingHashes, 0), 8))},
                  {<<"vm_version">>,       list_to_binary(leo_misc:get_value('vm_version', Statistics, []))},
                  {<<"total_mem_usage">>,  leo_misc:get_value('total_mem_usage',  Statistics, 0)},
                  {<<"system_mem_usage">>, leo_misc:get_value('system_mem_usage', Statistics, 0)},
                  {<<"procs_mem_usage">>,  leo_misc:get_value('proc_mem_usage',   Statistics, 0)},
                  {<<"ets_mem_usage">>,    leo_misc:get_value('ets_mem_usage',    Statistics, 0)},
                  {<<"num_of_procs">>,     leo_misc:get_value('num_of_procs',     Statistics, 0)},
                  {<<"limit_of_procs">>,   leo_misc:get_value('process_limit',    Statistics, 0)},
                  {<<"kernel_poll">>,      list_to_binary(atom_to_list(leo_misc:get_value('kernel_poll', Statistics, false)))},
                  {<<"thread_pool_size">>, leo_misc:get_value('thread_pool_size', Statistics, 0)},
                  %% config-2
                  {<<"handler">>,                  list_to_binary(atom_to_list(leo_misc:get_value('handler', HttpConf, '')))},
                  {<<"port">>,                     leo_misc:get_value('port',             HttpConf, 0)},
                  {<<"ssl_port">>,                 leo_misc:get_value('ssl_port',         HttpConf, 0)},
                  {<<"num_of_acceptors">>,         leo_misc:get_value('num_of_acceptors', HttpConf, 0)},
                  {<<"http_cache">>,               list_to_binary(atom_to_list(leo_misc:get_value('http_cache', HttpConf, '')))},
                  {<<"cache_workers">>,            leo_misc:get_value('cache_workers',            HttpConf, 0)},
                  {<<"cache_expire">>,             leo_misc:get_value('cache_expire',             HttpConf, 0)},
                  {<<"cache_ram_capacity">>,       leo_misc:get_value('cache_ram_capacity',       HttpConf, 0)},
                  {<<"cache_disc_capacity">>,      leo_misc:get_value('cache_disc_capacity',      HttpConf, 0)},
                  {<<"cache_disc_threshold_len">>, leo_misc:get_value('cache_disc_threshold_len', HttpConf, 0)},
                  {<<"cache_disc_dir_data">>,      list_to_binary(leo_misc:get_value('cache_disc_dir_data',    HttpConf, ""))},
                  {<<"cache_disc_dir_journal">>,   list_to_binary(leo_misc:get_value('cache_disc_dir_journal', HttpConf, ""))},
                  {<<"cache_max_content_len">>,    leo_misc:get_value('cache_max_content_len',    HttpConf, 0)},
                  %% large-object
                  {<<"max_chunked_objs">>,         MaxChunkedObjs},
                  {<<"chunked_obj_len">>,          ChunkedObjLen},
                  {<<"max_len_for_obj">>,          MaxObjLen},
                  {<<"reading_chunked_obj_len">>,  leo_misc:get_value('reading_chunked_obj_len',  HttpConf, 0)},
                  {<<"threshold_of_chunk_len">>,   leo_misc:get_value('threshold_of_chunk_len',   HttpConf, 0)}
                 ]}}
              ]});

node_stat(?SERVER_TYPE_STORAGE, State) ->
    Version     = leo_misc:get_value('version',       State, []),
    NumOfVNodes = leo_misc:get_value('num_of_vnodes', State, -1),
    GrpLevel2   = leo_misc:get_value('grp_level_2',   State, []),
    Directories = leo_misc:get_value('dirs',          State, []),
    RingHashes  = leo_misc:get_value('ring_checksum', State, []),
    Statistics  = leo_misc:get_value('statistics',    State, []),
    MsgQueue    = leo_misc:get_value('storage', Statistics, []),

    gen_json({[{<<"node_stat">>,
                {[{<<"version">>,          list_to_binary(Version)},
                  {<<"num_of_vnodes">>,    NumOfVNodes},
                  {<<"grp_level_2">>,      list_to_binary(GrpLevel2)},
                  {<<"log_dir">>,          list_to_binary(leo_misc:get_value('log', Directories, []))},
                  {<<"ring_cur">>,         list_to_binary(leo_hex:integer_to_hex(leo_misc:get_value('ring_cur',  RingHashes, 0), 8))},
                  {<<"ring_prev">>,        list_to_binary(leo_hex:integer_to_hex(leo_misc:get_value('ring_prev', RingHashes, 0), 8))},
                  {<<"vm_version">>,       list_to_binary(leo_misc:get_value('vm_version', Statistics, []))},
                  {<<"total_mem_usage">>,  leo_misc:get_value('total_mem_usage',  Statistics, 0)},
                  {<<"system_mem_usage">>, leo_misc:get_value('system_mem_usage', Statistics, 0)},
                  {<<"procs_mem_usage">>,  leo_misc:get_value('proc_mem_usage',   Statistics, 0)},
                  {<<"ets_mem_usage">>,    leo_misc:get_value('ets_mem_usage',    Statistics, 0)},
                  {<<"num_of_procs">>,     leo_misc:get_value('num_of_procs',     Statistics, 0)},
                  {<<"limit_of_procs">>,   leo_misc:get_value('process_limit',    Statistics, 0)},
                  {<<"kernel_poll">>,      list_to_binary(atom_to_list(leo_misc:get_value('kernel_poll', Statistics, false)))},
                  {<<"thread_pool_size">>, leo_misc:get_value('thread_pool_size', Statistics, 0)},
                  {<<"replication_msgs">>, leo_misc:get_value('num_of_replication_msg', MsgQueue, 0)},
                  {<<"sync_vnode_msgs">>,  leo_misc:get_value('num_of_sync_vnode_msg',  MsgQueue, 0)},
                  {<<"rebalance_msgs">>,   leo_misc:get_value('num_of_rebalance_msg',   MsgQueue, 0)}
                 ]}}
              ]}).

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

    gen_json({[{<<"compaction_status">>,
                {[{<<"status">>,                 Status},
                  {<<"last_compaction_start">>,  list_to_binary(Date)},
                  {<<"total_targets">>,          TotalNumOfTargets},
                  {<<"num_of_pending_targets">>, Targets1},
                  {<<"num_of_ongoing_targets">>, Targets2},
                  {<<"num_of_out_of_targets">>,  Targets3}
                 ]}}
              ]}).


%% @doc Format storage stats
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
    Ratio = ?ratio_of_active_size(ActiveSize, TotalSize),

    gen_json({[
               {<<"active_num_of_objects">>,  ActiveNum},
               {<<"total_num_of_objects">>,   TotalNum},
               {<<"active_size_of_objects">>, ActiveSize},
               {<<"total_size_of_objects">>,  TotalSize},
               {<<"ratio_of_active_size">>,   Ratio},
               {<<"last_compaction_start">>,  list_to_binary(StartStr)},
               {<<"last_compaction_end">>,    list_to_binary(EndStr)}
              ]});

du(detail, StatsList) when is_list(StatsList) ->
    JSON = lists:map(fun({ok, #storage_stats{file_path   = FilePath,
                                             compaction_histories = Histories,
                                             total_sizes = TotalSize,
                                             active_sizes = ActiveSize,
                                             total_num  = Total,
                                             active_num = Active}}) ->
                             {LatestStart1, LatestEnd1} =
                                 case length(Histories) of
                                     0 -> {?NULL_DATETIME, ?NULL_DATETIME};
                                     _ ->
                                         {StartComp, FinishComp} = hd(Histories),
                                         {leo_date:date_format(StartComp), leo_date:date_format(FinishComp)}
                                 end,
                             Ratio = ?ratio_of_active_size(ActiveSize, TotalSize),

                             {[{<<"file_path">>,              list_to_binary(FilePath)},
                               {<<"active_num_of_objects">>,  Active},
                               {<<"total_num_of_objects">>,   Total},
                               {<<"active_size_of_objects">>, ActiveSize},
                               {<<"total_size_of_objects">>,  TotalSize},
                               {<<"ratio_of_active_size">>,   Ratio},
                               {<<"last_compaction_start">>,  list_to_binary(LatestStart1)},
                               {<<"last_compaction_end">>,    list_to_binary(LatestEnd1)}
                              ]};
                        (_) ->
                             []
                     end, StatsList),
    gen_json(JSON);
du(_, _) ->
    gen_json([]).


%% @doc Format s3-gen-key result
%%
-spec(credential(binary(), binary()) ->
             string()).
credential(AccessKeyId, SecretAccessKey) ->
    gen_json({[
               {access_key_id,     AccessKeyId},
               {secret_access_key, SecretAccessKey}
              ]}).


%% @doc Format s3-owers
%%
-spec(users(list(#user_credential{})) ->
             string()).
users(Owners) ->
    JSON = lists:map(fun(User) ->
                             UserId      = leo_misc:get_value(user_id,       User),
                             RoleId      = leo_misc:get_value(role_id,       User),
                             AccessKeyId = leo_misc:get_value(access_key_id, User),
                             CreatedAt   = leo_misc:get_value(created_at,    User),
                             {[{<<"access_key_id">>, AccessKeyId},
                               {<<"user_id">>,       list_to_binary(UserId)},
                               {<<"role_id">>,       RoleId},
                               {<<"created_at">>,    list_to_binary(leo_date:date_format(CreatedAt))}
                              ]}
                     end, Owners),
    gen_json({[{<<"users">>, JSON}]}).


%% @doc Format a endpoint list
%%
-spec(endpoints(list(tuple())) ->
             string()).
endpoints(EndPoints) ->
    JSON = lists:map(fun({endpoint, EP, CreatedAt}) ->
                             {[{<<"endpoint">>,   EP},
                               {<<"created_at">>, list_to_binary(leo_date:date_format(CreatedAt))}
                              ]}
                     end, EndPoints),
    gen_json({[{<<"endpoints">>, JSON}]}).


%% @doc Format a bucket list
%%
-spec(buckets(list(tuple())) ->
             string()).
buckets(Buckets) ->
    JSON = lists:map(fun(#bucket_dto{name  = Bucket,
                                     owner = #user_credential{user_id = Owner},
                                     acls  = Permissions,
                                     cluster_id = ClusterId,
                                     created_at = CreatedAt}) ->
                             CreatedAt_1  = case (CreatedAt > 0) of
                                                true  -> leo_date:date_format(CreatedAt);
                                                false -> []
                                            end,
                             PermissionsStr =
                                 string:join([atom_to_list(Item) ||
                                                 #bucket_acl_info{permissions = [Item|_]} <- Permissions],
                                             ","),
                             {[{<<"bucket">>,      Bucket},
                               {<<"owner">>,       list_to_binary(Owner)},
                               {<<"permissions">>, list_to_binary(PermissionsStr)},
                               {<<"cluster_id">>,  list_to_binary(atom_to_list(ClusterId))},
                               {<<"created_at">>,  list_to_binary(CreatedAt_1)}
                              ]}
                     end, Buckets),
    gen_json({[{<<"buckets">>, JSON}]}).

-spec(bucket_by_access_key(list(#?BUCKET{})) ->
             string()).
bucket_by_access_key(Buckets) ->
    JSON = lists:map(fun(#?BUCKET{name = Bucket,
                                  acls = Permissions,
                                  created_at = CreatedAt}) ->
                             PermissionsStr = string:join([atom_to_list(Item)
                                                           || #bucket_acl_info{permissions = [Item|_]}
                                                                  <- Permissions], ","),
                             CreatedAt_1  = case (CreatedAt > 0) of
                                                true  -> leo_date:date_format(CreatedAt);
                                                false -> []
                                            end,
                             {[{<<"bucket">>,      Bucket},
                               {<<"permissions">>, list_to_binary(PermissionsStr)},
                               {<<"created_at">>,  list_to_binary(CreatedAt_1)}
                              ]}
                     end, Buckets),
    gen_json({[{<<"buckets">>, JSON}]}).


%% @doc Format a acl list
%%
-spec(acls(acls()) ->
             string()).
acls(ACLs) ->
    JSON = lists:map(fun(#bucket_acl_info{user_id = UserId, permissions = Permissions}) ->
                             {[{<<"user_id">>,   UserId},
                               {<<"permissions">>, Permissions}
                              ]}
                     end, ACLs),
    gen_json({[{<<"acls">>, JSON}]}).


cluster_status(Stats) ->
    JSON = lists:map(fun(Items) ->
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
                             Status = leo_misc:get_value('status', Items),
                             NumOfStorages = leo_misc:get_value('members', Items),
                             UpdatedAt = leo_misc:get_value('updated_at', Items),
                             UpdatedAt_1 = case (UpdatedAt > 0) of
                                 true  -> leo_date:date_format(UpdatedAt);
                                 false -> []
                             end,
                             {[{<<"cluster_id">>, list_to_binary(ClusterId_2)},
                               {<<"dc_id">>,      list_to_binary(DCId_2)},
                               {<<"status">>,     list_to_binary(atom_to_list(Status))},
                               {<<"num_of_storages">>, list_to_binary(integer_to_list(NumOfStorages))},
                               {<<"updated_at">>,      list_to_binary(UpdatedAt_1)}
                              ]}
                     end, Stats),
    gen_json({[{<<"cluster_stats">>, JSON}]}).


%% @doc Format an assigned file
%%
-spec(whereis(list()) ->
             string()).
whereis(AssignedInfo) ->
    JSON = lists:map(fun({Node, not_found}) ->
                             {[{<<"node">>,      list_to_binary(Node)},
                               {<<"vnode_id">>,      <<>>},
                               {<<"size">>,          <<>>},
                               {<<"num_of_chunks">>, 0},
                               {<<"clock">>,         <<>>},
                               {<<"checksum">>,      <<>>},
                               {<<"timestamp">>,     <<>>},
                               {<<"delete">>,        0}
                              ]};
                        ({Node, VNodeId, DSize, ChunkedObjs, Clock, Timestamp, Checksum, DelFlag}) ->
                             {[{<<"node">>,          list_to_binary(Node)},
                               {<<"vnode_id">>,      list_to_binary(leo_hex:integer_to_hex(VNodeId, 8))},
                               {<<"size">>,          DSize},
                               {<<"num_of_chunks">>, ChunkedObjs},
                               {<<"clock">>,         list_to_binary(leo_hex:integer_to_hex(Clock, 8))},
                               {<<"checksum">>,      list_to_binary(leo_hex:integer_to_hex(Checksum, 8))},
                               {<<"timestamp">>,     list_to_binary(leo_date:date_format(Timestamp))},
                               {<<"delete">>,        DelFlag}
                              ]}
                     end, AssignedInfo),
    gen_json({[{<<"assigned_info">>, JSON}]}).


%% @doc Format a history list
%%
-spec(histories(list(#history{})) ->
             string()).
histories(_) ->
    [].


-spec(authorized() ->
             string()).
authorized() ->
    [].

-spec(user_id() ->
             string()).
user_id() ->
    [].


-spec(password() ->
             string()).
password() ->
    [].


%%----------------------------------------------------------------------
%% Inner function(s)
%%----------------------------------------------------------------------
%% @doc Generate a JSON-format doc
%%
-spec(gen_json(list()) ->
             binary()).
gen_json(JSON) ->
    case catch jiffy:encode(JSON) of
        {'EXIT', _} ->
            [];
        Result ->
            <<Result/binary, ?CRLF>>
    end.
