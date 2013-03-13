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
%% LeoFS Manager - JSON Formatter
%% @doc
%% @end
%%======================================================================
-module(leo_manager_formatter_json).

-author('Yosuke Hara').

-include("leo_manager.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_s3_libs/include/leo_s3_user.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([ok/0, error/1, error/2, help/0, version/1, login/2,
         bad_nodes/1, system_info_and_nodes_stat/1, node_stat/2,
         compact_status/1, du/2, s3_credential/2, s3_users/1, endpoints/1, buckets/1,
         whereis/1, histories/1
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
-spec(login(#user{}, list(tuple())) ->
             string()).
login(User, Credential) ->
    gen_json({[{<<"user">>,
                {[{<<"id">>,            list_to_binary(User#user.id)},
                  {<<"role_id">>,       User#user.role_id},
                  {<<"access_key_id">>, leo_misc:get_value('access_key_id',     Credential)},
                  {<<"secret_key">>,    leo_misc:get_value('secret_access_key', Credential)},
                  {<<"created_at">>,    list_to_binary(leo_date:date_format(User#user.created_at))}
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

    gen_json({[{<<"system_info">>,
                {[{<<"version">>,        list_to_binary(Version)},
                  {<<"n">>,              list_to_binary(integer_to_list(SystemConf#system_conf.n))},
                  {<<"r">>,              list_to_binary(integer_to_list(SystemConf#system_conf.r))},
                  {<<"w">>,              list_to_binary(integer_to_list(SystemConf#system_conf.w))},
                  {<<"d">>,              list_to_binary(integer_to_list(SystemConf#system_conf.d))},
                  {<<"ring_size">>,      list_to_binary(integer_to_list(SystemConf#system_conf.bit_of_ring))},
                  {<<"ring_hash_cur">>,  list_to_binary(integer_to_list(RH0))},
                  {<<"ring_hash_prev">>, list_to_binary(integer_to_list(RH1))}
                 ]}},
               {<<"node_list">>, NodeInfo}
              ]}).


%% @doc Format a cluster node state
%%
-spec(node_stat(string(), #cluster_node_status{}) ->
             string()).
node_stat(?SERVER_TYPE_GATEWAY, State) ->
    Directories  = State#cluster_node_status.dirs,
    RingHashes   = State#cluster_node_status.ring_checksum,
    Statistics   = State#cluster_node_status.statistics,

    gen_json({[{<<"node_stat">>,
                {[{<<"version">>,          list_to_binary(State#cluster_node_status.version)},
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
                  {<<"thread_pool_size">>, leo_misc:get_value('thread_pool_size', Statistics, 0)}
                 ]}}
              ]});

node_stat(?SERVER_TYPE_STORAGE, State) ->
    Directories  = State#cluster_node_status.dirs,
    RingHashes   = State#cluster_node_status.ring_checksum,
    Statistics   = State#cluster_node_status.statistics,
    %% ObjContainer = State#cluster_node_status.avs,

    gen_json({[{<<"node_stat">>,
                {[{<<"version">>,          list_to_binary(State#cluster_node_status.version)},
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
                  {<<"thread_pool_size">>, leo_misc:get_value('thread_pool_size', Statistics, 0)}
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
-spec(s3_credential(binary(), binary()) ->
             string()).
s3_credential(AccessKeyId, SecretAccessKey) ->
    gen_json({[
               {access_key_id,     AccessKeyId},
               {secret_access_key, SecretAccessKey}
              ]}).


%% @doc Format s3-owers
%%
-spec(s3_users(list(#user_credential{})) ->
             string()).
s3_users(Owners) ->
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
    JSON = lists:map(fun({Bucket, #user_credential{user_id = Owner}, Created1}) ->
                             Created2  = case (Created1 > 0) of
                                             true  -> leo_date:date_format(Created1);
                                             false -> []
                                         end,
                             {[{<<"bucket">>,     Bucket},
                               {<<"owner">>,      list_to_binary(Owner)},
                               {<<"created_at">>, list_to_binary(Created2)}
                              ]}
                     end, Buckets),
    gen_json({[{<<"buckets">>, JSON}]}).


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

