%%======================================================================
%%
%% LeoFS Commons
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
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
%% Leo Commons -  Constant/Macro/Record
%%
%%======================================================================
-author('Yosuke Hara').

-record(cluster_node_status, {
          type                :: gateway | storage,
          version = []        :: string(),
          dirs    = []        :: list(),
          avs     = []        :: list(), %% [{dir, # of avs}]
          num_of_read_metas   :: integer(),
          disk_sync_interval  :: integer(),
          ring_checksum       :: string(),
          statistics          :: [any()]
         }).

%% leo_mneisa:
-define(EXPORT_TYPE_TUPLE, tuple).
-define(EXPORT_TYPE_JSON,  json).
-type(export_type() :: ?EXPORT_TYPE_TUPLE | ?EXPORT_TYPE_JSON).


%% Environment values
-define(ETS_ENV_TABLE, 'leo_env_values').

-define(env_log_dir(ServerType),
        case application:get_env(ServerType, log_dir) of
            {ok, EnvLogDir} -> EnvLogDir;
            _ -> "log"
        end).

-define(env_log_level(ServerType),
        case application:get_env(ServerType, log_level) of
            {ok, EnvLogLevel} -> EnvLogLevel;
            _ -> 0
        end).

-define(env_manager_nodes(ServerType),
        case application:get_env(ServerType, managers) of
            {ok, EnvManagerNode} -> EnvManagerNode;
            _ ->
                %% for test-case.
                {ok, CHostname} = inet:gethostname(),
                NewCHostname = "manager_0@" ++ CHostname,
                [NewCHostname]
        end).

-define(update_env_manager_nodes(ServerType, Managers),
        application:set_env(ServerType, managers, Managers)).

-define(env_queue_dir(ServerType),
        case application:get_env(ServerType, queue_dir) of
            {ok, EnvQueueDir} -> EnvQueueDir;
            _ -> "queue"
        end).

%% leo_file related constants
-define(PREAD_TIMEOUT, timer:seconds(1)).
