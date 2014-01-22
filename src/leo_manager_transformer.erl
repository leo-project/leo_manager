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
%% Leo Manager - API
%% @doc
%% @end
%%======================================================================
-module(leo_manager_transformer).

-author('Yosuke Hara').

-include("leo_manager.hrl").

-export([transform/0]).


%% @doc Migrate data
%%      - bucket  (s3-libs)
%%      - members (redundant-manager)
-spec(transform() ->
             ok).
transform() ->
    %% Update available commands
    ok = leo_manager_mnesia:update_available_commands(?env_available_commands()),

    %% data migration - bucket
    case ?env_use_s3_api() of
        false -> void;
        true  ->
            catch leo_s3_bucket_transform_handler:transform()
    end,

    %% data migration - system-conf
    ok = leo_system_conf_tbl_transformer:transform(),
    {ok, _} = leo_manager_api:load_system_config_with_store_data(),

    %% data migration - members
    {ok, ReplicaNodes} = leo_misc:get_env(leo_redundant_manager, ?PROP_MNESIA_NODES),
    ok = leo_members_tbl_transformer:transform(ReplicaNodes),

    %% data migration - ring
    ok = leo_ring_tbl_transformer:transform(),
    ok.
