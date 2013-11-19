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
    %% data migration#1 - bucket
    case ?env_use_s3_api() of
        false -> void;
        true  ->
            catch leo_s3_bucket_transform_handler:transform()
    end,
    %% data migration#1 - members
    {ok, ReplicaNodes} = leo_misc:get_env(leo_redundant_manager, ?PROP_MNESIA_NODES),
    ok = leo_members_table_transformer:transform(ReplicaNodes),
    ok.
