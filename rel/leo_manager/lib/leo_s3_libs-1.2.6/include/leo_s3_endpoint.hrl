%%======================================================================
%%
%% Leo Libs
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
%% Leo Libs - Endpoint
%% @doc
%% @end
%%======================================================================
%% @doc Endpoint Info
%%
-record(endpoint, {
          endpoint = <<>> :: binary(),
          created_at = 0 :: integer()
         }).

-record(endpoint_info, {
          type :: atom(),  %% [master | slave]
          db :: atom(),  %% db-type:[ets | mnesia]
          provider = [] :: [atom()] %% auth-info provides
         }).
