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
%% Leo Libs
%% @doc
%% @end
%%======================================================================
-ifndef(DEF_REQ_TIMEOUT).
-define(DEF_REQ_TIMEOUT, 30000).
-endif.

-define(ENDPOINT_INFO,  leo_s3_endpoint_info).
-define(ENDPOINT_TABLE, leo_s3_endpoints).
-define(AUTH_INFO,  leo_s3_auth_info).
-define(AUTH_TABLE, leo_s3_credentials).
-define(BUCKET_DB_TYPE,   leo_s3_bucket_db).
-define(BUCKET_INFO,      leo_s3_bucket_info).
-define(BUCKET_TABLE,     leo_s3_buckets).
-define(USERS_TABLE,           leo_s3_users).
-define(USER_CREDENTIAL_TABLE, leo_s3_user_credential).

%% @doc Checksum of entire tables
-record(s3_tbls_checksum, {
          auth   = 0     :: non_neg_integer(),
          bucket = 0     :: non_neg_integer(),
          user   = 0     :: non_neg_integer(),
          credential = 0 :: non_neg_integer()
         }).
