%%======================================================================
%%
%% Leo Bucket
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
%% Leo Bucket
%% @doc
%% @end
%%======================================================================

%% Predefined users
%%
-define(GRANTEE_ALL_USER, <<"http://acs.amazonaws.com/groups/global/AllUsers">>).
-define(GRANTEE_AUTHENTICATED_USER, <<"http://acs.amazonaws.com/groups/global/AuthenticatedUsers">>).
% for display
-define(GRANTEE_DISPLAY_ALL_USER, "Everyone").
-define(GRANTEE_DISPLAY_OWNER, "Me").

-ifdef(TEST).
-define(DEF_BUCKET_PROP_SYNC_INTERVAL, 60).
-else.
-define(DEF_BUCKET_PROP_SYNC_INTERVAL, 300).
-endif.

%% Permissions
%%
-define(CANNED_ACL_PRIVATE,            "private").
-define(CANNED_ACL_PUBLIC_READ,        "public-read").
-define(CANNED_ACL_PUBLIC_READ_WRITE,  "public-read-write").
-define(CANNED_ACL_AUTHENTICATED_READ, "authenticated-read").

-define(RED_METHOD_COPY, 'copy').
-define(RED_METHOD_EC, 'erasure_code').
-define(RED_METHOD_STR_COPY, "copy").
-define(RED_METHOD_STR_EC, "erasure-code").

-type permission()  :: read|write|read_acp|write_acp|full_control.
-type permissions() :: [permission()].

-record(bucket_acl_info, {
          user_id = <<>> :: binary(), %% correspond with user table's user_id
          permissions = full_control :: permissions()  %% permissions
         }).

-type acls() :: [#bucket_acl_info{}].


%% - LeoFS-v0.14.9
-record(bucket, {
          name = <<>> :: binary(),       %% bucket name
          access_key = <<>> :: binary(), %% access-key-id
          created_at = 0 :: integer()    %% created date and time
         }).

%% - LeoFS-v0.16.0 - v1.0.0-pre3
-record(bucket_0_16_0, {
          name = <<>> :: binary(),              %% bucket name
          access_key_id = <<>> :: binary(),     %% access-key-id
          acls = [] :: acls(),                  %% acl list
          last_synchroized_at = 0 :: integer(), %% last synchronized date and time
          created_at = 0 :: integer(),          %% created date and time
          last_modified_at = 0 :: integer()     %% modified date and time
         }).

%% - LeoFS-v1.0.0 - v1.2.12
-ifdef(TEST).
-record(bucket_1, {
          name = <<>> :: binary(),              %% bucket name
          access_key_id = <<>> :: binary(),     %% access-key-id
          acls = [] :: acls(),                  %% acl list
          cluster_id :: atom(),                 %% cluster_id
          last_synchroized_at = 0 :: integer(), %% last synchronized date and time
          created_at = 0 :: integer(),          %% created date and time
          last_modified_at = 0 :: integer(),    %% modified date and time
          del = false :: boolean()              %% delete-flag
         }).
-else.
-record(bucket_1, {
          name = <<>> :: binary(),
          access_key_id = <<>> :: binary(),
          acls = [] :: acls(),
          cluster_id :: atom(),
          last_synchroized_at = 0 :: integer(),
          created_at = leo_date:now() :: integer(),
          last_modified_at = leo_date:now() :: integer(),
          del = false :: boolean()
         }).
-endif.

%% - LeoFS-v1.4.0 -
-ifdef(TEST).
-record(bucket_2, {
          name = <<>> :: binary(),                    %% bucket name
          access_key_id = <<>> :: binary(),           %% access-key-id
          acls = [] :: acls(),                        %% acl list
          cluster_id :: atom(),                       %% cluster_id
          redundancy_method = ?RED_METHOD_COPY :: atom(), %% redundancy method: [copy|erasure-code]
          cp_params = undefined :: undefined|{pos_integer(),
                                              pos_integer(),
                                              pos_integer(),
                                              pos_integer()}, %% replication params
          ec_lib = undefined :: undefined|atom(),      %% erasure-code method: @DEPEND:leo_jerasure
          ec_params = undefined :: undefined|{pos_integer(),
                                              pos_integer()}, %% erasure-code params: @DEPEND:leo_jerasure
          last_synchroized_at = 0 :: integer(),       %% last synchronized date and time
          created_at = 0 :: integer(),                %% created date and time
          last_modified_at = 0 :: integer(),          %% modified date and time
          del = false :: boolean()                    %% delete-flag
         }).
-else.
-record(bucket_2, {
          %% basic items
          name = <<>> :: binary(),
          access_key_id = <<>> :: binary(),
          acls = [] :: acls(),
          cluster_id :: atom(),
          %% for the erasure-coding support
          redundancy_method = ?RED_METHOD_COPY :: atom(),
          cp_params = undefined :: undefined|{pos_integer(),
                                              pos_integer(),
                                              pos_integer(),
                                              pos_integer()},
          ec_lib = undefined :: undefined|atom(),
          ec_params = undefined :: undefined|{pos_integer(),
                                              pos_integer()},
          %% timestamps and flag
          last_synchroized_at = 0 :: integer(),
          created_at = leo_date:now() :: integer(),
          last_modified_at = leo_date:now() :: integer(),
          del = false :: boolean()
         }).
-endif.

%% Current bucket-record is 'bucket_2'
-define(BUCKET, bucket_2).


-record(bucket_info, {
          type :: atom(), %% [master | slave]
          db :: atom(),   %% db-type:[ets | mnesia]
          provider = [] :: list(), %% auth-info provides
          sync_interval = 0 :: non_neg_integer() %% interval in seconrd to use syncing local records with manager's
         }).

%% {Name, Owner_1, Permissions_1, CreatedAt}
-record(bucket_dto, {
          name = <<>> :: binary(),    %% bucket name
          redundancy_method = ?RED_METHOD_COPY :: atom(), %% redundancy method
          cp_params = undefined :: undefined|{pos_integer(),
                                              pos_integer(),
                                              pos_integer(),
                                              pos_integer()}, %% copy-object params
          ec_lib = undefined :: undefined|atom(), %% erasure-coding method
          ec_params = undefined :: undefined|{pos_integer(),
                                              pos_integer()}, %% erasure-coding params
          owner :: tuple(),           %% owner info
          acls = [] :: acls(),        %% acl list
          cluster_id :: atom(),       %% cluster_id
          created_at = 0 :: integer() %% created date and time
         }).
