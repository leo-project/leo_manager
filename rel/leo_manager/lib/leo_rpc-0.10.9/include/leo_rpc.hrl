%%======================================================================
%%
%% Leo Backend-DB
%%
%% Copyright (c) 2012-2016 Rakuten, Inc.
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
%%======================================================================

%% @doc: data parse related definitions
%%
-define(CRLF, <<"\r\n">>).
-define(CRLF_STR, "\r\n").
-define(CRLF_CRLF_STR, "\r\n\r\n").

-define(BIN_ORG_TYPE_BIN,   <<"B">>).
-define(BIN_ORG_TYPE_TERM,  <<"M">>).
-define(BIN_ORG_TYPE_TUPLE, <<"T">>).

-define(BLEN_MOD_METHOD_LEN,     8).
-define(BLEN_TYPE_LEN,           1).
-define(BLEN_PARAM_LEN,          8).
-define(BLEN_PARAM_TERM,        32).
-define(BLEN_BODY_LEN,          32).
-define(BLEN_LEN_TYPE_WITH_BODY, 8).
-define(RET_ERROR, <<"+ERROR\r\n">>).


%% @doc: rpc-server related definitions
%%
-define(POOL_NAME, 'leo_tcp_pool').
-define(DEF_ACCEPTORS,      64).
-define(DEF_LISTEN_IP,      "127.0.0.1").
-define(DEF_LISTEN_PORT,    13075).
-define(DEF_LISTEN_TIMEOUT, 5000).
-define(DEF_CLIENT_POOL_NAME_PREFIX, "leo_rpc_client_").
-define(DEF_CLIENT_CONN_POOL_SIZE, 4).
-define(DEF_CLIENT_CONN_BUF_SIZE,  4).
-define(DEF_CLIENT_WORKER_SUP_ID, 'leo_rpc_client_worker').
-define(MAX_NUM_OF_REQ, 64).

-record(rpc_info, { module :: atom(),
                    method :: atom(),
                    params = [] :: list(any())
                  }).


%% Errors
-define(ERROR_DUPLICATE_DEST, 'duplicate_destination').


%% @doc: rpc-connection/rpc-client related definitions
%%
-define(TBL_RPC_CONN_INFO, 'leo_rpc_conn_info').

-record(rpc_conn, { host = [] :: string(),
                    ip :: string(),
                    port = ?DEF_LISTEN_PORT :: pos_integer(),
                    workers = 0 :: pos_integer(),
                    manager_ref :: atom()
                  }).

-record(tcp_server_params, {
          prefix_of_name = "leo_rpc_listener_"  :: string(),
          listen = [binary, {packet, line},
                    {active, false}, {reuseaddr, true},
                    {backlog, 1024}, {nodelay, true}],
          port                    = 13075 :: pos_integer(),
          num_of_listeners        = 64    :: pos_integer(),
          restart_times           = 3     :: pos_integer(),
          time                    = 60    :: pos_integer(),
          shutdown                = 2000  :: pos_integer(),
          accept_timeout          = infinity,
          accept_error_sleep_time = 3000  :: pos_integer(),
          recv_length             = 0     :: non_neg_integer(),
          recv_timeout            = 5000  :: pos_integer()
         }).


-ifdef(TEST).
-define(DEF_INSPECT_INTERVAL, 500).
-else.
-define(DEF_INSPECT_INTERVAL, 5000).
-endif.


%% @doc Retrieve connection pool size for rpc-client
-define(env_rpc_con_pool_size(),
        case application:get_env(leo_rpc, 'connection_pool_size') of
            {ok, _ENV_CON_POOL_SIZE} ->
                _ENV_CON_POOL_SIZE;
            _ ->
                ?DEF_CLIENT_CONN_POOL_SIZE
        end).

%% @doc Retrieve connection buffer size for rpc-client
-define(env_rpc_con_buffer_size(),
        case application:get_env(leo_rpc, 'connection_buffer_size') of
            {ok, _ENV_CON_BUF_SIZE} ->
                _ENV_CON_BUF_SIZE;
            _ ->
                ?DEF_CLIENT_CONN_BUF_SIZE
        end).

%% @doc Retrieve maximum number of requests
-define(env_max_req_for_reconnection(),
        case application:get_env(leo_rpc, 'max_requests_for_reconnection') of
            {ok, _ENV_MAX_REQ_FOR_RECON} ->
                _ENV_MAX_REQ_FOR_RECON;
            _ ->
                ?MAX_NUM_OF_REQ
        end).
