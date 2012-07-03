%%======================================================================
%%
%% ARIA: Distributed File System.
%%
%% Copyright (c) 2010-2012 ARIA
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
%% TCS Server.
%%======================================================================
-define(TCP_SERVER_MONITOR_NAME, "tcp_server_monitor").
-define(TCP_SERVER_PREFIX,       "tcp_server_").

-record(tcp_server_params, {
    listen = [binary, {packet, line}, {active, false}, {reuseaddr, true}],
    port                    = 11211,
    num_of_listeners        = 3,
    restart_times           = 3,

    time                    = 60,
    shutdown                = 2000,

    accept_timeout          = infinity,
    accept_error_sleep_time = 3000,
    recv_length             = 0,
    recv_timeout            = infinity
}).

