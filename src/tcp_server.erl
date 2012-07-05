%%======================================================================
%%
%% Leo Manager
%%
%% Copyright (c) 2012
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
%% TCP Server
%%======================================================================
-module(tcp_server).

-author('Yosuke Hara').
-vsn('0.9.0').

-export([behaviour_info/1]).
-export([start_link/3, stop/0]).

-include("tcp_server.hrl").

%% Behaviour Callbacks
behaviour_info(callbacks) ->
    [{init, 1}, {handle_call, 3}];
behaviour_info(_Other) ->
    undefined.

%% External APIs
start_link(Module, Args, Option) ->
    tcp_server_sup:start_link({local, ?MODULE}, Module, Args, Option).

stop() ->
    ok.

