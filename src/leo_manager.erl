%%======================================================================
%%
%% LeoFS Manager -  Next Generation Distributed File System.
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
%% LeoFS Manager
%% @doc
%% @end
%%======================================================================
-module(leo_manager).

-author('Yosuke Hara').
-vsn('0.9.0').

-include("leo_manager.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").

%% Application and Supervisor callbacks
-export([start/0, stop/0]).

%%----------------------------------------------------------------------
%% Application behaviour callbacks
%%----------------------------------------------------------------------
start() ->
    application:start(crypto),
    application:start(leo_manager).

stop() ->
    application:stop(crypto),
    application:stop(mnesia),
    application:stop(leo_manager),
    init:stop().

%% ---------------------------------------------------------------------
%% Internal-Functions
%% ---------------------------------------------------------------------
%% prepare() ->
%% case lists:map(
%%        fun(A) ->
%%                {Type, Option} = A,
%%                check(Type, Option)
%%        end, [{system_conf,    [?SYSTEM_CONF_FILE]}]) of
%%     [{ok, M1, SystemConf}] ->
%%         Ret = [check(n, SystemConf#system_conf.n),
%%                check(r, {SystemConf#system_conf.n, SystemConf#system_conf.r}),
%%                check(w, {SystemConf#system_conf.n, SystemConf#system_conf.w})],
%%         case Ret of
%%             [{ok,N1},{ok,N2},{ok,N3}] -> {ok,   [M1,N1,N2,N3]};
%%             [{_, N1},{_, N2},{_, N3}] -> {error,[M1,N1,N2,N3]}
%%         end;
%%     [{_,  M1}] ->
%%         {error, [M1]}
%% end.

%% check(system_conf, Option) ->
%%     case leo_system_config:load_system_config(Option) of
%%         {ok, SystemConf} ->
%%             {ok, "OK: system-config~n", SystemConf};
%%         {error, _Cause} ->
%%             {error, "NG: invalid system-config~n"}
%%     end;

%% check(n, N)      when N > 3 orelse N < 1              -> {error,"NG: n-quorum~n"};
%% check(n,_N)                                           -> {ok,   "OK: n-quorum~n"};
%% check(r,{ N, R}) when R > 3 orelse R < 1 orelse N < R -> {error,"NG: r-quorum~n"};
%% check(r,{_N,_R})                                      -> {ok,   "OK: r-quorum~n"};
%% check(w,{ N, W}) when W > 3 orelse W < 1 orelse N < W -> {error,"NG: w-quorum~n"};
%% check(w,{_N,_W})                                      -> {ok,   "OK: w-quorum~n"}.
