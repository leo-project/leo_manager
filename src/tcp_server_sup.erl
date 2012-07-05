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
%% TCP Server - Supervisor
%% @doc
%% @end
%%======================================================================
-module(tcp_server_sup).

-author('Yosuke Hara').
-vsn('0.9.0').

-behaviour(supervisor).

-include("tcp_server.hrl").

%% External API
-export([start_link/4,
         stop/0]).

%% Callbacks
-export([init/1]). 

%%-----------------------------------------------------------------------
%% External API
%%-----------------------------------------------------------------------
start_link(Name, Module, Args, Option) ->
    supervisor:start_link(Name, ?MODULE, [Name, Module, Args, Option]).

stop() ->
    case whereis(?MODULE) of
        Pid when is_pid(Pid) == true ->
            exit(Pid, shutdown),
            ok;
        _ ->
            not_started
    end.

%% ---------------------------------------------------------------------
%% Callbacks
%% ---------------------------------------------------------------------
init([Name, Module, Args, Option]) ->
    case Module:init(Args) of
        {ok, State}  ->
            case gen_tcp:listen(Option#tcp_server_params.port,
                                Option#tcp_server_params.listen) of
                {ok, Socket} ->
                    init_result(Socket, State, Name, Module, Option);
                {error, Reason} ->
                    %% LOG
                    io:format("~p~n", [Reason]),
                    {stop, Reason}
            end;
        {stop, Reason} ->
            Reason;
        _ ->
            {error, []}
    end.


%% ---------------------------------------------------------------------
%% Internal Functions
%% ---------------------------------------------------------------------
init_result(Socket, State, {Locale, Name}, Module, Option) ->
    #tcp_server_params{restart_times = MaxRestarts,
                       time          = Time} = Option,

    MonitorName = list_to_atom(?TCP_SERVER_MONITOR_NAME),

    {ok, {{one_for_one, MaxRestarts, Time},
          [gen_tcp_monitor_spec({Locale, MonitorName}) |
           gen_tcp_acceptor_specs(Socket, State, {Locale, Name}, MonitorName, Module, Option)]}}.

gen_tcp_monitor_spec({Locale, MonitorName}) ->
    {MonitorName, {tcp_server_monitor,
                   start_link,
                   [{Locale, MonitorName}]},
     permanent, brutal_kill, worker, []}.

gen_tcp_acceptor_specs(Socket, State, {Locale,_Name}, MonitorName, Module, Option) ->
    NewMonitorName = case Locale of
                         local -> MonitorName;
                             _ -> {Locale, MonitorName}
                     end,
    Ret = lists:map(
            fun (Id) ->
                    AcceptorName = list_to_atom(?TCP_SERVER_PREFIX ++ integer_to_list(Id)),
                    {AcceptorName, {tcp_server_acceptor,
                                    start_link, [{Locale, AcceptorName},
                                                 Socket,
                                                 State,
                                                 NewMonitorName,
                                                 Module,
                                                 Option]},
                     permanent,
                     Option#tcp_server_params.shutdown,
                     worker,
                     []}
            end,
            lists:seq(1, Option#tcp_server_params.num_of_listeners)),
    Ret.

