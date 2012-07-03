%%======================================================================
%%
%% ARIA: Distributed File System.
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
%% TCP Server  - Acceptor.
%%======================================================================
-module(tcp_server_acceptor).

-author('Yosuke Hara').
-vsn('0.9.0').

%% External API
-export([start_link/6]).

%% Callbacks
-export([init/6, accept/5]).

-include("tcp_server.hrl").

%%-----------------------------------------------------------------------
%% External API
%%-----------------------------------------------------------------------
start_link({Locale, Name}, Socket, State, MonitorName, Module, Option) ->
    {ok, Pid} = proc_lib:start_link(
                  ?MODULE, init,
                  [self(), Socket, State, MonitorName, Module, Option]),

    case Locale of
        local -> register(Name, Pid);
            _ -> global:register_name(Name, Pid)
    end,
    {ok, Pid}.

%% ---------------------------------------------------------------------
%% Callbacks
%% ---------------------------------------------------------------------
init(Parent, Socket, State, MonitorName, Module, Option) ->
    proc_lib:init_ack(Parent, {ok, self()}),
    tcp_server_monitor:register(MonitorName, self()),
    accept(Socket, State, MonitorName, Module, Option).


accept(ListenSocket, State, MonitorName, Module, Option) ->
    case gen_tcp:accept(ListenSocket, Option#tcp_server_params.accept_timeout) of 
        {ok, Socket} ->
            try
                tcp_server_monitor:increment(MonitorName, self()),
                recv(proplists:get_value(
                       active, Option#tcp_server_params.listen),
                     Socket, State, Module, Option)
            catch
                Type:Reason ->
                    io:format("[error] ~p:~p - ~p,~p,~p~n",
                              [?MODULE, "accept/5a", Module, Type, Reason])
            after
                tcp_server_monitor:decrement(MonitorName, self()),
                gen_tcp:close(Socket)
            end;
        {error, Reason} ->
            io:format("[error] ~p:~p - ~p,~p~n",
                      [?MODULE, "accept/5b", Module, Reason]),
            timer:sleep(Option#tcp_server_params.accept_error_sleep_time)
    end,
    accept(ListenSocket, State, MonitorName, Module, Option).

recv(false, Socket, State, Module, Option) ->
    case gen_tcp:recv(Socket,
                      Option#tcp_server_params.recv_length,
                      Option#tcp_server_params.recv_timeout) of
        {ok, Data} ->
            call(false, Socket, Data, State, Module, Option);
        {error, closed} ->
            tcp_closed;
        {error,_Reason} ->
            %% TODO LOG
            error
    end;

recv(true, _DummySocket, State, Module, Option) ->
    receive
        {tcp, Socket, Data} ->
            call(true, Socket, Data, State, Module, Option);
        {tcp_closed, _Socket} ->
            tcp_closed;
        _Error ->
            %% TODO LOG
            error
    after Option#tcp_server_params.recv_timeout ->
            tcp_timeout
    end.

call(Active, Socket, Data, State, Module, Option) ->
    case Module:handle_call(Socket, Data, State) of
        {reply, DataToSend, NewState} ->
            gen_tcp:send(Socket, DataToSend),
            recv(Active, Socket, NewState, Module, Option);
        {noreply, NewState} ->
            recv(Active, Socket, NewState, Module, Option);
        {close, State} ->
            tcp_closed;
        {close, DataToSend, State} ->
            gen_tcp:send(Socket, DataToSend);
        Other ->
            %% TODO LOG
            io:format("~p~n", [Other])            
    end.

