%%======================================================================
%%
%% LeoFS Manaegr - Next Generation Distributed File System.
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
%% LeoFS Manager - Supervisor.
%% @doc
%% @end
%%======================================================================
-module(leo_manager_sup).

-author('Yosuke Hara').
-vsn('0.9.1').

-behaviour(supervisor).

-include("leo_manager.hrl").
-include("tcp_server.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("eunit/include/eunit.hrl").

%% External API
-export([start_link/0, stop/0]).
-export([inspect/2]).


%% Callbacks
-export([init/1]).

-define(CHECK_INTERVAL, 250).

%%-----------------------------------------------------------------------
%% External API
%%-----------------------------------------------------------------------
%% @spec () -> ok
%% @doc start link...
%% @end
start_link() ->
    ListenPort     = ?env_listening_port(leo_manager),
    NumOfAcceptors = ?env_num_of_acceptors(leo_manager),
    Mode           = ?env_mode_of_manager(),
    RedundantNode  = ?env_partner_of_manager_node(),

    case supervisor:start_link({local, ?MODULE}, ?MODULE,
                               [#tcp_server_params{port = ListenPort,
                                                   num_of_listeners = NumOfAcceptors}]) of
        {ok, Pid} ->
            %% Launch Logger
            DefLogDir = "./log/",
            LogDir    = case application:get_env(log_appender) of
                            {ok, [{file, Options}|_]} ->
                                proplists:get_value(path, Options,  DefLogDir);
                            _ ->
                                DefLogDir
                        end,
            ok = leo_logger_client_message:new(LogDir, ?env_log_level(leo_manager), log_file_appender()),

            %% Launch Statistics
            ok = leo_statistics_api:start(?MODULE, leo_manager,
                                          [{snmp, [leo_statistics_metrics_vm]},
                                           {stat, [leo_statistics_metrics_vm]}]),

            %% Launch Auth
            ok = leo_auth_api:start(mnesia, []),

            timer:apply_after(?CHECK_INTERVAL, ?MODULE, inspect, [Mode, RedundantNode]),
            {ok, Pid};
        Error ->
            Error
    end.


%% @doc
%%
-spec(inspect(master | slave, atom()) ->
             ok | {error, any()}).
inspect(master = Mode, []) ->
    Nodes = [node()],
    inspect1(Mode, [], Nodes);
inspect(slave, []) ->
    {error, badarg};
inspect(Mode, [RedundantNode0|_]) ->
    RedundantNode1 =
        case is_atom(RedundantNode0) of
            true  -> RedundantNode0;
            false -> list_to_atom(RedundantNode0)
        end,

    case net_adm:ping(RedundantNode1) of
        pong ->
            inspect1(Mode, RedundantNode1, [node(), RedundantNode1]);
        pang ->
            timer:apply_after(?CHECK_INTERVAL, ?MODULE, inspect, [Mode, [RedundantNode0]])
    end.

inspect1(master = Mode, RedundantNode0, Nodes) ->
    case mnesia:create_schema(Nodes) of
        ok ->
            try
                %% create mnesia's schema.
                mnesia:create_schema(Nodes),
                rpc:multicall(Nodes, application, stop,  [mnesia]),
                rpc:multicall(Nodes, application, start, [mnesia]),

                %% create table into the mnesia.
                leo_manager_mnesia:create_system_config(disc_copies, Nodes),
                leo_manager_mnesia:create_storage_nodes(disc_copies, Nodes),
                leo_manager_mnesia:create_gateway_nodes(disc_copies, Nodes),
                leo_manager_mnesia:create_rebalance_info(disc_copies, Nodes),
                leo_manager_mnesia:create_histories(disc_copies, Nodes),
                leo_redundant_manager_mnesia:create_members(disc_copies, Nodes),
                leo_redundant_manager_mnesia:create_ring_current(disc_copies, Nodes),
                leo_redundant_manager_mnesia:create_ring_prev(disc_copies, Nodes),

                %% Launch Auth
                leo_auth_api:create_credential_table(disc_copies, Nodes)

            catch _:Reason ->
                    ?error("inspect1/3", "cause:~p", [Reason])
            end,

            {ok, SystemConf} = load_system_config_with_store_data(),
            ok = leo_redundant_manager_api:start(Mode, Nodes, ?env_queue_dir(leo_manager),
                                                 [{n,           SystemConf#system_conf.n},
                                                  {r,           SystemConf#system_conf.r},
                                                  {w,           SystemConf#system_conf.w},
                                                  {d,           SystemConf#system_conf.d},
                                                  {bit_of_ring, SystemConf#system_conf.bit_of_ring}]),
            ok;
        {error,{_,{already_exists, _}}} ->
            inspect2(Mode, Nodes);
        {_, Cause} ->
            timer:apply_after(?CHECK_INTERVAL, ?MODULE, inspect, [Mode, [RedundantNode0]]),
            ?error("inspect1/3", "cause:~p", [Cause]),
            {error, Cause}
    end;
inspect1(slave = Mode, _, Nodes) ->
    inspect2(Mode, Nodes).

inspect2(Mode, Nodes) ->
    application:start(mnesia),
    timer:sleep(1000),

    case catch mnesia:system_info(tables) of
        Tbls when length(Tbls) > 1 ->
            ok = mnesia:wait_for_tables(Tbls, 30000),

            case leo_manager_mnesia:get_system_config() of
                {ok, SystemConf} ->
                    ok = leo_redundant_manager_api:start(Mode, Nodes, ?env_queue_dir(leo_manager),
                                                         [{n,           SystemConf#system_conf.n},
                                                          {r,           SystemConf#system_conf.r},
                                                          {w,           SystemConf#system_conf.w},
                                                          {d,           SystemConf#system_conf.d},
                                                          {bit_of_ring, SystemConf#system_conf.bit_of_ring}]);
                Error ->
                    ?error("inspect2/2", "cause:~p", [Error]),
                    Error
            end;
        Tbls when length(Tbls) =< 1 ->
            {error, no_exists};
        Error ->
            ?error("inspect2/2", "cause:~p", [Error]),
            Error
    end.


%% @spec () -> ok |
%%             not_started
%% @doc stop process.
%% @end
stop() ->
    case whereis(?MODULE) of
        Pid when is_pid(Pid) == true ->
            exit(Pid, shutdown),
            ok;
        _ -> not_started
    end.

%% ---------------------------------------------------------------------
%% Callbacks
%% ---------------------------------------------------------------------
%% @spec (Params) -> ok
%% @doc stop process.
%% @end
%% @private
init([TCPServerParams]) ->
    ChildProcs =
        [{leo_manager_controller,
          {leo_manager_controller, start_link, [TCPServerParams]},
          permanent,
          ?SHUTDOWN_WAITING_TIME,
          worker,
          [leo_manager_controller]},

         {leo_manager_cluster_monitor,
          {leo_manager_cluster_monitor, start_link, []},
          permanent,
          ?SHUTDOWN_WAITING_TIME,
          worker,
          [leo_manager_cluster_monitor]}
        ],
    {ok, {_SupFlags = {one_for_one, ?MAX_RESTART, ?MAX_TIME}, ChildProcs}}.


%% ---------------------------------------------------------------------
%% Inner Function(s)
%% ---------------------------------------------------------------------
log_file_appender() ->
    %% app.config:
    %% {log_appender, [{file, [{path, "./log/app/"}]},
    %%                 {zmq,  [{ip,   "10.0.0.1"},
    %%                         {port, 10501}]}
    %%                ]},

    case application:get_env(leo_manager, log_appender) of
        undefined   -> log_file_appender([], []);
        {ok, Value} -> log_file_appender(Value, [])
    end.

log_file_appender([], []) ->
    [{?LOG_ID_FILE_INFO,  ?LOG_APPENDER_FILE},
     {?LOG_ID_FILE_ERROR, ?LOG_APPENDER_FILE}];
log_file_appender([], Acc) ->
    lists:reverse(Acc);
log_file_appender([{Type, _}|T], Acc) when Type == file ->
    log_file_appender(T, [{?LOG_ID_FILE_ERROR, ?LOG_APPENDER_FILE}|[{?LOG_ID_FILE_INFO, ?LOG_APPENDER_FILE}|Acc]]);
log_file_appender([{Type, _}|T], Acc) when Type == zmq ->
    %% @TODO
    log_file_appender(T, [{?LOG_ID_ZMQ, ?LOG_APPENDER_ZMQ}|Acc]).


%% @spec (ConfFileName)-> ok | {error, Cause}
%% @doc load a system config file. a system config file store to mnesia.
%% @end
%% @private
-spec(load_system_config_with_store_data() ->
             {ok, #system_conf{}} | {error, any()}).
load_system_config_with_store_data() ->
    {ok, Props} = application:get_env(leo_manager, system),
    SystemConf = #system_conf{n = proplists:get_value(n, Props, 1),
                              w = proplists:get_value(w, Props, 1),
                              r = proplists:get_value(r, Props, 1),
                              d = proplists:get_value(d, Props, 1),
                              bit_of_ring = proplists:get_value(bit_of_ring, Props, 128)},

    case leo_manager_mnesia:update_system_config(SystemConf) of
        ok ->
            {ok, SystemConf};
        Error ->
            Error
    end.
