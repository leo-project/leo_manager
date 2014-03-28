%%======================================================================
%%
%% Leo Manager
%%
%% Copyright (c) 2012-2014 Rakuten, Inc.
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
-module(leo_manager_table_sync).

-author('Yosuke Hara').

-behaviour(gen_server).

-include("leo_manager.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/0,
         stop/0]).
-export([sync/0,
         force_sync/0
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
	       terminate/2,
         code_change/3]).

-undef(DEF_TIMEOUT).
-ifdef(TEST).
-define(CURRENT_TIME,  65432100000).
-define(DEF_INTERVAL,  1000).
-define(DEF_TIMEOUT,   1000).
-else.
-define(CURRENT_TIME,  leo_date:now()).
-define(DEF_INTERVAL,  timer:seconds(30)).
-define(DEF_TIMEOUT,   30000).
-endif.

-record(sync_state, {interval  = ?DEF_INTERVAL :: integer(),
                     timestamp = 0 :: integer()
                    }).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE,
                          [?DEF_INTERVAL], []).

stop() ->
    gen_server:call(?MODULE, stop, 30000).


-spec(sync() -> ok | {error, any()}).
sync() ->
    gen_server:cast(?MODULE, sync).


-spec(force_sync() -> ok | {error, any()}).
force_sync() ->
    gen_server:call(?MODULE, force_sync).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([Interval]) ->
    defer_sync(Interval),
    {ok, #sync_state{interval  = Interval,
                     timestamp = 0}}.


handle_call(stop,_From,State) ->
    {stop, normal, ok, State};

handle_call(force_sync,_From, State) ->
    %% @TODO
    %% ok = exec(),
    {reply, ok, State}.


%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
handle_cast(sync, State) ->
    case catch maybe_sync(State) of
        {'EXIT', _Reason} ->
            {noreply, State};
        NewState ->
            {noreply, NewState}
    end.

%% Function: handle_info(Info, State) -> {noreply, State}          |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
handle_info(_Info, State) ->
    {noreply, State}.

%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
terminate(_Reason, _State) ->
    ok.

%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Heatbeat
%% @private
-spec(maybe_sync(#sync_state{}) ->
             #sync_state{}).
maybe_sync(#sync_state{interval  = Interval,
                       timestamp = Timestamp} = State) ->
    ThisTime = leo_date:now() * 1000,

    case ((ThisTime - Timestamp) < Interval) of
        true ->
            void;
        false ->
            sync_1()
    end,

    defer_sync(Interval),
    State#sync_state{timestamp = leo_date:now() * 1000}.


%% @doc Heartbeat
%% @private
-spec(defer_sync(integer()) ->
             ok | any()).
defer_sync(Time) ->
    catch timer:apply_after(Time, ?MODULE, sync, []).


%% @doc Synchronize remote-cluster's status/configurations
%% @private
sync_1() ->
    case leo_mdcr_tbl_cluster_mgr:all() of
        {ok, Managers} ->
            ok = exec(Managers);
        not_found ->
            void;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "sync/1"},
                                    {line, ?LINE}, {body, Cause}])
    end,
    ok.


%% @doc Retrieve records of users and buckets,
%%      and then if find inconsistent records, fix them
%% @private
-spec(exec(list(#cluster_manager{})) ->
             ok | {error, any()}).
exec([]) ->
    ok;
exec([#cluster_manager{node = _Node,
                       cluster_id = _ClusterId}|Rest]) ->    
    exec(Rest).
