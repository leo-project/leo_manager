%%======================================================================
%%
%% Leo Manager
%%
%% Copyright (c) 2012 Rakuten, Inc.
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
%% LeoFS Manager - JSON Console
%% @doc
%% @end
%%======================================================================
-module(leo_manager_console_json).

-author('Yosuke Hara').

-include("leo_manager.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/1, stop/0]).
-export([init/1, handle_call/3]).


%%----------------------------------------------------------------------
%%
%%----------------------------------------------------------------------
start_link(Params) ->
    tcp_server:start_link(?MODULE, [], Params).

stop() ->
    tcp_server:stop().

%%----------------------------------------------------------------------
%% Callback function(s)
%%----------------------------------------------------------------------
init(_Args) ->
    {ok, []}.


%%----------------------------------------------------------------------
%% Operation-1
%%----------------------------------------------------------------------
%% Command: "version"
%%
handle_call(_Socket, <<?VERSION, _/binary>>, State) ->
    Reply = case application:get_key(leo_manager, vsn) of
                {ok, Version} ->
                    gen_json({[{result, list_to_binary(Version)}]});
                _ ->
                    []
            end,
    {reply, Reply, State};


%% Command: "status"
%% Command: "status ${NODE_NAME}"
%%
handle_call(_Socket, <<?STATUS, Option/binary>> = Command, State) ->
    Reply = case leo_manager_console_commons:status(Command, Option) of
                {ok, {node_list, Props}} ->
                    SystemConf = proplists:get_value('system_config', Props),
                    Version    = proplists:get_value('version',       Props),
                    [RH0, RH1] = proplists:get_value('ring_hash',     Props),
                    _Nodes     = proplists:get_value('nodes',         Props),

                    %% [{[{<<"foo1">>,<<"bar1">>}]},
                    %%  {[{<<"node_info">>,
                    %%     [{[{<<"node">>,<<"storage_0@127.0.0.1">>}]},
                    %%      {[{<<"status">>,<<"running">>}]}]}]},
                    %%  {[{<<"node_info">>,
                    %%     [{[{<<"node">>,<<"storage_1@127.0.0.1">>}]},
                    %%      {[{<<"status">>,<<"suspend">>}]}]}]}]

                    gen_json([
                              {[{<<"version">>,   list_to_binary(Version)}]},
                              {[{<<"n">>,         list_to_binary(integer_to_list(SystemConf#system_conf.n))}]},
                              {[{<<"r">>,         list_to_binary(integer_to_list(SystemConf#system_conf.r))}]},
                              {[{<<"w">>,         list_to_binary(integer_to_list(SystemConf#system_conf.w))}]},
                              {[{<<"d">>,         list_to_binary(integer_to_list(SystemConf#system_conf.d))}]},
                              {[{<<"ring_size">>, list_to_binary(integer_to_list(SystemConf#system_conf.bit_of_ring))}]},
                              {[{<<"ring_hash_cur">>,  list_to_binary(integer_to_list(RH0))}]},
                              {[{<<"ring_hash_prev">>, list_to_binary(integer_to_list(RH1))}]}
                             ]);
                {ok, _NodeStatus} ->
                    %%format_node_state(NodeStatus);
                    ?OK;
                {error, Cause} ->
                    gen_json({[{error, list_to_binary(Cause)}]})
            end,
    {reply, Reply, State};


%% @TODO
%% Command : "detach ${NODE_NAME}"
%%
handle_call(_Socket, <<?DETACH_SERVER, _Option/binary>> = _Command, State) ->
    {reply, [], State};


%% @TODO
%% Command: "suspend ${NODE_NAME}"
%%
handle_call(_Socket, <<?SUSPEND, _Option/binary>> = _Command, State) ->
    {reply, [], State};


%% @TODO
%% Command: "resume ${NODE_NAME}"
%%
handle_call(_Socket, <<?RESUME, _Option/binary>> = _Command, State) ->
    {reply, [], State};


%% Command: "start"
%%
handle_call(_Socket, <<?START, _Option/binary>> = _Command, State) ->
    {reply, [], State};


%% @TODO
%% Command: "rebalance"
%%
handle_call(_Socket, <<?REBALANCE, _Option/binary>> = _Command, State) ->
    {reply, [], State};


%%----------------------------------------------------------------------
%% Operation-2
%%----------------------------------------------------------------------
%% @TODO
%% Command: "du ${NODE_NAME}"
%%
handle_call(_Socket, <<?STORAGE_STATS, _Option/binary>> = _Command, State) ->
    {reply, [], State};


%% @TODO
%% Command: "compact ${NODE_NAME}"
%%
handle_call(_Socket, <<?COMPACT, _Option/binary>> = _Command, State) ->
    {reply, [], State};


%%----------------------------------------------------------------------
%% Operation-3
%%----------------------------------------------------------------------
%% @TODO
%% Command: "s3-gen-key ${USER_ID}"
%%
handle_call(_Socket, <<?S3_GEN_KEY, _Option/binary>> = _Command, State) ->
    {reply, [], State};


%% @TODO
%% Command: "s3-set-endpoint ${END_POINT}"
%%
handle_call(_Socket, <<?S3_SET_ENDPOINT, _Option/binary>> = _Command, State) ->
    {reply, [], State};


%% @TODO
%% Command: "s3-del-endpoint ${END_POINT}"
%%
handle_call(_Socket, <<?S3_DEL_ENDPOINT, _Option/binary>> = _Command, State) ->
    {reply, [], State};


%% @TODO
%% Command: "s3-get-endpoints"
%%
handle_call(_Socket, <<?S3_GET_ENDPOINTS, _Option/binary>> = _Command, State) ->
    {reply, [], State};


%% @TODO
%% Command: "s3-get-buckets"
%%
handle_call(_Socket, <<?S3_GET_BUCKETS, _Option/binary>> = _Command, State) ->
    {reply, [], State};


%% @TODO
%% Command: "whereis ${PATH}"
%%
handle_call(_Socket, <<?WHEREIS, _Option/binary>> = _Command, State) ->
    {reply, [], State};


%% @TODO
%% Command: "purge ${PATH}"
%%
handle_call(_Socket, <<?PURGE, _Option/binary>> = _Command, State) ->
    {reply, [], State};


%% @TODO
%% Command: "history"
%%
handle_call(_Socket, <<?HISTORY, _Option/binary>>, State) ->
    {reply, [], State};


%% @TODO
%% Command: "quit"
%%
handle_call(_Socket, <<?QUIT>>, State) ->
    {close, <<?BYE>>, State};

handle_call(_Socket, <<?CRLF>>, State) ->
    {reply, <<"">>, State};

handle_call(_Socket, _Data, State) ->
    Reply = gen_json({[{error, list_to_binary(?ERROR_COMMAND_NOT_FOUND)}]}),
    {reply, Reply, State}.


%%----------------------------------------------------------------------
%% Inner function(s)
%%----------------------------------------------------------------------
%% @doc Generate a JSON-format doc
%%
-spec(gen_json(list()) ->
             binary()).
gen_json(JSON) ->
    case catch jiffy:encode(JSON) of
        {'EXIT', _} ->
            [];
        Result ->
            Result
    end.

