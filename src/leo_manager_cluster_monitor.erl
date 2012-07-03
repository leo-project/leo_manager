%%======================================================================
%%
%% LeoFS Manager - Next Generation Distributed File System.
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
%% LeoFS Manager - Cluster Node(s) Monitor.
%% @doc
%% @end
%%======================================================================
-module(leo_manager_cluster_monitor).

-author('Yosuke Hara').
-vsn('0.9.0').

-behaviour(gen_server).

-include("leo_manager.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").


-export([start_link/0,
         stop/0]).

-export([register/4,
         demonitor/1,
         get_remote_node_proc/0,
         get_server_node_alias/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-ifdef(TEST).
-define(CURRENT_TIME, 65432100000).
-define(APPLY_AFTER_TIME, 0).
-else.
-define(CURRENT_TIME, leo_utils:now()).
-define(APPLY_AFTER_TIME, 1000).
-endif.


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:call(?MODULE, stop).


%% @doc Register gateway and storage pid in monitor.
%%
-spec(register(first|again, pid(), atom(), storage|gateway) ->
             ok).
register(RequestedTimes, Pid, Node, TypeOfNode) ->
    gen_server:call(?MODULE, {register, RequestedTimes, Pid, Node, TypeOfNode}).


%% @doc Demonitor pid from monitor.
%%
-spec(demonitor(Node::atom()) -> ok | undefined).
demonitor(Node) ->
    gen_server:call(?MODULE, {demonitor, Node}).


%% @doc Retrieve pid of remote-nodes.
%%
-spec(get_remote_node_proc() -> ok ).
get_remote_node_proc() ->
    gen_server:cast(?MODULE, {get_remote_node_proc}).


%% @doc Retrieve node-alias.
%%
-spec(get_server_node_alias(Node::atom()) -> {ok, tuple()}).
get_server_node_alias(Node) ->
    NewNode = case is_atom(Node) of
                  true -> Node;
                  _    -> list_to_atom(Node)
              end,
    gen_server:call(?MODULE, {get_server_node_alias, NewNode}).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([]) ->
    _Res = timer:apply_after(?APPLY_AFTER_TIME, ?MODULE, get_remote_node_proc, []),
    {ok, {_Refs = [],
          _Htbl = leo_hashtable:new(),
          _Pids = []}}.


handle_call(stop, _From, State) ->
    {stop, normal, ok, State};


handle_call({register, _RequestedTimes, Pid, Node, TypeOfNode}, _From, {Refs, Htbl, Pids} = Arg) ->
    %% ?info("handle_call - register", "requested-times:~w, node:~w", [RequestedTimes, Node]),

    case is_exists_proc(Htbl, Node) of
        true ->
            {reply, ok, Arg};
        false ->
            MonitorRef = erlang:monitor(process, Pid),
            leo_hashtable:put(Htbl, Pid, {atom_to_list(Node), Node, TypeOfNode, MonitorRef}),

            case TypeOfNode of
                gateway ->
                    case leo_manager_mnesia:get_gateway_node_by_name(Node) of
                        {ok, [#node_state{state = ?STATE_RUNNING}|_]} ->
                            void;
                        not_found ->
                            void;
                        {error, Cause} ->
                            ?error("handle_call/3 - register", "cause:~p", [Cause]);
                        _Other ->
                            leo_manager_mnesia:update_gateway_node(
                              #node_state{node    = Node,
                                          state   = ?STATE_RUNNING,
                                          when_is = ?CURRENT_TIME})
                    end;
                storage ->
                    case leo_manager_mnesia:get_storage_node_by_name(Node) of
                        {ok, [#node_state{state = State}|_]} ->
                            update_node_state(start, State, Node);
                        not_found = State ->
                            update_node_state(start, State, Node);
                        {error, Cause} ->
                            ?error("handle_call/3 - register", "cause:~p", [Cause])
                    end
            end,
            {reply, ok, {_Refs = [MonitorRef | Refs],
                         _Htbl = Htbl,
                         _Pids = Pids}}
    end;

handle_call({demonitor, Node}, _From, {MonitorRefs, Hashtable, Pids} = Arg) ->
    %% ?debug("handle_call - demonitor", "node:~w, ret:~p", [Node, find_by_node_alias(Hashtable, Node)]),

    case find_by_node_alias(Hashtable, Node) of
        undefined ->
            {reply, undefined, Arg};

        {Pid, MonitorRef} ->
            erlang:demonitor(MonitorRef),
            leo_hashtable:delete(Hashtable, Pid),

            {reply, ok, {_MonitorRefs = lists:delete(MonitorRef, MonitorRefs),
                         Hashtable,
                         _Pids = lists:delete(Pid, Pids)}}
    end;


handle_call({get_server_node_alias, Node}, _From, {Refs, Htbl, Pids}) ->
    Reply = lists:foldl(
              fun(X, N) ->
                      {_, {_, NodeAlias, _TypeOfNode, _MonitorRef}} = X,
                      case Node of
                          NodeAlias ->
                              NodeAlias;
                          _ ->
                              N
                      end
              end, undefined, leo_hashtable:all(Htbl)),
    {reply, Reply, {Refs, Htbl, Pids}}.


%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
handle_cast({get_remote_node_proc}, State) ->
    ok = get_remote_node_proc_fun(),
    {noreply, State};

handle_cast(_Message, State) ->
    {noreply, State}.


%% Function: handle_info(Info, State) -> {noreply, State}          |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
handle_info({'DOWN', MonitorRef, _Type, Pid, _Info}, {MonitorRefs, Hashtable, Pids}) ->
    timer:sleep(random:uniform(500)),

    try
        case leo_hashtable:get(Hashtable, Pid) of
            undefined ->
                void;
            {_, Node, TypeOfNode, _} ->
                ?error("handle_call - DOWN", "node:~w", [Node]),
                leo_hashtable:delete(Hashtable, Pid),

                case TypeOfNode of
                    gateway ->
                        leo_manager_mnesia:update_gateway_node(
                          #node_state{node    = Node,
                                      state   = ?STATE_DOWNED,
                                      when_is = ?CURRENT_TIME});
                    storage ->
                        case leo_manager_mnesia:get_storage_node_by_name(Node) of
                            {ok, [#node_state{state = State} = NodeInfo|_]} ->

                                case update_node_state(down, State, Node) of
                                    delete ->
                                        leo_manager_mnesia:delete_storage_node_by_name(NodeInfo);
                                    _Other ->
                                        void
                                end;
                            _Error ->
                                void
                        end
                end
        end
    catch
        _:Reason ->
            ?debugVal({"DOWN", Reason})
    end,

    erlang:demonitor(MonitorRef),
    {noreply, {_MonitorRefs = lists:delete(MonitorRef, MonitorRefs),
               Hashtable,
               _Pids = lists:delete(Pid, Pids)}};

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
%% Internal Functions
%%--------------------------------------------------------------------
%% @doc Modify node state.
%%
-spec(update_node_state(start|down, leo_redundant_manger:node_state(), atom()) ->
             ok | delete | {error, any()}).
update_node_state(start, ?STATE_IDLING,   _Node) -> ok;
update_node_state(start, ?STATE_ATTACHED, _Node) -> ok;
update_node_state(start, ?STATE_DETACHED, _Node) -> ok;
update_node_state(start, ?STATE_SUSPEND,   Node) -> update_node_state1(?STATE_RESTARTED, Node);
update_node_state(start, ?STATE_RUNNING,  _Node) -> ok;
update_node_state(start, ?STATE_DOWNED,    Node) -> update_node_state1(?STATE_RESTARTED, Node);
update_node_state(start, ?STATE_STOP,      Node) -> update_node_state1(?STATE_RESTARTED, Node);
update_node_state(start, ?STATE_RESTARTED,_Node) -> ok;
update_node_state(start, not_found,        Node) -> update_node_state1(?STATE_IDLING,    Node);

update_node_state(down,  ?STATE_IDLING,   _Node) -> delete;
update_node_state(down,  ?STATE_ATTACHED, _Node) -> delete;
update_node_state(down,  ?STATE_DETACHED, _Node) -> ok;
update_node_state(down,  ?STATE_SUSPEND,  _Node) -> ok;
update_node_state(down,  ?STATE_RUNNING,   Node) -> update_node_state1(?STATE_DOWNED, Node);
update_node_state(down,  ?STATE_DOWNED,   _Node) -> ok;
update_node_state(down,  ?STATE_STOP,     _Node) -> ok;
update_node_state(down,  ?STATE_RESTARTED,_Node) -> delete;
update_node_state(down,  not_found,       _Node) -> ok.

update_node_state1(State, Node) ->
    leo_manager_mnesia:update_storage_node_status(
      update, #node_state{node          = Node,
                          state         = State,
                          ring_hash_new = [],
                          ring_hash_old = [],
                          when_is       = ?CURRENT_TIME}).


%% @doc Register pid of remote-nodes in this monitor.
%%
-spec(get_remote_node_proc_fun() ->
             ok).
get_remote_node_proc_fun() ->
    case leo_manager_api:get_cluster_nodes() of
        {ok, Members} ->
            lists:foreach(
              fun({_Type, _Node, ?STATE_DETACHED}) -> void;
                 ({_Type, _Node, ?STATE_SUSPEND})  -> void;
                 ({_Type, _Node, ?STATE_DOWNED})   -> void;
                 ({_Type, _Node, ?STATE_STOP})     -> void;
                 ({ Type,  Node, _}) ->
                      get_remote_node_proc_fun(Type, Node)
              end, Members);
        _ ->
            void
    end,
    ok.

get_remote_node_proc_fun(storage, Node) ->
    timer:sleep(50),

    case leo_utils:node_existence(Node) of
        true ->
            Mod = leo_storage_api,
            case rpc:call(Node, Mod, register_in_monitor, [again]) of
                ok              -> Ret = ok;
                {_, Cause}      -> Ret = {error, Cause};
                timeout = Cause -> Ret = {error, Cause}
            end,
            Ret;
        false ->
            {error, 'not_connected'}
    end;

get_remote_node_proc_fun(gateway, Node) ->
    timer:sleep(50),

    case leo_utils:node_existence(Node) of
        true ->
            Mod = leo_gateway_api,
            case rpc:call(Node, Mod, register_in_monitor, [again]) of
                ok              -> Ret = ok;
                {_, Cause}      -> Ret = {error, Cause};
                timeout = Cause -> Ret = {error, Cause}
            end,
            Ret;
        false ->
            {error, 'not_connected'}
    end.


is_exists_proc(Hashtable, Node) ->
    lists:foldl(fun({_K, {_, N,_,_}},_S) when Node == N ->
                        true;
                   ({_K, {_,_N,_,_}}, S) ->
                        S
                end, false, leo_hashtable:all(Hashtable)).

find_by_node_alias(Hashtable, Node) ->
    lists:foldl(fun({ Pid, {_, N,_, MonitorRef}},_S) when Node == N ->
                        {Pid, MonitorRef};
                   ({_Pid, {_,_N,_,_MonitorRef}}, S) ->
                        S
                end, undefined, leo_hashtable:all(Hashtable)).

