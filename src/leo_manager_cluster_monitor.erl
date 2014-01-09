%%======================================================================
%%
%% Leo Manager
%%
%% Copyright (c) 2012-2013 Rakuten, Inc.
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
%% Leo Manager - Cluster Node(s) Monitor.
%% @doc
%% @end
%%======================================================================
-module(leo_manager_cluster_monitor).

-author('Yosuke Hara').

-behaviour(gen_server).

-include("leo_manager.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").


-export([start_link/0,
         stop/0]).

-export([register/4, register/7,
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
-define(CURRENT_TIME, leo_date:now()).
-define(APPLY_AFTER_TIME, 1000).
-endif.

-undef(DEF_TIMEOUT).
-define(DEF_TIMEOUT, 30000).

-record(registration, {pid           :: pid(),
                       node          :: atom(),
                       type          :: gateway|storage,
                       times         :: integer(),
                       level_1 = []  :: string(),
                       level_2 = []  :: string(),
                       num_of_vnodes = ?DEF_NUMBER_OF_VNODES :: pos_integer()
                      }).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:call(?MODULE, stop, ?DEF_TIMEOUT).


%% @doc Register gateway and storage pid in monitor.
%%
-spec(register(first|again, pid(), atom(), storage|gateway) ->
             ok).
register(RequestedTimes, Pid, Node, TypeOfNode) ->
    RegistrationInfo = #registration{pid   = Pid,
                                     node  = Node,
                                     type  = TypeOfNode,
                                     times = RequestedTimes},
    gen_server:call(?MODULE, {register, RegistrationInfo}, ?DEF_TIMEOUT).

-spec(register(first|again, pid(), atom(), storage, string(), string(), pos_integer()) ->
             ok).
register(RequestedTimes, Pid, Node, TypeOfNode, L1Id, L2Id, NumOfVNodes) ->
    RegistrationInfo = #registration{pid   = Pid,
                                     node  = Node,
                                     type  = TypeOfNode,
                                     times = RequestedTimes,
                                     level_1 = L1Id,
                                     level_2 = L2Id,
                                     num_of_vnodes = NumOfVNodes},
    gen_server:call(?MODULE, {register, RegistrationInfo}, ?DEF_TIMEOUT).


%% @doc Demonitor pid from monitor.
%%
-spec(demonitor(Node::atom()) -> ok | undefined).
demonitor(Node) ->
    gen_server:call(?MODULE, {demonitor, Node}, ?DEF_TIMEOUT).


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
    gen_server:call(?MODULE, {get_server_node_alias, NewNode}, ?DEF_TIMEOUT).


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
          _Htbl = [],
          _Pids = []}}.


handle_call(stop, _From, State) ->
    {stop, normal, ok, State};


handle_call({register, RegistrationInfo}, _From, {Refs, Htbl, Pids} = Arg) ->
    ?debug("handle_call - register", "requested-times:~w, node:~w",
           [RegistrationInfo#registration.times,
            RegistrationInfo#registration.node]),
    #registration{pid   = Pid,
                  node  = Node,
                  type  = TypeOfNode} = RegistrationInfo,

    case is_exists_proc(Htbl, Node) of
        true ->
            Ret = register_fun_1(RegistrationInfo),
            {reply, Ret, Arg};
        false ->
            case register_fun_1(RegistrationInfo) of
                ok ->
                    MonitorRef = erlang:monitor(process, Pid),
                    ProcInfo   = {Pid, {atom_to_list(Node), Node, TypeOfNode, MonitorRef}},
                    {reply, ok, {_Refs = [MonitorRef | Refs],
                                 _Htbl = [ProcInfo   | Htbl],
                                 _Pids = Pids}};
                Error ->
                    {reply, Error, Arg}
            end
    end;

handle_call({demonitor, Node}, _From, {MonitorRefs, Htbl, Pids} = Arg) ->
    case find_by_node_alias(Htbl, Node) of
        undefined ->
            {reply, undefined, Arg};
        {Pid, MonitorRef} ->
            erlang:demonitor(MonitorRef),
            NewHtbl = delete_by_pid(Htbl, Pid),

            {reply, ok, {_MonitorRefs = lists:delete(MonitorRef, MonitorRefs),
                         NewHtbl,
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
              end, undefined, Htbl),
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
handle_info({'DOWN', MonitorRef, _Type, Pid, _Info}, {MonitorRefs, Htbl, Pids}) ->
    timer:sleep(random:uniform(500)),

    NewHtbl =
        case find_by_pid(Htbl, Pid) of
            undefined ->
                Htbl;
            {_, Node, TypeOfNode, _} ->
                ?error("handle_call - DOWN", "node:~w", [Node]),

                case TypeOfNode of
                    gateway ->
                        catch leo_manager_mnesia:update_gateway_node(
                                #node_state{node    = Node,
                                            state   = ?STATE_STOP,
                                            when_is = ?CURRENT_TIME});
                    storage ->
                        case catch leo_manager_mnesia:get_storage_node_by_name(Node) of
                            {ok, [#node_state{state = State} = NodeInfo|_]} ->
                                case update_node_state(down, State, Node) of
                                    delete ->
                                        leo_manager_mnesia:delete_storage_node(NodeInfo);
                                    _Other ->
                                        void
                                end;
                            _Error ->
                                void
                        end
                end,
                delete_by_pid(Htbl, Pid)
        end,

    erlang:demonitor(MonitorRef),
    {noreply, {_MonitorRefs = lists:delete(MonitorRef, MonitorRefs),
               NewHtbl,
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
update_node_state(start, ?STATE_ATTACHED, _Node) -> ok;
update_node_state(start, ?STATE_DETACHED, _Node) -> ok;
update_node_state(start, ?STATE_SUSPEND,   Node) -> update_node_state_1(?STATE_RESTARTED, Node);
update_node_state(start, ?STATE_RUNNING,  _Node) -> ok;
update_node_state(start, ?STATE_STOP,      Node) -> update_node_state_1(?STATE_RESTARTED, Node);
update_node_state(start, ?STATE_RESTARTED,_Node) -> ok;
update_node_state(start, not_found,        Node) -> update_node_state_1(?STATE_ATTACHED,  Node, leo_date:clock());

update_node_state(down,  ?STATE_ATTACHED, _Node) -> delete;
update_node_state(down,  ?STATE_DETACHED, _Node) -> ok;
update_node_state(down,  ?STATE_SUSPEND,  _Node) -> ok;
update_node_state(down,  ?STATE_RUNNING,   Node) -> update_node_state_1(?STATE_STOP, Node);
update_node_state(down,  ?STATE_STOP,     _Node) -> ok;
update_node_state(down,  ?STATE_RESTARTED, Node) -> update_node_state_1(?STATE_STOP, Node);
update_node_state(down,  not_found,       _Node) -> ok.

update_node_state_1(State, Node) ->
    update_node_state_1(State, Node, -1).
update_node_state_1(State, Node, Clock) ->
    case leo_manager_mnesia:update_storage_node_status(
           update, #node_state{node          = Node,
                               state         = State,
                               ring_hash_new = [],
                               ring_hash_old = [],
                               when_is       = ?CURRENT_TIME}) of
        ok ->
            case leo_redundant_manager_api:update_member_by_node(Node, Clock, State) of
                ok ->
                    leo_manager_api:distribute_members(ok, []);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


%% @doc Register pid of remote-nodes in this monitor.
%%
-spec(get_remote_node_proc_fun() ->
             ok).
get_remote_node_proc_fun() ->
    case leo_manager_api:get_nodes() of
        {ok, Members} ->
            ?debugVal(Members),
            lists:foreach(
              fun({_Type, _Node, ?STATE_DETACHED}) -> void;
                 ({_Type, _Node, ?STATE_SUSPEND})  -> void;
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

    case leo_misc:node_existence(Node) of
        true ->
            Mod = leo_storage_api,
            case rpc:call(Node, Mod, register_in_monitor, [again], ?DEF_TIMEOUT) of
                ok              -> ok;
                {_, Cause}      -> {error, Cause};
                timeout = Cause -> {error, Cause}
            end;
        false ->
            {error, 'not_connected'}
    end;

get_remote_node_proc_fun(gateway, Node) ->
    timer:sleep(50),
    ?debugVal(Node),

    case leo_misc:node_existence(Node) of
        true ->
            Mod = leo_gateway_api,
            case rpc:call(Node, Mod, register_in_monitor, [again], ?DEF_TIMEOUT) of
                ok              -> ok;
                {_, Cause}      -> {error, Cause};
                timeout = Cause -> {error, Cause}
            end;
        false ->
            {error, 'not_connected'}
    end.


%% @doc Returns true if exists a process, false otherwise
%%
-spec(is_exists_proc(list(), atom()) ->
             boolean()).
is_exists_proc(ProcList, Node) ->
    lists:foldl(fun({_K, {_, N,_,_}},_S) when Node == N ->
                        true;
                   ({_K, {_,_N,_,_}}, S) ->
                        S
                end, false, ProcList).


%% @doc Retrieve a process by the node-alias
%%
-spec(find_by_node_alias(list(), atom()) ->
             {pid(), reference()}).
find_by_node_alias(ProcList, Node) ->
    lists:foldl(fun({ Pid, {_, N,_, MonitorRef}},_S) when Node == N ->
                        {Pid, MonitorRef};
                   ({_Pid, {_,_N,_,_MonitorRef}}, S) ->
                        S
                end, undefined, ProcList).


%% @doc Returns a process by the pid
%%
-spec(find_by_pid(list(), pid()) ->
             tuple()).
find_by_pid(ProcList, Pid0) ->
    lists:foldl(fun({Pid1, ProcInfo}, undefined) when Pid0 == Pid1 ->
                        ProcInfo;
                   (_, Acc) ->
                        Acc
                end, undefined, ProcList).


%% @doc Remove a process by the pid.
%%
-spec(delete_by_pid(list(), pid()) ->
             list()).
delete_by_pid(ProcList, Pid0) ->
    lists:foldl(fun({Pid1, _}, Acc) when Pid0 == Pid1 ->
                        Acc;
                   (ProcInfo,  Acc) ->
                        [ProcInfo|Acc]
                end, [], ProcList).


%% @doc Register a remote-node's process into the monitor
%%
-spec(register_fun_1(#registration{}) ->
             ok | {error, any()}).
register_fun_1(#registration{node = Node,
                             type = gateway}) ->
    case leo_manager_mnesia:get_gateway_node_by_name(Node) of
        {ok, [#node_state{state = ?STATE_RUNNING}|_]} ->
            ok;
        {error, Cause} ->
            ?error("register_fun_1/2", "cause:~p", [Cause]),
            {error, Cause};
        _Other ->
            case rpc:call(Node, leo_redundant_manager_api,
                          checksum, [?CHECKSUM_RING], ?DEF_TIMEOUT) of
                {ok, {Chksum0, Chksum1}} ->
                    leo_manager_mnesia:update_gateway_node(
                      #node_state{node    = Node,
                                  state   = ?STATE_RUNNING,
                                  ring_hash_new = leo_hex:integer_to_hex(Chksum0, 8),
                                  ring_hash_old = leo_hex:integer_to_hex(Chksum1, 8),
                                  when_is = ?CURRENT_TIME});
                _Error ->
                    void
            end
    end;

register_fun_1(#registration{node = Node,
                             type = storage} = RegistrationInfo) ->
    Ret = leo_manager_mnesia:get_storage_node_by_name(Node),
    register_fun_2(Ret, RegistrationInfo).


-spec(register_fun_2({ok, list(#node_state{})} | not_found| {error, any()}, #registration{}) ->
             ok | {error, any()}).

register_fun_2({ok, [#node_state{state = ?STATE_RUNNING}|_]}, #registration{node = Node,
                                                                            type = storage}) ->
    %% synchronize member and ring
    leo_manager_api:synchronize(?CHECKSUM_MEMBER, Node),
    leo_manager_api:synchronize(?CHECKSUM_RING,   Node),
    ok;

register_fun_2({ok, [#node_state{state = ?STATE_DETACHED}|_]}, #registration{node = Node,
                                                                             type = storage,
                                                                             level_1 = L1,
                                                                             level_2 = L2,
                                                                             num_of_vnodes = NumOfVNodes}) ->
    case leo_manager_api:attach(Node, L1, L2, NumOfVNodes) of
        ok ->
            update_node_state_1(?STATE_RESTARTED, Node);
        {error, Cause} ->
            ?error("register_fun_2/2", "node:~w, cause:~p", [Node, Cause]),
            {error, Cause}
    end;

register_fun_2({ok, [#node_state{state = State}|_]}, #registration{node = Node,
                                                                   type = storage}) ->
    update_node_state(start, State, Node);

register_fun_2(not_found = State, #registration{node = Node,
                                                type = storage,
                                                level_1 = L1,
                                                level_2 = L2,
                                                num_of_vnodes = NumOfVNodes}) ->
    case leo_manager_api:attach(Node, L1, L2, NumOfVNodes) of
        ok ->
            case update_node_state(start, State, Node) of
                ok ->
                    ok;
                {error, Cause} ->
                    ?error("register_fun_2/2", "node:~w, cause:~p", [Node, Cause]),
                    {error, Cause}
            end;
        {error, Cause} ->
            ?error("register_fun_2/2", "node:~w, cause:~p", [Node, Cause]),
            {error, Cause}
    end;

register_fun_2({error, Cause}, #registration{node = Node,
                                             type = storage}) ->
    ?error("register_fun_2/2", "node:~w, cause:~p", [Node, Cause]),
    {error, Cause}.
