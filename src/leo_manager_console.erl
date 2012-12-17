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
%% LeoFS Manager - Console
%% @doc
%% @end
%%======================================================================
-module(leo_manager_console).

-author('Yosuke Hara').

-include("leo_manager.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("leo_s3_libs/include/leo_s3_auth.hrl").
-include_lib("leo_s3_libs/include/leo_s3_user.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/2, stop/0]).
-export([init/1, handle_call/3]).

%%----------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------
start_link(Formatter, Params) ->
    tcp_server:start_link(?MODULE, [Formatter], Params).


stop() ->
    tcp_server:stop().


%%----------------------------------------------------------------------
%% Callback function(s)
%%----------------------------------------------------------------------
init([Formatter]) ->
    {ok, #state{formatter = Formatter}}.


%%----------------------------------------------------------------------
%% Operation-1
%%----------------------------------------------------------------------
handle_call(_Socket, <<?HELP>>, #state{formatter = Formatter} = State) ->
    Reply = Formatter:help(),
    {reply, Reply, State};


%% Command: "version"
%%
handle_call(_Socket, <<?VERSION>>, #state{formatter = Formatter} = State) ->
    {ok, Version} = version(),
    Reply = Formatter:version(Version),
    {reply, Reply, State};


%% Command: "_user-id_"
%%
handle_call(_Socket, ?USER_ID, #state{formatter = Formatter} = State) ->
    Reply = Formatter:user_id(),
    {reply, Reply, State};


%% Command: "_password_"
%%
handle_call(_Socket, ?PASSWORD, #state{formatter = Formatter} = State) ->
    Reply = Formatter:password(),
    {reply, Reply, State};

%% Command: "_authorized_"
%%
handle_call(_Socket, ?AUTHORIZED, #state{formatter = Formatter} = State) ->
    Reply = Formatter:authorized(),
    {reply, Reply, State};

%% Command: "_authorized_"
%%
handle_call(_Socket, <<?LOGIN, ?SPACE, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case login(Command, Option) of
                {ok, User, Credential} ->
                    Formatter:login(User, Credential);
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "status"
%% Command: "status ${NODE_NAME}"
%%
handle_call(_Socket, <<?STATUS, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case status(Command, Option) of
                {ok, {node_list, Props}} ->
                    Formatter:system_info_and_nodes_stat(Props);
                {ok, NodeStatus} ->
                    Formatter:node_stat(NodeStatus);
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command : "detach ${NODE_NAME}"
%%
handle_call(_Socket, <<?DETACH_SERVER, ?SPACE, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case detach(Command, Option) of
                ok ->
                    Formatter:ok();
                {error, {Node, Cause}} ->
                    Formatter:error(Node, Cause);
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "suspend ${NODE_NAME}"
%%
handle_call(_Socket, <<?SUSPEND, ?SPACE, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case suspend(Command, Option) of
                ok ->
                    Formatter:ok();
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "resume ${NODE_NAME}"
%%
handle_call(_Socket, <<?RESUME, ?SPACE, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case resume(Command, Option) of
                ok ->
                    Formatter:ok();
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "start"
%%
handle_call(_Socket, <<?START>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case start(Command) of
                ok ->
                    Formatter:ok();
                {error, {bad_nodes, BadNodes}} ->
                    Formatter:bad_nodes(BadNodes);
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "rebalance"
%%
handle_call(_Socket, <<?REBALANCE>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case rebalance(Command) of
                ok ->
                    Formatter:ok();
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%%----------------------------------------------------------------------
%% Operation-2
%%----------------------------------------------------------------------
%% Command: "du ${NODE_NAME}"
%%
handle_call(_Socket, <<?STORAGE_STATS, ?SPACE, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case du(Command, Option) of
                {ok, {Option1, StorageStats}} ->
                    Formatter:du(Option1, StorageStats);
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "compact ${NODE_NAME}"
%%
handle_call(_Socket, <<?COMPACT, ?SPACE, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case compact(Command, Option) of
                ok ->
                    Formatter:ok();
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%%----------------------------------------------------------------------
%% Operation-3
%%----------------------------------------------------------------------
%% Command: "create-user ${USER_ID} ${PASSWORD}"
%%
handle_call(_Socket, <<?S3_CREATE_USER, ?SPACE, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case s3_create_user(Command, Option) of
                {ok, PropList} ->
                    AccessKeyId     = leo_misc:get_value('access_key_id',     PropList),
                    SecretAccessKey = leo_misc:get_value('secret_access_key', PropList),
                    Formatter:s3_credential(AccessKeyId, SecretAccessKey);
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "update-user-role ${USER_ID} ${ROLE}"
%%
handle_call(_Socket, <<?S3_UPDATE_USER_ROLE, ?SPACE, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case s3_update_user_role(Command, Option) of
                ok ->
                    Formatter:ok();
                not_found = Cause ->
                    Formatter:error(Cause);
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "update-user-password ${USER_ID} ${PASSWORD}"
%%
handle_call(_Socket, <<?S3_UPDATE_USER_PW, ?SPACE, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case s3_update_user_password(Command, Option) of
                ok ->
                    Formatter:ok();
                not_found = Cause ->
                    Formatter:error(Cause);
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "delete-user ${USER_ID} ${PASSWORD}"
%%
handle_call(_Socket, <<?S3_DELETE_USER, ?SPACE, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case s3_delete_user(Command, Option) of
                ok ->
                    Formatter:ok();
                not_found = Cause ->
                    Formatter:error(Cause);
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "get-users"
%%
handle_call(_Socket, <<?S3_GET_USERS>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case s3_get_users(Command) of
                {ok, List} ->
                    Formatter:s3_users(List);
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "set-endpoint ${END_POINT}"
%%
handle_call(_Socket, <<?S3_SET_ENDPOINT, ?SPACE, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case s3_set_endpoint(Command, Option) of
                ok ->
                    Formatter:ok();
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "get-endpoints"
%%
handle_call(_Socket, <<?S3_GET_ENDPOINTS>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case s3_get_endpoints(Command) of
                {ok, EndPoints} ->
                    Formatter:endpoints(EndPoints);
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "del-endpoint ${END_POINT}"
%%
handle_call(_Socket, <<?S3_DEL_ENDPOINT, ?SPACE, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case s3_del_endpoint(Command, Option) of
                ok ->
                    Formatter:ok();
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "get-buckets"
%%
handle_call(_Socket, <<?S3_ADD_BUCKET, ?SPACE, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case s3_add_bucket(Command, Option) of
                ok ->
                    Formatter:ok();
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "get-buckets"
%%
handle_call(_Socket, <<?S3_GET_BUCKETS>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case s3_get_buckets(Command) of
                {ok, Buckets} ->
                    Formatter:buckets(Buckets);
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "whereis ${PATH}"
%%
handle_call(_Socket, <<?WHEREIS, ?SPACE, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case whereis(Command, Option) of
                {ok, AssignedInfo} ->
                    Formatter:whereis(AssignedInfo);
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "purge ${PATH}"
%%
handle_call(_Socket, <<?PURGE, ?SPACE, Option/binary>> = Command, #state{formatter = Formatter} = State) ->
    Reply = case purge(Command, Option) of
                ok ->
                    Formatter:ok();
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "history"
%%
handle_call(_Socket, <<?HISTORY>>, #state{formatter = Formatter} = State) ->
    Reply = case leo_manager_mnesia:get_histories_all() of
                {ok, Histories} ->
                    Formatter:histories(Histories);
                not_found ->
                    Formatter:histories([]);
                {error, Cause} ->
                    Formatter:error(Cause)
            end,
    {reply, Reply, State};


%% Command: "quit"
%%
handle_call(_Socket, <<?QUIT>>, State) ->
    {close, <<?BYE>>, State};


handle_call(_Socket, <<?CRLF>>, State) ->
    {reply, "", State};


handle_call(_Socket, _Data, #state{formatter = Formatter} = State) ->
    Reply = Formatter:error(?ERROR_COMMAND_NOT_FOUND),
    {reply, Reply, State}.


%%----------------------------------------------------------------------
%% Inner function(s)
%%----------------------------------------------------------------------
%% @doc Retrieve version of the system
%% @private
-spec(version() ->
             {ok, string() | list()}).
version() ->
    case application:get_key(leo_manager, vsn) of
        {ok, Version} ->
            {ok, Version};
        _ ->
            {ok, []}
    end.


%% @doc Exec login
%% @private
-spec(login(binary(), binary()) ->
             {ok, #user{}, list()} | {error, any()}).
login(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),
    Token = string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER),

    case (erlang:length(Token) == 2) of
        true ->
            [UserId, Password] = Token,
            case leo_s3_user:auth(UserId, Password) of
                {ok, #user{id = UserId} = User} ->
                    case leo_s3_user:get_credential_by_id(UserId) of
                        {ok, Credential} ->
                            {ok, User, Credential};
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        false ->
            {error, invalid_args}
    end.


%% @doc Retrieve state of each node
%% @private
-spec(status(binary(), binary()) ->
             {ok, any()} | {error, any()}).
status(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),
    Token = string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER),

    case (erlang:length(Token) == 0) of
        true ->
            status(node_list);
        false ->
            [Node|_] = Token,
            status({node_state, Node})
    end.

status(node_list) ->
    {ok, SystemConf} = leo_manager_mnesia:get_system_config(),
    Version = case application:get_key(leo_manager, vsn) of
                  {ok, Vsn} -> Vsn;
                  undefined -> []
              end,
    {ok, {RingHash0, RingHash1}} = leo_redundant_manager_api:checksum(ring),

    S1 = case leo_manager_mnesia:get_storage_nodes_all() of
             {ok, R1} ->
                 lists:map(fun(N) ->
                                   {"S",
                                    atom_to_list(N#node_state.node),
                                    atom_to_list(N#node_state.state),
                                    N#node_state.ring_hash_new,
                                    N#node_state.ring_hash_old,
                                    N#node_state.when_is}
                           end, R1);
             _ ->
                 []
         end,
    S2 = case leo_manager_mnesia:get_gateway_nodes_all() of
             {ok, R2} ->
                 lists:map(fun(N) ->
                                   {"G",
                                    atom_to_list(N#node_state.node),
                                    atom_to_list(N#node_state.state),
                                    N#node_state.ring_hash_new,
                                    N#node_state.ring_hash_old,
                                    N#node_state.when_is}
                           end, R2);
             _ ->
                 []
         end,
    {ok, {node_list, [{system_config, SystemConf},
                      {version,       Version},
                      {ring_hash,     [RingHash0, RingHash1]},
                      {nodes,         S1 ++ S2}
                     ]}};

status({node_state, Node}) ->
    case leo_manager_api:get_node_status(Node) of
        {ok, State} ->
            {ok, State};
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Launch the storage cluster
%% @private
-spec(start(binary()) ->
             ok | {error, any()}).
start(CmdBody) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case leo_manager_api:get_system_status() of
        ?STATE_STOP ->
            {ok, SystemConf} = leo_manager_mnesia:get_system_config(),

            case leo_manager_mnesia:get_storage_nodes_by_status(?STATE_ATTACHED) of
                {ok, Nodes} when length(Nodes) >= SystemConf#system_conf.n ->
                    case leo_manager_api:start() of
                        {error, Cause} ->
                            {error, Cause};
                        {_ResL, []} ->
                            ok;
                        {_ResL, BadNodes} ->
                            {error, {bad_nodes, lists:foldl(fun(Node, Acc) ->
                                                                    Acc ++ [Node]
                                                            end, [], BadNodes)}}
                    end;
                {ok, Nodes} when length(Nodes) < SystemConf#system_conf.n ->
                    {error, "Attached nodes less than # of replicas"};
                Error ->
                    Error
            end;
        ?STATE_RUNNING ->
            {error, "System already started"}
    end.


%% @doc Detach a storage-node
%% @private
-spec(detach(binary(), binary()) ->
             ok | {error, {atom(), string()}} | {error, any()}).
detach(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),
    {ok, SystemConf} = leo_manager_mnesia:get_system_config(),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [] ->
            {error, ?ERROR_NO_NODE_SPECIFIED};
        [Node|_] ->
            NodeAtom = list_to_atom(Node),

            case leo_manager_mnesia:get_storage_node_by_name(NodeAtom) of
                {ok, [#node_state{state = ?STATE_ATTACHED} = NodeState|_]} ->
                    ok = leo_manager_mnesia:delete_storage_node(NodeState),
                    ok = leo_manager_cluster_monitor:demonitor(NodeAtom),
                    ok;
                _ ->
                    case leo_manager_mnesia:get_storage_nodes_by_status(?STATE_RUNNING) of
                        {ok, Nodes} when length(Nodes) >= SystemConf#system_conf.n ->
                            case leo_manager_api:detach(NodeAtom) of
                                ok ->
                                    ok;
                                {error, _} ->
                                    {error, {Node, ?ERROR_COULD_NOT_DETACH_NODE}}
                            end;
                        {ok, Nodes} when length(Nodes) =< SystemConf#system_conf.n ->
                            {error, "Attached nodes less than # of replicas"};
                        _Error ->
                            {error, "Could not get node-status"}
                    end
            end
    end.


%% @doc Suspend a storage-node
%% @private
-spec(suspend(binary(), binary()) ->
             ok | {error, any()}).
suspend(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [] ->
            {error, ?ERROR_NO_NODE_SPECIFIED};
        [Node|_] ->
            case leo_manager_api:suspend(list_to_atom(Node)) of
                ok ->
                    ok;
                {error, Cause} ->
                    {error, Cause}
            end
    end.


%% @doc Resume a storage-node
%% @private
-spec(resume(binary(), binary()) ->
             ok | {error, any()}).
resume(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [] ->
            {error, ?ERROR_NO_NODE_SPECIFIED};
        [Node|_] ->
            case leo_manager_api:resume(list_to_atom(Node)) of
                ok ->
                    ok;
                {error, Cause} ->
                    {error, Cause}
            end
    end.


%% @doc Rebalance the storage cluster
%% @private
-spec(rebalance(binary()) ->
             ok | {error, any()}).
rebalance(CmdBody) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case leo_redundant_manager_api:checksum(?CHECKSUM_RING) of
        {ok, {CurRingHash, PrevRingHash}} when CurRingHash =/= PrevRingHash ->
            case leo_manager_api:rebalance() of
                ok ->
                    ok;
                _Other ->
                    {error, "Fail rebalance"}
            end;
        _Other ->
            {error, "Could not launch the storage"}
    end.


%% @doc Purge an object from the cache
%% @private
-spec(purge(binary(), binary()) ->
             ok | {error, any()}).
purge(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [] ->
            {error, ?ERROR_NO_PATH_SPECIFIED};
        [Key|_] ->
            case leo_manager_api:purge(Key) of
                ok ->
                    ok;
                {error, Cause} ->
                    {error, Cause}
            end
    end.


%% @doc Retrieve the storage stats
%% @private
-spec(du(binary(), binary()) ->
             ok | {error, any()}).
du(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [] ->
            {error, ?ERROR_NO_NODE_SPECIFIED};
        Tokens ->
            Mode = case length(Tokens) of
                       1 -> {summary, lists:nth(1, Tokens)};
                       2 -> {list_to_atom(lists:nth(1, Tokens)),  lists:nth(2, Tokens)};
                       _ -> {error, badarg}
                   end,

            case Mode of
                {error, _Cause} ->
                    {error, ?ERROR_INVALID_ARGS};
                {Option1, Node1} ->
                    case leo_manager_api:stats(Option1, Node1) of
                        {ok, StatsList} ->
                            {ok, {Option1, StatsList}};
                        {error, Cause} ->
                            {error, Cause}
                    end
            end
    end.


%% @doc Compact target node of objects into the object-storages
%% @private
-spec(compact(binary(), binary()) ->
             ok | {error, any()}).
compact(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [] ->
            {error, ?ERROR_NO_NODE_SPECIFIED};
        [Node|_] ->
            NodeAtom = list_to_atom(Node),

            case leo_manager_mnesia:get_storage_node_by_name(NodeAtom) of
                {ok, [#node_state{state = ?STATE_RUNNING}|_]} ->
                    case leo_manager_api:suspend(NodeAtom) of
                        ok ->
                            try
                                case leo_manager_api:compact(NodeAtom) of
                                    {ok, _} ->
                                        ok;
                                    {error, Cause} ->
                                        {error, Cause}
                                end
                            after
                                leo_manager_api:resume(NodeAtom)
                            end;
                        {error, Cause} ->
                            {error, Cause}
                    end;
                _ ->
                    {error, not_running}
            end
    end.


%% @doc Retrieve information of an Assigned object
%% @private
-spec(whereis(binary(), binary()) ->
             ok | {error, any()}).
whereis(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [] ->
            {error, ?ERROR_NO_PATH_SPECIFIED};
        Key ->
            HasRoutingTable = (leo_redundant_manager_api:checksum(ring) >= 0),

            case catch leo_manager_api:whereis(Key, HasRoutingTable) of
                {ok, AssignedInfo} ->
                    {ok, AssignedInfo};
                {_, Cause} ->
                    {error, Cause}
            end
    end.


%% @doc Create a user account (S3)
%% @private
-spec(s3_create_user(binary(), binary()) ->
             ok | {error, any()}).
s3_create_user(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    Ret = case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
              [UserId] ->
                  {ok, {UserId, []}};
              [UserId, Password] ->
                  {ok, {UserId, Password}};
              _ ->
                  {error, "No user-id/password specified"}
          end,
    case Ret of
        {ok, {Arg0, Arg1}} ->
            case leo_s3_user:add(Arg0, Arg1, true) of
                {ok, Keys} ->
                    AccessKeyId     = leo_misc:get_value(access_key_id,     Keys),
                    SecretAccessKey = leo_misc:get_value(secret_access_key, Keys),

                    case Arg1 of
                        [] ->
                            ok = leo_s3_user:update(#user{id       = Arg0,
                                                          role_id  = ?ROLE_GENERAL,
                                                          password = SecretAccessKey});
                        _ ->
                            void
                    end,
                    {ok, [{access_key_id,     AccessKeyId},
                          {secret_access_key, SecretAccessKey}]};
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            {error, Cause}
    end.



%% @doc Update user's role-id
%% @private
-spec(s3_update_user_role(binary(), binary()) ->
             ok | {error, any()}).
s3_update_user_role(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [UserId, RoleId|_] ->
            case leo_s3_user:update(#user{id       = UserId,
                                          role_id  = list_to_integer(RoleId),
                                          password = <<>>}) of
                ok ->
                    ok;
                {error, Cause} ->
                    {error, Cause}
            end;
        _ ->
            {error, "No user-id/role_id specified"}
    end.


%% @doc Update user's password
%% @private
-spec(s3_update_user_password(binary(), binary()) ->
             ok | {error, any()}).
s3_update_user_password(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [UserId, Password|_] ->
            case leo_s3_user:update(#user{id       = UserId,
                                          password = Password}) of
                ok ->
                    ok;
                {error, Cause} ->
                    {error, Cause}
            end;
        _ ->
            {error, "No user-id/password specified"}
    end.


%% @doc Remove a user
%% @private
-spec(s3_delete_user(binary(), binary()) ->
             ok | {error, any()}).
s3_delete_user(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [UserId|_] ->
            case leo_s3_user:delete(UserId) of
                ok ->
                    ok;
                not_found = Cause ->
                    {error, Cause};
                {error, Cause} ->
                    {error, Cause}
            end;
        _ ->
            {error, "No user-id specified"}
    end.


%% @doc Retrieve Users
%% @private
-spec(s3_get_users(binary()) ->
             {ok, list(#credential{})} | {error, any()}).
s3_get_users(CmdBody) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case leo_s3_user:find_all() of
        {ok, Users} ->
            {ok, Users};
        Error ->
            Error
    end.


%% @doc Insert an Endpoint into the manager
%% @private
-spec(s3_set_endpoint(binary(), binary()) ->
             ok | {error, any()}).
s3_set_endpoint(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [] ->
            {error, ?ERROR_INVALID_ARGS};
        [EndPoint|_] ->
            EndPointBin = list_to_binary(EndPoint),
            case leo_manager_api:set_endpoint(EndPointBin) of
                ok ->
                    case leo_s3_endpoint:set_endpoint(EndPointBin) of
                        ok ->
                            ok;
                        {error, Cause} ->
                            {error, Cause}
                    end;
                {error, Cause} ->
                    {error, Cause}
            end
    end.


%% @doc Retrieve an Endpoint from the manager
%% @private
-spec(s3_get_endpoints(binary()) ->
             ok | {error, any()}).
s3_get_endpoints(CmdBody) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case leo_s3_endpoint:get_endpoints() of
        {ok, EndPoints} ->
            {ok, EndPoints};
        not_found ->
            {ok, "Not Found"};
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Remove an Endpoint from the manager
%% @private
-spec(s3_del_endpoint(binary(), binary()) ->
             ok | {error, any()}).
s3_del_endpoint(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [] ->
            {error, ?ERROR_INVALID_ARGS};
        [EndPoint|_] ->
            case leo_s3_endpoint:delete_endpoint(list_to_binary(EndPoint)) of
                ok ->
                    ok;
                not_found ->
                    {error, ?ERROR_ENDPOINT_NOT_FOUND};
                {error, Cause} ->
                    {error, Cause}
            end
    end.


%% @doc Insert an Buckets in the manager
%% @private
-spec(s3_add_bucket(binary(), binary()) ->
             ok | {error, any()}).
s3_add_bucket(CmdBody, Option) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case string:tokens(binary_to_list(Option), ?COMMAND_DELIMITER) of
        [Bucket, AccessKey] ->
            leo_s3_bucket:put(list_to_binary(AccessKey), list_to_binary(Bucket));
        _ ->
            {error, ?ERROR_INVALID_ARGS}
    end.


%% @doc Retrieve an Buckets from the manager
%% @private
-spec(s3_get_buckets(binary()) ->
             ok | {error, any()}).
s3_get_buckets(CmdBody) ->
    _ = leo_manager_mnesia:insert_history(CmdBody),

    case leo_s3_bucket:find_all_including_owner() of
        {ok, Buckets} ->
            {ok, Buckets};
        not_found ->
            {error, "Not Found"};
        {error, Cause} ->
            {error, Cause}
    end.

