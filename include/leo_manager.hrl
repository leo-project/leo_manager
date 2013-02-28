%%====================================================================
%%
%% Leo FS Manager
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
%% -------------------------------------------------------------------
%% LeoFS Manager - Constant/Macro/Record
%%
%% -------------------------------------------------------------------
-author('yosuke hara').
-include_lib("eunit/include/eunit.hrl").

%% constants
-define(SHUTDOWN_WAITING_TIME, 2000).
-define(MAX_RESTART,              5).
-define(MAX_TIME,                60).

-ifdef(TEST).
-define(DEF_TIMEOUT,           1000). %% 1sec
-define(DEF_MONITOR_INTERVAL,  3000). %% 3sec
-else.
-define(DEF_TIMEOUT,          30000). %% 30sec
-define(DEF_MONITOR_INTERVAL, 20000). %% 20sec
-endif.

-define(SYSTEM_CONF_FILE,  "conf/leofs.conf").


%% manager-related tables
-define(TBL_STORAGE_NODES,  'leo_storage_nodes').
-define(TBL_GATEWAY_NODES,  'leo_gateway_nodes').
-define(TBL_SYSTEM_CONF,    'leo_system_conf').
-define(TBL_REBALANCE_INFO, 'leo_rebalance_info').
-define(TBL_HISTORIES,      'leo_histories').
-define(TBL_AVAILABLE_CMDS, 'leo_available_commands').

%% server-type
-define(SERVER_TYPE_STORAGE, "S").
-define(SERVER_TYPE_GATEWAY, "G").


%% command-related
-define(COMMAND_ERROR,        "Command Error").
-define(COMMAND_DELIMITER,    " \r\n").

-define(OK,                   "OK\r\n").
-define(ERROR,                "ERROR\r\n").
-define(CRLF,                 "\r\n").
-define(SPACE,                " ").
-define(STORED,               "STORED\r\n").
-define(NOT_STORED,           "NOT_STORED\r\n").
-define(DELETED,              "DELETED\r\n").
-define(NOT_FOUND,            "NOT FOUND\r\n").
-define(SERVER_ERROR,         "SERVER_ERROR").
-define(BYE,                  "BYE\r\n").

-define(CMD_HELP,             "help").
-define(CMD_QUIT,             "quit").
-define(CMD_VERSION,          "version").
-define(CMD_STATUS,           "status").
-define(CMD_ATTACH,           "attach").
-define(CMD_DETACH,           "detach").
-define(CMD_SUSPEND,          "suspend").
-define(CMD_RESUME,           "resume").
-define(CMD_START,            "start").
-define(CMD_REBALANCE,        "rebalance").
-define(CMD_COMPACT,          "compact").
-define(CMD_CREATE_USER,      "create-user").
-define(CMD_UPDATE_USER_ROLE, "update-user-role").
-define(CMD_UPDATE_USER_PW,   "update-user-password").
-define(CMD_DELETE_USER,      "delete-user").
-define(CMD_GET_USERS,        "get-users").
-define(CMD_SET_ENDPOINT,     "set-endpoint").
-define(CMD_DEL_ENDPOINT,     "delete-endpoint").
-define(CMD_GET_ENDPOINTS,    "get-endpoints").
-define(CMD_ADD_BUCKET,       "add-bucket").
-define(CMD_GET_BUCKETS,      "get-buckets").
-define(CMD_DU,               "du").
-define(CMD_WHEREIS,          "whereis").
-define(CMD_HISTORY,          "history").
-define(CMD_PURGE,            "purge").
-define(LOGIN,                "login").
-define(AUTHORIZED,           <<"_authorized_\r\n">>).
-define(USER_ID,              <<"_user_id_\r\n">>).
-define(PASSWORD,             <<"_password_\r\n">>).

-define(COMMANDS, [{?CMD_HELP,          "help"},
                   {?CMD_QUIT,          "quit"},
                   {?CMD_VERSION,       "version"},
                   {?CMD_STATUS,        "status [${NODE]}"},
                   {?CMD_HISTORY,       "history"},
                   {?CMD_WHEREIS,       "whereis ${PATH}"},
                   {?CMD_DETACH,        "detach ${NODE}"},
                   {?CMD_SUSPEND,       "suspend ${NODE}"},
                   {?CMD_RESUME,        "resume ${NODE}"},
                   {?CMD_DETACH,        "detach ${NODE}"},
                   {?CMD_START,         "start"},
                   {?CMD_REBALANCE,     "rebalance"},
                   {?CMD_COMPACT,       lists:append(
                                          ["compact start ${storage-node} all|${storage_pids} [${num_of_compact_proc}]", ?CRLF,
                                           "compact suspend ${storage-node}", ?CRLF,
                                           "compact resume  ${storage-node}", ?CRLF,
                                           "compact status  ${storage-node} "
                                          ])},
                   {?CMD_DU,            "du ${NODE}"},
                   {?CMD_PURGE,         "purge ${PATH}"},
                   {?CMD_CREATE_USER,   "create-user ${USER-ID} [${PASSWORD}]"},
                   {?CMD_DELETE_USER,   "delete-user ${USER-ID}"},
                   {?CMD_UPDATE_USER_ROLE, "update-user-role ${USER-ID} ${ROLE-ID}"},
                   {?CMD_UPDATE_USER_PW,   "update-user-password ${USER-ID} ${PASSWORD}"},
                   {?CMD_GET_USERS,     "get-users"},
                   {?CMD_SET_ENDPOINT,  "set-endpoint ${ENDPOINT}"},
                   {?CMD_DEL_ENDPOINT,  "delete-endpoint ${ENDPOINT}"},
                   {?CMD_GET_ENDPOINTS, "get-endpoints"},
                   {?CMD_ADD_BUCKET,    "add-bucket ${BUCKET} ${ACCESS_KEY_ID}"},
                   {?CMD_GET_BUCKETS,   "get-buckets"}
                  ]).
-record(cmd_state, {name :: string(),
                    help :: string(),
                    available = true :: boolean()
                   }).

%% du-command-related
-define(NULL_DATETIME, "____-__-__ __:__:__").

%% compaction-related
-define(COMPACT_START,      "start").
-define(COMPACT_SUSPEND,    "suspend").
-define(COMPACT_RESUME,     "resume").
-define(COMPACT_STATUS,     "status").
-define(COMPACT_TARGET_ALL, "all").


%% membership
-define(DEF_NUM_OF_ERROR_COUNT, 3).

%% error
-define(ERROR_COULD_NOT_CONNECT,        "Could not connect").
-define(ERROR_NODE_NOT_EXISTS,          "Node not exist").
-define(ERROR_FAILED_COMPACTION,        "Failed compaction").
-define(ERROR_FAILED_GET_STORAGE_STATS, "Failed to get storage stats").
-define(ERROR_ENDPOINT_NOT_FOUND,       "Specified endpoint not found").
-define(ERROR_COULD_NOT_ATTACH_NODE,    "Could not attach a node").
-define(ERROR_COULD_NOT_DETACH_NODE,    "Could not detach a node").
-define(ERROR_NOT_SPECIFIED_COMMAND,    "Command not exist").
-define(ERROR_NOT_SPECIFIED_NODE,       "Not specified node").
-define(ERROR_NO_CMODE_SPECIFIED,       "Not specified compaction mode").
-define(ERROR_INVALID_PATH,             "Invalid path").
-define(ERROR_INVALID_ARGS,             "Invalid arguments").
-define(ERROR_COULD_NOT_STORE,          "Could not store value").
-define(ERROR_INVALID_BUCKET_FORMAT,    "Invalid bucket format").


%% type of console
-define(CONSOLE_CUI,  'cui').
-define(CONSOLE_JSON, 'json').
-define(MOD_TEXT_FORMATTER, 'leo_manager_formatter_text').
-define(MOD_JSON_FORMATTER, 'leo_manager_formatter_json').

%% records
%%
-define(AUTH_NOT_YET, 0).
-define(AUTH_USERID_1, 1).
-define(AUTH_USERID_2, 2).
-define(AUTH_PASSWORD, 3).
-define(AUTH_DONE,     5).
-type(auth() :: ?AUTH_NOT_YET  |
                ?AUTH_USERID_1 |
                ?AUTH_USERID_2 |
                ?AUTH_PASSWORD |
                ?AUTH_DONE).

-ifdef(TEST).
-record(state, {formatter         :: atom(),
                auth = ?AUTH_DONE :: auth(),
                user_id = []      :: string(),
                password = []     :: string()
               }).
-else.
-record(state, {formatter         :: atom(),
                auth = ?AUTH_DONE :: auth(),
                user_id = []      :: string(),
                password = []     :: string()
               }).
-endif.

-record(rebalance_info, {
          vnode_id         = -1  :: integer(),
          node                   :: atom(),
          total_of_objects = 0   :: integer(),
          num_of_remains   = 0   :: integer(),
          when_is          = 0   :: integer() %% Posted at
         }).

-record(history, {
          id                     :: integer(),
          command = []           :: string(), %% Command
          created = -1           :: integer() %% Created
         }).


%% macros
%%
-define(env_mode_of_manager(),
        case application:get_env(leo_manager, manager_mode) of
            {ok, EnvModeOfManager} -> EnvModeOfManager;
            _ -> 'master'
        end).

-define(env_partner_of_manager_node(),
        case application:get_env(leo_manager, manager_partners) of
            {ok, EnvPartnerOfManagerNode} -> EnvPartnerOfManagerNode;
            _ -> []
        end).

-define(env_listening_port_cui(),
        case application:get_env(leo_manager, port_cui) of
            {ok, EnvCUIListeningPort} -> EnvCUIListeningPort;
            _ -> 10010
        end).

-define(env_listening_port_json(),
        case application:get_env(leo_manager, port_json) of
            {ok, EnvJSONListeningPort} -> EnvJSONListeningPort;
            _ -> 10020
        end).

-define(env_num_of_acceptors_cui(),
        case application:get_env(leo_manager, num_of_acceptors_cui) of
            {ok, EnvCUINumOfAcceptors} -> EnvCUINumOfAcceptors;
            _ -> 3
        end).

-define(env_num_of_acceptors_json(),
        case application:get_env(leo_manager, num_of_acceptors_json) of
            {ok, EnvJSONNumOfAcceptors} -> EnvJSONNumOfAcceptors;
            _ -> 3
        end).

-define(env_console_user_id(),
        case application:get_env(leo_manager, console_user_id) of
            {ok, EnvConsoleUserId} -> EnvConsoleUserId;
            _ -> "leo"
        end).

-define(env_console_password(),
        case application:get_env(leo_manager, console_password) of
            {ok, EnvConsolePassword} -> EnvConsolePassword;
            _ -> "faststorage"
        end).

-define(env_num_of_compact_proc(),
        case application:get_env(leo_manager, num_of_compact_proc) of
            {ok, EnvConsoleNumOfCompactProc} -> EnvConsoleNumOfCompactProc;
            _ -> 3
        end).

-define(env_available_commands(),
        case application:get_env(leo_manager, available_commands) of
            {ok, EnvAvailableCommands} -> EnvAvailableCommands;
            _ -> all
        end).

