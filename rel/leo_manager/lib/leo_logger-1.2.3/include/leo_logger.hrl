%%======================================================================
%%
%% Leo Logger
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
%% ---------------------------------------------------------------------
%% Leo Logger
%% @doc
%% @end
%%======================================================================
%% log-level
-define(LOG_LEVEL_DEBUG, 0).
-define(LOG_LEVEL_INFO,  1).
-define(LOG_LEVEL_WARN,  2).
-define(LOG_LEVEL_ERROR, 3).
-define(LOG_LEVEL_FATAL, 4).
-type(log_level() :: ?LOG_LEVEL_DEBUG |
                     ?LOG_LEVEL_INFO  |
                     ?LOG_LEVEL_WARN  |
                     ?LOG_LEVEL_ERROR |
                     ?LOG_LEVEL_FATAL).

%% log-appender (for message-log)
-define(LOG_APPENDER_FILE,    'file').
-define(LOG_APPENDER_ESEARCH, 'elastic_search').

-define(FILE_PROP_ROOT_PATH,  'root_path').
-define(FILE_PROP_FILE_NAME,  'org_filename').
-define(FILE_PROP_CUR_NAME,   'cur_filename').
-define(FILE_PROP_HANDLER,    'file_handler').
-define(FILE_PROP_LOG_LEVEL,  'log_level').

-define(ESEARCH_PROP_HOST,    'esearch_host').
-define(ESEARCH_PROP_PORT,    'esearch_port').
-define(ESEARCH_PROP_TIMEOUT, 'esearch_timeout').
-define(ESEARCH_DOC_INDEX,    'esearch_doc_index').
-define(ESEARCH_DOC_TYPE,     'esearch_doc_type').
-define(ESEARCH_PROP_BULK_DURATION, 'esearch_bulk_duration').

-define(LOG_ID_FILE_INFO,  'leo_logger_file_i').
-define(LOG_ID_FILE_ERROR, 'leo_logger_file_e').
-define(LOG_ID_ESEARCH,    'leo_logger_esearch').

-define(DEF_ESEARCH_HOST,     "127.0.0.1").
-define(DEF_ESEARCH_PORT,     9200).
-define(DEF_ESEARCH_TIMEOUT,  5000).
-define(DEF_ESEARCH_BULK_DURATION, 3000).


%%
-record(logger_state, {appender_type         :: atom(),
                       appender_mod          :: atom(),
                       callback_mod          :: atom(),
                       props                 :: list(),
                       level = 0             :: non_neg_integer(),
                       hourstamp = -1        :: {integer(), integer(), integer(), integer()},

                       buffer = []           :: list(any()),
                       buf_duration = 0      :: non_neg_integer(),
                       buf_begining = 0      :: non_neg_integer(),
                       is_buf_output = false :: boolean()
                      }).

-record(message_log,  {level              :: log_level(),
                       module             :: string()|atom(),
                       function           :: string()|atom(),
                       line = 0           :: non_neg_integer(),
                       format  = []       :: string(),
                       message = []       :: [any()],
                       formatted_msg = [] :: string()|binary(),
                       esearch = []       :: list(tuple())
                      }).


-type(log_appender() :: ?LOG_APPENDER_FILE | ?LOG_APPENDER_ESEARCH).

-define(LOG_APPEND_SYNC,  'sync').
-define(LOG_APPEND_ASYNC, 'async').
-define(ETS_LOGGER_GROUP, 'leo_logger_group').

%% macros.
%%
-define(appender_mod(AppenderType),
        case AppenderType of
            ?LOG_APPENDER_FILE    -> leo_logger_appender_file;
            ?LOG_APPENDER_ESEARCH -> leo_logger_appender_esearch;
            _ ->
                undefined
        end).


-define(log(_ModLevel,_ModuleString,_FuncName,_Line,_Format,_Message),
        begin
            leo_logger_client_message:_ModLevel(#message_log{module   = _ModuleString,
                                                             function = _FuncName,
                                                             line     = _Line,
                                                             format   = _Format,
                                                             message  = _Message})
        end).


-define(fatal(_Func,_Format,_Message),
        leo_logger_client_message:fatal(#message_log{level    = ?LOG_LEVEL_FATAL,
                                                     module   = ?MODULE_STRING,
                                                     function = _Func,
                                                     line     = ?LINE,
                                                     format   = _Format,
                                                     message  = _Message})).
-define(fatal(_Func,_MsgL),
        leo_logger_client_message:fatal(#message_log{level    = ?LOG_LEVEL_FATAL,
                                                     module   = ?MODULE_STRING,
                                                     function = _Func,
                                                     line     = ?LINE,
                                                     format   = "~p",
                                                     message  = [_MsgL]})).
-define(error(_Func,_Format,_Message),
        leo_logger_client_message:error(#message_log{level    = ?LOG_LEVEL_ERROR,
                                                     module   = ?MODULE_STRING,
                                                     function = _Func,
                                                     line     = ?LINE,
                                                     format   = _Format,
                                                     message  = _Message})).
-define(error(_Func,_MsgL),
        leo_logger_client_message:error(#message_log{level    = ?LOG_LEVEL_ERROR,
                                                     module   = ?MODULE_STRING,
                                                     function = _Func,
                                                     line     = ?LINE,
                                                     format   = "~p",
                                                     message  = [_MsgL]})).
-define(warn(_Func,_Format,_Message),
        leo_logger_client_message:warn(#message_log{level    = ?LOG_LEVEL_WARN,
                                                    module   = ?MODULE_STRING,
                                                    function = _Func,
                                                    line     = ?LINE,
                                                    format   = _Format,
                                                    message  = _Message})).
-define(warn(_Func,_MsgL),
        leo_logger_client_message:warn(#message_log{level    = ?LOG_LEVEL_WARN,
                                                    module   = ?MODULE_STRING,
                                                    function = _Func,
                                                    line     = ?LINE,
                                                    format   = "~p",
                                                    message  = [_MsgL]})).
-define(info(_Func,_Format,_Message),
        leo_logger_client_message:info(#message_log{level    = ?LOG_LEVEL_INFO,
                                                    module   = ?MODULE_STRING,
                                                    function = _Func,
                                                    line     = ?LINE,
                                                    format   = _Format,
                                                    message  = _Message})).
-define(info(_Func,_MsgL),
        leo_logger_client_message:info(#message_log{level    = ?LOG_LEVEL_INFO,
                                                    module   = ?MODULE_STRING,
                                                    function = _Func,
                                                    line     = ?LINE,
                                                    format   = "~p",
                                                    message  = [_MsgL]})).
-define(debug(_Func,_Format,_Message),
        leo_logger_client_message:debug(#message_log{level    = ?LOG_LEVEL_DEBUG,
                                                     module   = ?MODULE_STRING,
                                                     function = _Func,
                                                     line     = ?LINE,
                                                     format   = _Format,
                                                     message  = _Message})).
-define(debug(_Func,_MsgL),
        leo_logger_client_message:debug(#message_log{level    = ?LOG_LEVEL_DEBUG,
                                                     module   = ?MODULE_STRING,
                                                     function = _Func,
                                                     line     = ?LINE,
                                                     format   = "~p",
                                                     message  = [_MsgL]})).
