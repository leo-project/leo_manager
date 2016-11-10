%%======================================================================
%%
%% LeoProject - Savanna Commons
%%
%% Copyright (c) 2014 Rakuten, Inc.
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
-author('Yosuke Hara').

-type(sv_metric() :: binary()).
-type(sv_schema() :: binary()).
-type(sv_key()    :: binary()).
-type(sv_keyval() :: {binary(), any()}).
-type(sv_values() :: [tuple()]).
-type(sv_metric_grp() :: binary()).

-type(sv_result_counter()   :: integer()).
-type(sv_result_gauge()     :: float()|integer()).
-type(sv_result_histogram() :: {[sv_result_gauge()], [tuple()]}).
-type(sv_result() :: sv_result_counter()|
                     sv_result_gauge()|
                     sv_result_histogram()).

-define(ERROR_ETS_NOT_AVAILABLE,    "ETS is not available").
-define(ERROR_MNESIA_NOT_START,     "Mnesia is not available").
-define(ERROR_COULD_NOT_GET_SCHEMA, "Could not get a schema").

-define(METRIC_COUNTER,   'counter').
-define(METRIC_HISTOGRAM, 'histogram').
-define(METRIC_HISTORY,   'history').
-define(METRIC_GAUGE,     'gauge').
-type(sv_metric_type() :: ?METRIC_COUNTER |
                          ?METRIC_HISTOGRAM |
                          ?METRIC_HISTORY |
                          ?METRIC_GAUGE).


-define(MOD_METRICS_COUNTER,   'svc_metrics_counter').
-define(MOD_METRICS_HISTOGRAM, 'svc_metrics_histogram').
-define(MOD_METRICS_GAUGE,     'svc_metrics_gauge').
-type(mod_sv_metrics() :: ?MOD_METRICS_COUNTER |
                          ?MOD_METRICS_HISTOGRAM |
                          ?MOD_METRICS_GAUGE).


-define(HISTOGRAM_UNIFORM, 'uniform').
-define(HISTOGRAM_EXDEC,   'exdec').
-define(HISTOGRAM_SLIDE,   'slide').
-type(sv_histogram_type() :: ?HISTOGRAM_UNIFORM |
                             ?HISTOGRAM_EXDEC |
                             ?HISTOGRAM_SLIDE).

-define(HISTOGRAM_CONS_SAMPLE, 'sample').
-define(HISTOGRAM_CONS_ALPHA,  'alpha').
-type(sv_histogram_constraint() :: ?HISTOGRAM_CONS_SAMPLE |
                                   ?HISTOGRAM_CONS_ALPHA).

-define(TBL_SCHEMAS,    'sv_schemas').
-define(TBL_COLUMNS,    'sv_columns').
-define(TBL_METRIC_GRP, 'sv_metric_grp').

-define(COL_TYPE_COUNTER,   'counter').
-define(COL_TYPE_H_UNIFORM, 'histogram_uniform').
-define(COL_TYPE_H_SLIDE,   'histogram_slide').
-define(COL_TYPE_H_EXDEC,   'histogram_exdec').
-define(COL_TYPE_HISTORY,   'history').
-define(COL_TYPE_GAUGE,     'gauge').
-type(sv_column_type() :: ?COL_TYPE_COUNTER |
                          ?COL_TYPE_H_UNIFORM |
                          ?COL_TYPE_H_SLIDE |
                          ?COL_TYPE_H_EXDEC |
                          ?COL_TYPE_HISTORY |
                          ?COL_TYPE_GAUGE |
                          undefined
                          ).

-define(SV_PREFIX_NAME, "sv_").


%% @doc Expiration of a process (metric-server)
-ifdef(TEST).
-define(SV_EXPIRATION_TIME, 60). %% 60sec (1min)
-else.
%% -define(SV_EXPIRATION_TIME, 60).
-define(SV_EXPIRATION_TIME, 'infinity').
-endif.


%% @doc Unit of windows
-define(SV_WINDOW_10S, 10).
-define(SV_WINDOW_30S, 30).
-define(SV_WINDOW_1M,  60).
-define(SV_WINDOW_5M, 300).

%% @doc Unit of steps
-define(SV_STEP_1M,   60).
-define(SV_STEP_5M,  300).
-define(SV_STEP_10M, 600).
-define(SV_STEP_15M, 900).


%% Macro
%% @doc Generate a metric-name from a schema-name and a key
-define(sv_metric_name(_MetricGroup, _Key),
        list_to_atom(lists:append([?SV_PREFIX_NAME,
                                   binary_to_list(_MetricGroup), ".", binary_to_list(_Key)]))).

%% @doc Retrieve a schema-name and a key from a metric-name
-define(sv_schema_and_key(_Name),
        begin
            _Name_1 = atom_to_list(_Name),
            _PrefixLen = length(?SV_PREFIX_NAME),
            _Index  = string:chr(_Name_1, $.),

            _MetricGrp = list_to_binary(string:sub_string(_Name_1, 1 + _PrefixLen, _Index - 1)),
            _Column    = list_to_binary(string:sub_string(_Name_1, _Index + 1)),
            {_MetricGrp,_Column}
        end).

%% @doc Retrieve process expire-time
-define(env_proc_expiration_time(),
        case application:get_env('savanna_commons', proc_expiration_time) of
            {ok,_EnvProcExpirationTime} ->
                _EnvProcExpirationTime;
            _ ->
                ?SV_EXPIRATION_TIME
        end).

%% @doc Sup restart type
-define(restart_type_of_child(_Exp),
        case _Exp of
            'infinity' ->
                permanent;
            _ ->
                temporary
        end).


%% Records
-record(sv_metric_state, {id :: atom(),
                          sample_mod  :: atom(),
                          type        :: sv_histogram_type() | ?METRIC_COUNTER | ?METRIC_GAUGE,
                          notify_to   :: atom(),
                          window      :: pos_integer(),
                          step        :: pos_integer(),
                          expire_time :: pos_integer() | 'infinity',
                          updated_at  :: pos_integer(),
                          trimed_at   :: pos_integer()
                         }).

-record(sv_schema, {
          name           :: sv_schema(),
          created_at = 0 :: integer()
         }).
-record(sv_schema_1, {
          name             :: sv_schema(),
          name_string = [] :: string(),
          created_at = 0   :: integer()
         }).
-define(SV_SCHEMA, 'sv_schema_1').


-record(sv_column, {
          id              :: any(),
          schema_name     :: sv_schema(),
          name            :: sv_key(),
          type            :: sv_column_type(),
          constraint = [] :: list(),
          created_at = 0  :: integer()
         }).
-record(sv_column_1, {
          id              :: any(),
          schema_name     :: sv_schema(),
          name            :: sv_key(),
          type            :: sv_column_type(),
          constraint = [] :: list(),
          updated_at = 0  :: integer(),
          created_at = 0  :: integer(),
          del = false     :: boolean()
         }).
-define(SV_COLUMN, 'sv_column_1').


-record(sv_metric_group, {
          id          :: any(),
          schema_name :: sv_schema(),
          name        :: sv_metric_grp(),
          window = 0  :: integer(),
          step   = 0  :: integer(),
          callback    :: atom(),
          created_at  :: pos_integer() %% see:'svc_notify_behaviour'
         }).

-record(sv_metric_conf, {
          metric_type       :: sv_metric_type(),
          histogram_type    :: sv_histogram_type(),
          metric_group_name :: sv_metric_grp(),
          name              :: sv_key(),
          window = 0        :: integer(),
          step = 0          :: integer(),
          sample_size = -1  :: integer(),
          alpha = 0.0       :: float(),
          callback          :: atom()
          }).

-record(sv_result, {
          metric_type       :: sv_metric_type(),
          schema_name       :: sv_schema(),
          metric_group_name :: sv_metric_grp(),
          from              :: integer(),
          to                :: integer(),
          window            :: integer(),
          adjusted_step     :: integer(),
          col_name          :: sv_key(),
          result            :: sv_result()
         }).
