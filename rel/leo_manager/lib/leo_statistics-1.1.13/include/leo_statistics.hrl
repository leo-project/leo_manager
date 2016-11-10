%%======================================================================
%%
%% Leo Statistics
%%
%% Copyright (c) 2012-2016 Rakuten, Inc.
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
%% Leo Statistics
%% @doc
%% @end
%%======================================================================
-define(STAT_INTERVAL_1S,   1). %% for test
-define(STAT_INTERVAL_3S,   3). %% for test
-define(STAT_INTERVAL_1M,  60).
-define(STAT_INTERVAL_5M, 300).

-ifdef(TEST).
-define(SNMP_SYNC_INTERVAL_5S,   5). %%  5ms
-define(SNMP_SYNC_INTERVAL_10S, 10). %% 10ms
-define(SNMP_SYNC_INTERVAL_20S, 20). %% 20ms
-define(SNMP_SYNC_INTERVAL_30S, 30). %% 30ms
-define(SNMP_SYNC_INTERVAL_60S, 60). %% 60ms
-define(STATISTICS_SYNC_INTERVAL, 1000). %% 1s
-define(DEF_SMPLING_TIMEOUT,       500). %% 500ms
-else.
-define(SNMP_SYNC_INTERVAL_5S,  timer:seconds(5)).    %%  5sec
-define(SNMP_SYNC_INTERVAL_10S, timer:seconds(10)).   %% 10sec
-define(SNMP_SYNC_INTERVAL_20S, timer:seconds(20)).   %% 20sec
-define(SNMP_SYNC_INTERVAL_30S, timer:seconds(30)).   %% 30sec
-define(SNMP_SYNC_INTERVAL_60S, timer:seconds(60)).   %% 60sec
-define(STATISTICS_SYNC_INTERVAL, timer:seconds(10)). %% 10sec
-define(DEF_SMPLING_TIMEOUT,      timer:seconds(10)). %% 10sec
-endif.

-define(SNMP_NODE_NAME, 'node-name').

%% request metrics
-define(METRIC_GRP_REQ_1MIN, << "access_stats_1m" >>).
-define(METRIC_GRP_REQ_5MIN, << "access_stats_5m" >>).

-define(STAT_COUNT_GET,  << "req_count_get" >>).
-define(STAT_COUNT_PUT,  << "req_count_put" >>).
-define(STAT_COUNT_DEL,  << "req_count_del" >>).

-define(STAT_HISTO_GET,  << "req_histo_get" >>).
-define(STAT_HISTO_PUT,  << "req_histo_put" >>).
-define(STAT_HISTO_DEL,  << "req_histo_del" >>).

-define(STAT_SIZE_GET,  << "req_size_get" >>).
-define(STAT_SIZE_PUT,  << "req_size_put" >>).
-define(STAT_SIZE_DEL,  << "req_size_del" >>).

-define(SNMP_COUNT_WRITES_1M,  'req-writes-1m').
-define(SNMP_COUNT_READS_1M,   'req-reads-1m').
-define(SNMP_COUNT_DELETES_1M, 'req-deletes-1m').

-define(SNMP_SIZE_WRITES_1M,  'size-writes-1m').
-define(SNMP_SIZE_READS_1M,   'size-reads-1m').
-define(SNMP_SIZE_DELETES_1M, 'size-deletes-1m').

%% -define(SNMP_MAX_SIZE_WRITES_1M,   'max-size-writes-1m').
%% -define(SNMP_MAX_SIZE_READS_1M,    'max-size-reads-1m').
%% -define(SNMP_MAX_SIZE_DELETES_1M,  'max-size-deletes-1m').
%% -define(SNMP_MIN_SIZE_WRITES_1M,   'min-size-writes-1m').
%% -define(SNMP_MIN_SIZE_READS_1M,    'min-size-reads-1m').
%% -define(SNMP_MIN_SIZE_DELETES_1M,  'min-size-deletes-1m').
%% -define(SNMP_MEAN_SIZE_WRITES_1M,  'avg-size-writes-1m').
%% -define(SNMP_MEAN_SIZE_READS_1M,   'avg-size-reads-1m').
%% -define(SNMP_MEAN_SIZE_DELETES_1M, 'avg-size-deletes-1m').

-define(SNMP_COUNT_WRITES_5M,  'req-writes-5m').
-define(SNMP_COUNT_READS_5M,   'req-reads-5m').
-define(SNMP_COUNT_DELETES_5M, 'req-deletes-5m').

-define(SNMP_SIZE_WRITES_5M,  'size-writes-5m').
-define(SNMP_SIZE_READS_5M,   'size-reads-5m').
-define(SNMP_SIZE_DELETES_5M, 'size-deletes-5m').

%% -define(SNMP_MAX_SIZE_WRITES_5M,   'max-size-writes-5m').
%% -define(SNMP_MAX_SIZE_READS_5M,    'max-size-reads-5m').
%% -define(SNMP_MAX_SIZE_DELETES_5M,  'max-size-deletes-5m').
%% -define(SNMP_MIN_SIZE_WRITES_5M,   'min-size-writes-5m').
%% -define(SNMP_MIN_SIZE_READS_5M,    'min-size-reads-5m').
%% -define(SNMP_MIN_SIZE_DELETES_5M,  'min-size-deletes-5m').
%% -define(SNMP_MEAN_SIZE_WRITES_5M,  'avg-size-writes-5m').
%% -define(SNMP_MEAN_SIZE_READS_5M,   'avg-size-reads-5m').
%% -define(SNMP_MEAN_SIZE_DELETES_5M, 'avg-size-deletes-5m').


%% vm-stat metrics
-define(METRIC_GRP_VM_1MIN, << "vm_stats_1m" >>).
-define(METRIC_GRP_VM_5MIN, << "vm_stats_5m" >>).

-define(STAT_VM_TOTAL_MEM,  << "vm-total-mem" >>).
-define(STAT_VM_PROCS_MEM,  << "vm-procs-mem" >>).
-define(STAT_VM_SYSTEM_MEM, << "vm-system-mem" >>).
-define(STAT_VM_ETS_MEM,    << "vm-ets-mem" >>).
-define(STAT_VM_PROC_COUNT, << "vm-proc-count" >>).
-define(STAT_VM_USED_PER_ALLOC_MEM, <<"vm-used-per-allocated-mem">>).
-define(STAT_VM_ALLOC_MEM,          <<"vm-allocated-mem">>).

-define(SNMP_VM_TOTAL_MEM_1M,  'vm-total-mem-1m').
-define(SNMP_VM_PROCS_MEM_1M,  'vm-procs-mem-1m').
-define(SNMP_VM_SYSTEM_MEM_1M, 'vm-system-mem-1m').
-define(SNMP_VM_ETS_MEM_1M,    'vm-ets-mem-1m').
-define(SNMP_VM_PROC_COUNT_1M, 'vm-proc-count-1m').
-define(SNMP_VM_USED_PER_ALLOC_MEM_1M, 'vm-used-per-allocated-mem-1m').
-define(SNMP_VM_ALLOC_MEM_1M,  'vm-allocated-mem-1m').

-define(SNMP_VM_TOTAL_MEM_5M,  'vm-total-mem-5m').
-define(SNMP_VM_PROCS_MEM_5M,  'vm-procs-mem-5m').
-define(SNMP_VM_SYSTEM_MEM_5M, 'vm-system-mem-5m').
-define(SNMP_VM_ETS_MEM_5M,    'vm-ets-mem-5m').
-define(SNMP_VM_PROC_COUNT_5M, 'vm-proc-count-5m').
-define(SNMP_VM_USED_PER_ALLOC_MEM_5M, 'vm-used-per-allocated-mem-5m').
-define(SNMP_VM_ALLOC_MEM_5M,  'vm-allocated-mem-5m').


-define(STAT_ELEMENT_MEAN,   'arithmetic_mean').
-define(STAT_ELEMENT_MEDIAN, 'median').
-define(STAT_ELEMENT_MAX,    'max').
-define(STAT_ELEMENT_MIN,    'min').

-undef(MAX_RETRY_TIMES).
-define(MAX_RETRY_TIMES, 3).

%% @doc SNMPA Value
-record(snmpa_value, {oid = []  :: list(),
                      type = [] :: string(),
                      value     :: any()
                     }).
