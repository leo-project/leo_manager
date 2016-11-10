%%======================================================================
%%
%% Leo Backend-DB
%%
%% Copyright (c) 2012-2014
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
%% Leo Object Storage
%%
%%======================================================================

-define(ETS_TABLE_NAME, 'leo_backend_db_pd').
-define(APP_NAME, 'leo_backend_db').

-type(type_of_methods() :: put | get | delete | fetch).
-type(backend_db() :: bitcask | leveldb | ets).


-define(get_new_count(_DBMod,_Handler,_Method,_Key,_Cnt,_IsStrictCheck),
        case _DBMod of
            'ets' ->
                _Cnt;
            _ when _IsStrictCheck == true ->
                case erlang:apply(_DBMod, get, [_Handler,_Key]) of
                    not_found ->
                        case _Method of
                            'put' ->
                                _Cnt + 1;
                            _ ->
                                _Cnt
                        end;
                    {ok,_} ->
                        case _Method of
                            'put' ->
                                _Cnt;
                            _ ->
                                _Cnt - 1
                        end;
                    _ ->
                        _Cnt
                end;
            _ ->
                _Cnt + 1
        end).
