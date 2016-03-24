%% -------------------------------------------------------------------
%%
%% riak_core_coverage_fsm: Distribute work to a covering set of VNodes.
%%
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc The coverage fsm is a behavior used to create
%%      a plan to cover a set of VNodes, distribute
%%      a specified command to each VNode in the plan
%%      and then compile the results.
%%
%%      The number of VNodes required for full coverage
%%      is based on the number of partitions, the bucket
%%      n_val, and the number of primary VNodes from the
%%      preference list that are configured to be used by
%%      the module implementing this behavior.
%%
%%      Modules implementing this behavior should return
%%      a 9 member tuple from their init function that looks
%%      like this:
%%
%%         `{Request, VNodeSelector, NVal, PrimaryVNodeCoverage,
%%          NodeCheckService, VNodeMaster, Timeout, State}'
%%
%%      The description of the tuple members is as follows:
%%
%%      <ul>
%%      <li>Request - An opaque data structure that is used by
%%        the VNode to implement the specific coverage request.</li>
%%      <li>VNodeSelector - If using the standard riak_core_coverage_plan
%%          module, 'all' for all VNodes or 'allup' for all reachable.
%%          If using an alternative coverage plan generator, whatever
%%          value is useful to its `create_plan' function; will be passed
%%          as the first argument.</li>
%%      <li>NVal - Indicates the replication factor and is used to
%%        accurately create a minimal covering set of VNodes.</li>
%%      <li>PrimaryVNodeCoverage - The number of primary VNodes
%%      from the preference list to use in creating the coverage
%%      plan. Typically just 1.</li>
%%      <li>NodeCheckService - The service to use to check for available
%%      nodes (e.g. riak_kv).</li>
%%      <li>VNodeMaster - The atom to use to reach the vnode master module.</li>
%%      <li>Timeout - The timeout interval for the coverage request.</li>
%%      <li>PlannerMod - The module which defines `create_plan' and optionally
%%          functions to support a coverage query API</li>
%%      <li>State - The initial state for the module.</li>
%%      </ul>
-module(riak_core_coverage_fsm).

-include("riak_core_vnode.hrl").

-behaviour(gen_fsm).

%% API
-export([behaviour_info/1]).

-export([start_link/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%% Test API
-export([test_link/5]).
-endif.

%% gen_fsm callbacks
-export([init/1,
         initialize/2,
         waiting_results/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include_lib("profiler/include/profiler.hrl").

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [
     {init, 2},
     {process_results, 2},
     {finish, 2}
    ];
behaviour_info(_) ->
    undefined.

-define(DEFAULT_TIMEOUT, 60000*8).

-type req_id() :: non_neg_integer().
-type from() :: {atom(), req_id(), pid()}.
-type index() :: chash:index_as_int().

%-define(PROF_QUERY, 1).

-ifdef(PROF_QUERY).
-compile(export_all).
-endif.

-ifdef(PROF_QUERY).
fakeResp() ->
    [{<<16,0,0,0,3,12,183,128,8,16,0,0,0,2,18,163,217,109,244,59,69,150,199,107,180,219,128,8,18,163,217,109,244,59,69,150,199,107,180,219,128,8,16,0,0,0,3,18,179,88,109,182,155,101,230,98,8,18,185,217,110,86,155,45,206,176,8,10,0,0,0,200>>,<<53,1,0,0,0,27,131,108,0,0,0,1,104,2,109,0,0,0,1,0,104,2,97,1,110,5,0,74,27,77,208,14,106,0,0,0,1,0,0,0,86,2,135,168,109,121,102,97,109,105,108,121,167,102,97,109,105,108,121,49,168,109,121,115,101,114,105,101,115,167,115,101,114,105,101,115,88,164,116,105,109,101,100,165,109,121,105,110,116,1,165,109,121,98,105,110,165,116,101,115,116,49,167,109,121,102,108,111,97,116,203,63,240,0,0,0,0,0,0,166,109,121,98,111,111,108,195,0,0,0,52,0,0,5,177,0,0,145,10,0,3,147,80,22,49,115,55,78,74,98,86,109,89,88,105,69,55,65,86,107,65,111,100,54,52,68,0,0,0,0,4,1,100,100,108,0,0,0,4,0,131,97,1>>}].
-endif.

-record(state, {coverage_vnodes :: [{non_neg_integer(), node()}],
                mod :: atom(),
                mod_state :: tuple(),
                n_val :: pos_integer(),
                node_check_service :: module(),
                %% `vnode_selector' can be any useful value for different
                %% `riak_core' applications that define their own coverage
                %% plan module
                vnode_selector :: vnode_selector() | vnode_coverage() | term(),
                pvc :: all | pos_integer(), % primary vnode coverage
                request :: tuple(),
                req_id :: req_id(),
                required_responses :: pos_integer(),
                response_count=0 :: non_neg_integer(),
                timeout :: timeout(),
                vnode_master :: atom(),
                plan_fun :: function(),
                process_fun :: function(),
                coverage_plan_mod :: module(),
		from :: tuple()
               }).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a riak_core_coverage_fsm.
-spec start_link(module(), from(), [term()]) ->
                        {ok, pid()} | ignore | {error, term()}.
start_link(Mod, From, RequestArgs) ->
    gen_fsm:start_link(?MODULE, [Mod, From, RequestArgs], []).

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

%% Create a coverage FSM for testing.
test_link(Mod, From, RequestArgs, _Options, StateProps) ->
    Timeout = 60000,
    gen_fsm:start_link(?MODULE,
                       {test,
                        [Mod,
                         From,
                         RequestArgs,
                         Timeout],
                        StateProps},
                       []).

-endif.

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @private
init([Mod,
      From={_, ReqId, _},
      RequestArgs]) ->

    profiler:perf_profile({stop, 24}),
    profiler:perf_profile({start, 22, ?FNNAME()}),

    profiler:perf_profile({start, 25, "riak_kv_index_fsm:module_info"}),
    Exports = Mod:module_info(exports),
    profiler:perf_profile({stop, 25}),

    {Request, VNodeSelector, NVal, PrimaryVNodeCoverage,
     NodeCheckService, VNodeMaster, Timeout, PlannerMod, ModState} =
        Mod:init(From, RequestArgs),

    profiler:perf_profile({start, 28, "riak_core_coverage_fsm:maybe_start_timeout_timer"}),
    maybe_start_timeout_timer(Timeout),
    profiler:perf_profile({stop, 28}),

    PlanFun = plan_callback(Mod, Exports),
    ProcessFun = process_results_callback(Mod, Exports),
    StateData = #state{mod=Mod,
                       mod_state=ModState,
                       node_check_service=NodeCheckService,
                       vnode_selector=VNodeSelector,
                       n_val=NVal,
                       pvc = PrimaryVNodeCoverage,
                       request=Request,
                       req_id=ReqId,
                       timeout=infinity,
                       vnode_master=VNodeMaster,
                       plan_fun = PlanFun,
                       process_fun = ProcessFun,
                       coverage_plan_mod = PlannerMod,
		       from = From},
    profiler:perf_profile({stop, 22}),
    {ok, initialize, StateData, 0};
init({test, Args, StateProps}) ->
    profiler:perf_profile({start, 23, ?FNNAME()}),
    %% Call normal init
    {ok, initialize, StateData, 0} = init(Args),

    %% Then tweak the state record with entries provided by StateProps
    Fields = record_info(fields, state),
    FieldPos = lists:zip(Fields, lists:seq(2, length(Fields)+1)),
    F = fun({Field, Value}, State0) ->
                Pos = proplists:get_value(Field, FieldPos),
                setelement(Pos, State0, Value)
        end,
    TestStateData = lists:foldl(F, StateData, StateProps),

    %% Enter into the execute state, skipping any code that relies on the
    %% state of the rest of the system
    profiler:perf_profile({stop, 23}),
    {ok, waiting_results, TestStateData, 0}.

%% @private
maybe_start_timeout_timer(infinity) ->
    ok;
maybe_start_timeout_timer(Bad) when not is_integer(Bad) ->
    maybe_start_timeout_timer(?DEFAULT_TIMEOUT);
maybe_start_timeout_timer(Timeout) ->
    gen_fsm:start_timer(Timeout, {timer_expired, Timeout}),
    ok.

%% @private
find_plan(_Mod, #vnode_coverage{}=Plan, _NVal, _PVC, _ReqId, _Service,
          _Request) ->
    interpret_plan(Plan);
find_plan(Mod, Target, NVal, PVC, ReqId, Service, Request) ->
    Mod:create_plan(Target, NVal, PVC, ReqId, Service, Request).

%% @private
%% Take a `vnode_coverage' record and interpret it as a mini coverage plan
-spec interpret_plan(vnode_coverage()) ->
                            {list({index(), node()}),
                             list({index(), list(index())|tuple()})}.
interpret_plan(#vnode_coverage{vnode_identifier  = TargetHash,
                               partition_filters = [],
                               subpartition = undefined}) ->
    {[{TargetHash, node()}], []};
interpret_plan(#vnode_coverage{vnode_identifier  = TargetHash,
                               partition_filters = HashFilters,
                               subpartition = undefined}) ->
    {[{TargetHash, node()}], [{TargetHash, HashFilters}]};
interpret_plan(#vnode_coverage{vnode_identifier = TargetHash,
                               subpartition     = {Mask, BSL}}) ->
    {[{TargetHash, node()}], [{TargetHash, {Mask, BSL}}]}.

-ifdef(PROF_QUERY).
coverageProf(_Request, _CoverageVNodes, _FilterVNodes, _Sender, _VNodeMaster, _StateData, _Timeout, {_,_,ClientPid}, ReqId, ModState) ->
    ClientPid ! {ReqId, {results, fakeResp()}},
    ClientPid ! {ReqId, done},
    {stop, normal, ModState}.
-else.
coverageProf(Request, CoverageVNodes, FilterVNodes, Sender, VNodeMaster, StateData, Timeout, _From, _ReqId, _ModState) ->
    riak_core_vnode_master:coverage(Request,
				    CoverageVNodes,
				    FilterVNodes,
				    Sender,
				    VNodeMaster),
    {next_state, waiting_results, StateData, Timeout}.
-endif.

%% @private
initialize(timeout, StateData0=#state{mod=Mod,
                                      mod_state=ModState,
                                      n_val=NVal,
                                      node_check_service=NodeCheckService,
                                      vnode_selector=VNodeSelector,
                                      pvc=PVC,
                                      request=Request,
                                      req_id=ReqId,
                                      timeout=Timeout,
                                      vnode_master=VNodeMaster,
                                      plan_fun = PlanFun,
                                      coverage_plan_mod = PlanMod,
				      from = From}) ->
    profiler:perf_profile({start, 10, ?FNNAME()}),
    CoveragePlan = find_plan(PlanMod,
                             VNodeSelector,
                             NVal,
                             PVC,
                             ReqId,
                             NodeCheckService,
                             Request),
    Ret =
    case CoveragePlan of
        {error, Reason} ->
            Mod:finish({error, Reason}, ModState);
        {CoverageVNodes, FilterVNodes} ->
            {ok, UpModState} = PlanFun(CoverageVNodes, ModState),
            Sender = {fsm, ReqId, self()},
            StateData = StateData0#state{coverage_vnodes=CoverageVNodes, mod_state=UpModState},                     
	    coverageProf(Request, CoverageVNodes, FilterVNodes, Sender, VNodeMaster, StateData, Timeout, From, ReqId, ModState)
    end,
    profiler:perf_profile({stop, 10}),
    Ret.

%% @private
waiting_results({{ReqId, VNode}, Results},
                StateData=#state{coverage_vnodes=CoverageVNodes,
                                 mod=Mod,
                                 mod_state=ModState,
                                 req_id=ReqId,
                                 timeout=Timeout,
                                 process_fun = ProcessFun}) ->
    case ProcessFun(VNode, Results, ModState) of
        {ok, UpdModState} ->
            UpdStateData = StateData#state{mod_state=UpdModState},
            {next_state, waiting_results, UpdStateData, Timeout};
        {done, UpdModState} ->
            UpdatedVNodes = lists:delete(VNode, CoverageVNodes),
            case UpdatedVNodes of
                [] ->
                    Mod:finish(clean, UpdModState);
                _ ->
                    UpdStateData =
                        StateData#state{coverage_vnodes=UpdatedVNodes,
                                        mod_state=UpdModState},
                    {next_state, waiting_results, UpdStateData, Timeout}
            end;
        Error ->
            Mod:finish(Error, ModState)
    end;
waiting_results({timeout, _, _}, #state{mod=Mod, mod_state=ModState}) ->
    Mod:finish({error, timeout}, ModState);
waiting_results(timeout, #state{mod=Mod, mod_state=ModState}) ->
    Mod:finish({error, timeout}, ModState).

%% @private
handle_event(_Event, _StateName, State) ->
    {stop, badmsg, State}.

%% @private
handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.

%% @private
handle_info({'EXIT', _Pid, Reason}, _StateName, #state{mod=Mod,
                                                       mod_state=ModState}) ->
    Mod:finish({error, {node_failure, Reason}}, ModState);
handle_info({_ReqId, {ok, _Pid}},
            StateName,
            StateData=#state{timeout=Timeout}) ->
    %% Received a message from a coverage node that
    %% did not start up within the timeout. Just ignore
    %% the message and move on.
    {next_state, StateName, StateData, Timeout};
handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

%% @private
terminate(Reason, _StateName, _State) ->
    Reason.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

plan_callback(Mod, Exports) ->
    profiler:perf_profile({start, 26, ?FNNAME()}),
    Ret =
    case exports(plan, Exports) of
        true ->
            fun(CoverageVNodes, ModState) ->
                    Mod:plan(CoverageVNodes, ModState) end;
        _ -> fun(_, ModState) ->
                     {ok, ModState} end
    end,
    profiler:perf_profile({stop, 26}),
    Ret.

process_results_callback(Mod, Exports) ->
    profiler:perf_profile({start, 27, ?FNNAME()}),
    Ret = 
    case exports_arity(process_results, 3, Exports) of
        true ->
            fun(VNode, Results, ModState) ->
                    Mod:process_results(VNode, Results, ModState) end;
        false ->
            fun(_VNode, Results, ModState) ->
                    Mod:process_results(Results, ModState) end
    end,
    profiler:perf_profile({stop, 27}),
    Ret.

exports(Function, Exports) ->
    proplists:is_defined(Function, Exports).

exports_arity(Function, Arity, Exports) ->
    lists:member(Arity, proplists:get_all_values(Function, Exports)).
