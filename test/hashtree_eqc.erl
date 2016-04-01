%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012-2015 Basho Technologies, Inc.  All Rights Reserved.
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
%%
%% Hashtree EQC test.  
%%
%% Generates a pair of logically identical AAE trees populated with data
%% and some phantom trees in the same leveldb database to exercise all
%% of the cases in iterate.
%%
%% Then runs commands to insert, delete, snapshot, update tree
%% and compare.
%%
%% The expected values are stored in two ETS tables t1 and t2,
%% with the most recently snapshotted values copied to tables s1 and s2.
%% (the initial common seed data is not included in the ETS tables).
%%
%% The hashtree's themselves are stored in the process dictionary under
%% key t1 and t2.  This helps with shrinking as it reduces dependencies
%% between states (or at least that's why I remember doing it).
%%
%% Model state stores where each tree is through the snapshot/update cycle.
%% The command frequencies are deliberately manipulated to make it more
%% likely that compares will take place once both trees are updated.
%%

-module(hashtree_eqc).
-compile([export_all]).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-define(POWERS, [8, 16, 32, 64, 128, 256, 512, 1024]).

-include_lib("eunit/include/eunit.hrl").

hashtree_test_() ->
    {setup,
     fun() ->
             application:set_env(lager, handlers, [{lager_console_backend, info}]),
             application:ensure_started(syntax_tools),
             application:ensure_started(compiler),
             application:ensure_started(goldrush),
             application:ensure_started(lager)
     end,
     fun(_) ->
             application:stop(lager),
             application:stop(goldrush),
             application:stop(compiler),
             application:stop(syntax_tools),
             application:unload(lager)
     end,
     [{timeout, 60,
       fun() ->
              
              lager:info("Any warnings should be investigated.  No lager output expected.\n"),
              ?assert(eqc:quickcheck(?QC_OUT(eqc:testing_time(29,
                                                              hashtree_eqc:prop_correct()))))
      end
      }]}.

-record(state,
    {
      started = false,    % Boolean to prevent commands running before initialization step.
      params = undefined, % {Segments, Width, MemLevels}
      snap1 = undefined,  % undefined, created, updated
      snap2 = undefined,  % undefined, created, updated
      num_updates = 0     % number of insert/delete operations
    }).

integer_to_binary(Int) ->
    list_to_binary(integer_to_list(Int)).

-ifndef(old_hash).
sha(Bin) ->
    crypto:hash(sha, Bin).
-else.
sha(Bin) ->
    crypto:sha(Bin).
-endif.

key() ->
    ?LET(Key, int(), ?MODULE:integer_to_binary(Key)).

object() ->
    {key(), sha(term_to_binary(make_ref()))}.

objects() ->
    non_empty(list(object())).

params() -> % {Segments, Width, MemLevels}
    %% Generate in terms of number of levels, from that work out the segments
    ?LET({Levels, Width, MemLevels},{choose(1,3), oneof(?POWERS), choose(0, 4)},
         {trunc(math:pow(Width, Levels)), Width, MemLevels}).
    %% Default for new()
    %% {1024*1024, 1024, 0}.
    %% TODO: Re-enable mem levels [{oneof(?POWERS), oneof(?POWERS), choose(0, 4)}



mark() ->
    frequency([{1, mark_empty}, {5, mark_open}]).

seed_data() ->
    list(oneof([object(),
		{key(), delete}])).

ids() ->
    non_empty(list({?LET(X, nat(), 1+X), ?LET(X,nat(), min(X,255))})).

command(_S = #state{started = false}) ->
    {call, ?MODULE, start, [params(), ids(), mark(), mark(), seed_data()]};
command(_S = #state{started = true, params = {_Segments, _Width, MemLevels},
                    snap1 = Snap1, snap2 = Snap2}) ->
    %% Increase snap frequency once update snapshot has begun
    SS = Snap1 /= undefined orelse Snap2 /= undefined,
    SF = case SS of true -> 10; _ -> 1 end,
    frequency(
      %% Update snapshots/trees. If memory is enabled must test with update_tree
      %% If not, can use the method used by kv/yz_index_hashtree and separate
      %% the two steps, dumping the result from update_perform.
	[{ SF, {call, ?MODULE, update_tree,  [t1, s1]}} || Snap1 == undefined] ++
	[{ SF, {call, ?MODULE, update_snapshot,  [t1, s1]}} || Snap1 == undefined, MemLevels == 0] ++
	[{ SF, {call, ?MODULE, update_perform,   [t1]}} || Snap1 == created, MemLevels == 0] ++
	[{ SF, {call, ?MODULE, set_next_rebuild, [t1]}} || Snap1 == updated] ++
	[{ SF, {call, ?MODULE, update_tree,  [t2, s2]}} || Snap1 == undefined] ++
	[{ SF, {call, ?MODULE, update_snapshot,  [t2, s2]}} || Snap2 == undefined, MemLevels == 0] ++
	[{ SF, {call, ?MODULE, update_perform,   [t2]}} || Snap2 == created, MemLevels == 0] ++
	[{ SF, {call, ?MODULE, set_next_rebuild, [t2]}} || Snap2 == updated] ++

      %% Can only run compares when both snapshots are updated.  Boost the frequency
      %% when both are snapshotted (note this is guarded by both snapshot being updatable)
        [{10*SF, {call, ?MODULE, local_compare, []}} || Snap1 == updated, Snap2 == updated] ++

      %% Modify the data in the two tables
        [{11-SF, {call, ?MODULE, write, [t1, objects()]}},
	 {11-SF, {call, ?MODULE, write, [t2, objects()]}},
         {11-SF, {call, ?MODULE, write_both, [objects()]}},
         {11-SF, {call, ?MODULE, delete, [t1, key()]}},
         {11-SF, {call, ?MODULE, delete, [t2, key()]}},
         {11-SF, {call, ?MODULE, delete_both, [key()]}},

      %% Mess around with reopening, crashing and rehashing.
         {  1, {call, ?MODULE, reopen_tree, [t1]}},
         {  1, {call, ?MODULE, reopen_tree, [t2]}}
         %% {  1, {call, ?MODULE, unsafe_close, [t1]}},
         %% {  1, {call, ?MODULE, unsafe_close, [t2]}},
         %% {  1, {call, ?MODULE, rehash_tree, [t1]}},
	 %% {  1, {call, ?MODULE, rehash_tree, [t2]}}
	]
    ).


start(Params, [Id | ExtraIds], T1Mark, T2Mark, SeedData) ->
    {Segments, Width, MemLevels} = Params,
    %% Return now so we can store symbolic value in procdict in next_state call
    T1 = hashtree:new(Id, [{segments, Segments},
                           {width, Width},
                           {mem_levels, MemLevels}]),

    T1A = case T1Mark of
        mark_empty -> hashtree:mark_open_empty(Id, T1);
        _ -> hashtree:mark_open_and_check(Id, T1)
    end,

    T1B = load_seed(SeedData, T1A),
    add_extra_hashtrees(ExtraIds, T1B),

    put(t1, T1B),

    T2 = hashtree:new(Id, [{segments, Segments},
                               {width, Width},
                               {mem_levels, MemLevels}]),

    T2A = case T2Mark of
        mark_empty -> hashtree:mark_open_empty(Id, T2);
        _ -> hashtree:mark_open_and_check(Id, T2)
    end,

    T2B = load_seed(SeedData, T2A),
    add_extra_hashtrees(ExtraIds, T2B),

    put(t2, T2B),

    %% Make sure ETS is pristine
    catch ets:delete(t1),
    catch ets:delete(t2),
    catch ets:delete(s1),
    catch ets:delete(s2),
    ets_new(t1),
    ets_new(t2),
    ok.

%% Load up seed data to pre-populate and make more interesting.
%% Seed data is not tracked in either the t1/t2 ETS tables
%% as it is common to both sides so there should only be differences
%% relative to the base to make more tractable. To make sure
%% updates don't cause a difference, the keyspace is modified to add
%% a $ symbol on the end.
load_seed(SeedData, T) ->
    lists:foldl(fun({K, delete}, Tacc) ->
			hashtree:delete(<<K/binary, "$">>, Tacc);
		   ({K, V}, Tacc) ->
			hashtree:insert(<<K/binary, "$">>, V, Tacc)
		end, T, SeedData).
			   

%% Add some extra tree ids and update the metadata to give
%% the iterator code a workout on non-matching ids.
add_extra_hashtrees(ExtraIds, T) ->
    lists:foldl(fun(ExtraId, Tacc) ->
			Tacc2 = hashtree:new(ExtraId, Tacc),
			Tacc3 = hashtree:mark_open_empty(ExtraId, Tacc2),
			Tacc4 = hashtree:insert(<<"k">>, <<"v">>, Tacc3),
			hashtree:flush_buffer(Tacc4)
		end, T, ExtraIds).

%% Snapshot and update the tree in a single step.  This works with memory levels
%% enabled.
update_tree(T, S) ->
    %% Snapshot the hashtree and store both states
    HT = hashtree:update_tree(get(T)),
    put(T, HT),
    %% Copy the current ets table to the snapshot table.
    copy_tree(T, S),
    ok.

%% Create a new snapshot and update.  This does not work with
%% memory levels enabled as update_perform uses the snapshot
%% state which is dumped.
update_snapshot(T, S) ->
    %% Snapshot the hashtree and store both states
    {SS, HT} = hashtree:update_snapshot(get(T)),

    %% Mark as a full rebuild until the update perfom step happens.
    HT2 = hashtree:set_next_rebuild(HT, full),

    put(T, HT2),
    put({snap, T}, SS),
    %% Copy the current ets table to the snapshot table.
    copy_tree(T, S),
    ok.

%% Copy the model data from the live to snapshot table for update_tree/update_snapshot 
copy_tree(T, S) ->
    catch ets:delete(S),
    ets_new(S),
    ets:insert(S, ets:tab2list(T)),
    ok.


%% Update the tree and dump the state.
update_perform(T) ->
    _ = hashtree:update_perform(get({snap, T})),
    erase({snap, T}),
    ok.

set_next_rebuild(T) ->
    _ = hashtree:set_next_rebuild(get(T), incremental).

write(T, Objects) ->
    lists:foreach(fun({Key, Hash}) ->
                          put(T, hashtree:insert(Key, Hash, get(T))),
                          ets:insert(T, {Key, Hash})
                  end, Objects),
    ok.

write_both(Objects) ->
    write(t1, Objects),
    write(t2, Objects),
    ok.

delete(T, Key) ->
    put(T, hashtree:delete(Key, get(T))),
    ets:delete(T, Key),
    ok.

delete_both(Key) ->
    delete(t1, Key),
    delete(t2, Key),
    ok.

rehash_tree(T) ->
    put(T, hashtree:rehash_tree(get(T))),
    ok.

reopen_tree(T) ->
    HT = hashtree:flush_buffer(get(T)),
    {Segments, Width, MemLevels} = {hashtree:segments(HT), hashtree:width(HT),
                                    hashtree:mem_levels(HT)},
    Path = hashtree:path(HT),

    UpdatedHT = hashtree:update_tree(HT),
    CleanClosedHT = hashtree:mark_clean_close({0,0}, UpdatedHT),
    hashtree:close(CleanClosedHT),

    T1 = hashtree:new({0,0}, [{segments, Segments},
                              {width, Width},
                              {mem_levels, MemLevels},
                              {segment_path, Path}]),

    put(T, hashtree:mark_open_and_check({0,0}, T1)),
    ok.

unsafe_close(T) ->
    HT = get(T),
    {Segments, Width, MemLevels} = {hashtree:segments(HT), hashtree:width(HT),
                                    hashtree:mem_levels(HT)},
    Path = hashtree:path(HT),
    %% Although this is an unsafe close, it's unsafe in metadata/building
    %% buckets.  Rather than model the queue behavior, flush those and just
    %% check the buckets are correctly recomputed next compare.
    hashtree:flush_buffer(HT),
    hashtree:fake_close(HT),

    T0 = hashtree:new({0,0}, [{segments, Segments},
                              {width, Width},
                              {mem_levels, MemLevels},
                              {segment_path, Path}]),

    put(T, hashtree:mark_open_and_check({0,0}, T0)),

    ok.

local_compare() ->
    put(t1, hashtree:flush_buffer(get(t1))),
    put(t2, hashtree:flush_buffer(get(t2))),
    hashtree:local_compare(get(t1), get(t2)).

precondition(#state{started = false}, {call, _, F, _A}) ->
    F == start;
precondition(#state{params = {_, _, MemLevels}, snap1 = Snap1}, {call, _, update_tree, [t1, _]}) ->
    Snap1 == undefined andalso MemLevels == 0;
precondition(#state{params = {_, _, MemLevels}, snap2 = Snap2}, {call, _, update_tree, [t2, _]}) ->
    Snap2 == undefined andalso MemLevels == 0;
precondition(#state{snap1 = Snap1}, {call, _, update_snapshot, [t1, _]}) ->
    Snap1 == undefined;
precondition(#state{snap1 = Snap1}, {call, _, update_perform, [t1]}) ->
    Snap1 == created;
precondition(#state{snap1 = Snap1}, {call, _, set_next_rebuild, [t1]}) ->
    Snap1 == updated;
precondition(#state{snap2 = Snap2}, {call, _, update_snapshot, [t2, _]}) ->
    Snap2 == undefined;
precondition(#state{snap2 = Snap2}, {call, _, update_perform, [t2]}) ->
    Snap2 == created;
precondition(#state{snap2 = Snap2}, {call, _, set_next_rebuild, [t2]}) ->
    Snap2 == updated;
precondition(#state{snap1 = Snap1, snap2 = Snap2}, {call, _, local_compare, []}) ->
    Snap1 == updated andalso Snap2 == updated;
precondition(_S, _C) ->
    true.


postcondition(_S,{call,_,start, [_Params, _ExtraIds, T1Mark, T2Mark, _SeedData]},_R) ->
    NextRebuildT1 = hashtree:next_rebuild(get(t1)),
    NextRebuildT2 = hashtree:next_rebuild(get(t2)),
    case T1Mark of
        mark_empty -> eq(NextRebuildT1, incremental);
        _ -> eq(NextRebuildT1, full)
    end,
    case T2Mark of
        mark_empty -> eq(NextRebuildT2, incremental);
        _ -> eq(NextRebuildT2, full)
    end;
postcondition(_S,{call, _, local_compare, _},  Result) ->
    Expect = expect_compare(),
    eq(Expect, lists:sort(Result));	
postcondition(_S,{call,_,_,_},_R) ->
    true.

next_state(S,_R,{call, _, start, [Params,_ExtraIds,_,_,SeedData]}) ->
    S#state{started = true, params = Params, num_updates = length(SeedData)};
next_state(S,_V,{call, _, update_tree, [t1, _]}) ->
    S#state{snap1 = updated};
next_state(S,_V,{call, _, update_tree, [t2, _]}) ->
    S#state{snap2 = updated};
next_state(S,_V,{call, _, update_snapshot, [t1, _]}) ->
    S#state{snap1 = created};
next_state(S,_V,{call, _, update_snapshot, [t2, _]}) ->
    S#state{snap2 = created};
next_state(S,_V,{call, _, update_perform, [t1]}) ->
    S#state{snap1 = updated};
next_state(S,_V,{call, _, update_perform, [t2]}) ->
    S#state{snap2 = updated};
next_state(S,_V,{call, _, set_next_rebuild, [t1]}) ->
    S#state{snap1 = undefined};
next_state(S,_V,{call, _, set_next_rebuild, [t2]}) ->
    S#state{snap2 = undefined};
next_state(S,_V,{call, _, write, [_T, Objs]}) ->
    S#state{num_updates = S#state.num_updates + length(Objs)};
next_state(S,_R,{call, _, write_both, [Objs]}) ->
    S#state{num_updates = S#state.num_updates + 2*length(Objs)};
next_state(S,_V,{call, _, delete, _}) ->
    S#state{num_updates = S#state.num_updates + 1};
next_state(S,_R,{call, _, delete_both, _}) ->
    S#state{num_updates = S#state.num_updates + 2};
next_state(S,_R,{call, _, reopen_tree, [t1]}) ->
    S#state{snap1 = undefined};
next_state(S,_R,{call, _, reopen_tree, [t2]}) ->
    S#state{snap2 = undefined};
next_state(S,_R,{call, _, unsafe_close, [t1]}) ->
    S#state{snap1 = undefined};
next_state(S,_R,{call, _, unsafe_close, [t2]}) ->
    S#state{snap2 = undefined};
next_state(S,_R,{call, _, rehash_tree, [t1]}) ->
    S#state{snap1 = undefined};
next_state(S,_R,{call, _, rehash_tree, [t2]}) ->
    S#state{snap2 = undefined};
next_state(S,_R,{call, _, local_compare, []}) ->
    S.

prop_correct() ->
    ?FORALL(Cmds,commands(?MODULE, #state{}),
            aggregate(command_names(Cmds),
                begin
                    %%io:format(user, "Starting in ~p\n", [self()]),
                    put(t1, undefined),
                    put(t2, undefined),
                    catch ets:delete(t1),
                    catch ets:delete(t2),
                    {_H,S,Res} = HSR = run_commands(?MODULE,Cmds),
		    {Segments, Width, MemLevels} = 
                        case S#state.params of
                            undefined ->
                                %% Possible if Cmds just init
                                %% set segments to 1 to avoid div by zero
                                {1, undefined, undefined};
                            Params ->
                                Params
                        end,
                    NumUpdates = S#state.num_updates,
                    pretty_commands(?MODULE, Cmds, HSR,
                                    ?WHENFAIL(
                                       begin
					   {Segments, Width, MemLevels} = S#state.params,
					   eqc:format("Segments ~p\nWidth ~p\nMemLevels ~p\n",
						      [Segments, Width, MemLevels]),
					   eqc:format("=== t1 ===\n~p\n\n", [ets:tab2list(t1)]),
					   eqc:format("=== s1 ===\n~p\n\n", [safe_tab2list(s1)]),
					   eqc:format("=== t2 ===\n~p\n\n", [ets:tab2list(t2)]),
					   eqc:format("=== s2 ===\n~p\n\n", [safe_tab2list(s2)]),
					   eqc:format("=== ht1 ===\n~w\n~p\n\n", [get(t1), catch dump(get(t1))]),
					   eqc:format("=== ht2 ===\n~w\n~p\n\n", [get(t2), catch dump(get(t2))]),
					   
                                           catch hashtree:destroy(hashtree:close(get(t1))),
                                           catch hashtree:destroy(hashtree:close(get(t2)))
                                       end,
                                       measure(num_updates, NumUpdates,
                                       measure(segment_fill_ratio, NumUpdates / (2 * Segments), % Est of avg fill rate per segment
				       collect(with_title(mem_levels), MemLevels,
                                       collect(with_title(segments), Segments,
                                       collect(with_title(width), Width,
                                               equals(ok, Res))))))))
		end)).

safe_tab2list(Id) ->
    try
        ets:tab2list(Id)
    catch
        _:_ ->
            undefined
    end.

dump(Tree) ->
    Fun = fun(Entries) ->
                  Entries
          end,
    {SnapTree, _Tree2} = hashtree:update_snapshot(Tree),
    hashtree:multi_select_segment(SnapTree, ['*','*'], Fun).

%% keymerge(S) ->
%%     keymerge(S#state.only1, S#state.only2).

%% keymerge(SuccList, L) ->
%%     lists:ukeymerge(1, lists:ukeysort(1, SuccList),
%%                     lists:ukeysort(1, L)).


expect_compare() ->
    Snap1 = orddict:from_list(ets:tab2list(s1)),
    Snap2 = orddict:from_list(ets:tab2list(s2)),
    SnapDeltas = riak_ensemble_util:orddict_delta(Snap1, Snap2),
    %% DeltaKeys = [K || {K, {V1, V2}} <- SnapDeltas, V1 /= '$none', V2 /= '$none'],
    %% Filter = fun(K, _H) ->
    %% 		     lists:member(K, DeltaKeys)
    %% 	     end,
    %% F1 = orddict:filter(Filter, orddict:from_list(ets:tab2list(t1))),
    %% F2 = orddict:filter(Filter, orddict:from_list(ets:tab2list(t2))),
    %% Deltas = riak_ensemble_util:orddict_delta(F1, F2),

    %% Missing keys are detected from the snapshots, but different keys are double-checked
    %% against the current hash value to replicate a segment.

    %% Have to task into account flushing the buffer... sigh, so messy. temporarily fix with flush before local compare.
    %% Wonder if the AAE lookup functions should come from the snapshot.
    lists:sort(
      [{missing, K} || {K, {'$none', _}} <- SnapDeltas] ++
	  [{remote_missing, K} || {K, {_, '$none'}} <- SnapDeltas] ++
          [{different, K} || {K, {V1, V2}} <- SnapDeltas, V1 /= '$none', V2 /= '$none']). %% UNDO SnapDeltas this line
	
ets_new(T) ->
    ets:new(T, [named_table, public, set]).


-endif.
-endif.
