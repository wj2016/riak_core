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
    {timeout, 60,
        fun() ->
                ?assert(eqc:quickcheck(?QC_OUT(eqc:testing_time(29,
                            hashtree_eqc:prop_correct()))))
        end
    }.

-record(state,
    {
      started = false,
      params = undefined, % {Segments, Width, MemLevels}
      snap1 = undefined,  % undefined, created, updated
      snap2 = undefined   % undefined, created, updated
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

mark() ->
    frequency([{1, mark_empty}, {5, mark_open}]).

params() -> % {Segments, Width, MemLevels}
    %% TODO: Re-enable mem levels [{oneof(?POWERS), oneof(?POWERS), choose(0, 4)}
    %% TODO: {oneof(?POWERS), oneof(?POWERS), 0}.
    {1024*1024, 1024, 0}.

command(_S = #state{started = false}) ->
    {call, ?MODULE, start, [params(), mark(), mark()]};
command(_S = #state{started = true, snap1 = Snap1, snap2 = Snap2}) ->
    %% Increase snap frequency once update snapshot has begun
    SS = Snap1 /= undefined orelse Snap2 /= undefined,
    SF = case SS of true -> 10; _ -> 1 end,
    frequency(
	[{ SF, {call, ?MODULE, update_snapshot,  [t1, s1]}} || Snap1 == undefined] ++
	[{ SF, {call, ?MODULE, update_perform,   [t1]}} || Snap1 == created] ++
	[{ SF, {call, ?MODULE, set_next_rebuild, [t1]}} || Snap1 == updated] ++
	[{ SF, {call, ?MODULE, update_snapshot,  [t2, s2]}} || Snap2 == undefined] ++
	[{ SF, {call, ?MODULE, update_perform,   [t2]}} || Snap2 == created] ++
	[{ SF, {call, ?MODULE, set_next_rebuild, [t2]}} || Snap2 == updated] ++
        [{10*SF, {call, ?MODULE, local_compare, []}} || Snap1 == updated, Snap2 == updated] ++
        [{11-SF, {call, ?MODULE, write, [t1, objects()]}},
	 {11-SF, {call, ?MODULE, write, [t2, objects()]}},
         {11-SF, {call, ?MODULE, write_both, [objects()]}},
         {11-SF, {call, ?MODULE, delete, [t1, key()]}},
         {11-SF, {call, ?MODULE, delete, [t2, key()]}},
         {11-SF, {call, ?MODULE, delete_both, [key()]}},
         {  1, {call, ?MODULE, reopen_tree, [t1]}},
         {  1, {call, ?MODULE, reopen_tree, [t2]}},
         {  1, {call, ?MODULE, unsafe_close, [t1]}},
         {  1, {call, ?MODULE, unsafe_close, [t2]}},
         {  1, {call, ?MODULE, rehash_tree, [t1]}},
	 {  1, {call, ?MODULE, rehash_tree, [t2]}}]
    ).


start(Params, T1Mark, T2Mark) ->
    {Segments, Width, MemLevels} = Params,
    %% Return now so we can store symbolic value in procdict in next_state call
    HT1 = hashtree:new({0,0}, [{segments, Segments},
                               {width, Width},
                               {mem_levels, MemLevels}]),

    T1 = case T1Mark of
        mark_empty -> hashtree:mark_open_empty({0,0}, HT1);
        _ -> hashtree:mark_open_and_check({0,0}, HT1)
    end,

    put(t1, T1),

    HT2 = hashtree:new({0,0}, [{segments, Segments},
                               {width, Width},
                               {mem_levels, MemLevels}]),

    T2 = case T2Mark of
        mark_empty -> hashtree:mark_open_empty({0,0}, HT2);
        _ -> hashtree:mark_open_and_check({0,0}, HT2)
    end,

    put(t2, T2),

    %% Make sure ETS is pristine
    catch ets:delete(t1),
    catch ets:delete(t2),
    catch ets:delete(s1),
    catch ets:delete(s2),
    ets_new(t1),
    ets_new(t2),
    ok.

update_snapshot(T, S) ->
    %% Snapshot the hashtree and store both states
    {SS, HT} = hashtree:update_snapshot(get(T)),
    put(T, HT),
    put({snap, T}, SS),
    %% Copy the current ets table to the snapshot table.
    catch ets:delete(S),
    ets_new(S),
    ets:insert(S, ets:tab2list(T)),
    ok.

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


postcondition(_S,{call,_,start, [_, T1Mark, T2Mark]},_R) ->
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

next_state(S,_R,{call, _, start, [Params,_,_]}) ->
    S#state{started = true, params = Params};
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
next_state(S,_V,{call, _, write, _}) ->
    S;
next_state(S,_R,{call, _, write_both, _}) ->
    S;
next_state(S,_V,{call, _, delete, _}) ->
    S;
next_state(S,_R,{call, _, delete_both, _}) ->
    S;
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
    ?FORALL(Cmds,non_empty(commands(?MODULE, #state{})),
            aggregate(command_names(Cmds),
                begin
                    %%io:format(user, "Starting in ~p\n", [self()]),
                    put(t1, undefined),
                    put(t2, undefined),
                    catch ets:delete(t1),
                    catch ets:delete(t2),
                    {_H,S,Res} = HSR = run_commands(?MODULE,Cmds),
		    %% {Segments, Width, MemLevels} = S#state.params,
                    pretty_commands(?MODULE, Cmds, HSR,
                                    ?WHENFAIL(
                                       begin
					   {Segments, Width, MemLevels} = S#state.params,
					   eqc:format("Segments ~p\nWidth ~p\nMemLevels ~p\n",
						      [Segments, Width, MemLevels]),
					   eqc:format("=== t1 ===\n~p\n\n", [ets:tab2list(t1)]),
					   eqc:format("=== s1 ===\n~p\n\n", [ets:tab2list(s1)]),
					   eqc:format("=== t2 ===\n~p\n\n", [ets:tab2list(t2)]),
					   eqc:format("=== s2 ===\n~p\n\n", [ets:tab2list(s2)]),
					   eqc:format("=== ht1 ===\n~w\n~p\n\n", [get(t1), catch dump(get(t1))]),
					   eqc:format("=== ht2 ===\n~w\n~p\n\n", [get(t2), catch dump(get(t2))]),
					   
                                           catch hashtree:destroy(hashtree:close(get(t1))),
                                           catch hashtree:destroy(hashtree:close(get(t2)))
                                       end,
				       equals(ok, Res)))
		end)).

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
    DeltaKeys = [K || {K, {V1, V2}} <- SnapDeltas, V1 /= '$none', V2 /= '$none'],
    Filter = fun(K, _H) ->
		     lists:member(K, DeltaKeys)
	     end,
    F1 = orddict:filter(Filter, orddict:from_list(ets:tab2list(t1))),
    F2 = orddict:filter(Filter, orddict:from_list(ets:tab2list(t2))),
    Deltas = riak_ensemble_util:orddict_delta(F1, F2),

    %% Missing keys are detected from the snapshots, but different keys are double-checked
    %% against the current hash value to replicate a segment.

    %% Have to task into account flushing the buffer... sigh, so messy. temporarily fix with flush before local compare.
    %% Wonder if the AAE lookup functions should come from the snapshot.
    lists:sort(
      [{missing, K} || {K, {'$none', _}} <- SnapDeltas] ++
	  [{remote_missing, K} || {K, {_, '$none'}} <- SnapDeltas] ++
          [{different, K} || {K, {V1, V2}} <- Deltas, V1 /= '$none', V2 /= '$none']).
	
ets_new(T) ->
    ets:new(T, [named_table, public, set]).


-endif.
-endif.
