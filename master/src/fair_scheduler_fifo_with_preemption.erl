-module(fair_scheduler_fifo_with_preemption).
-behaviour(gen_server).

-include("common_types.hrl").
-include("gs_util.hrl").
-include("disco.hrl").
-include("pipeline.hrl").
-include("fair_scheduler.hrl").

-export([start_link/0, init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-type state() :: {[{pid(), jobname()}], cores()}.

-spec start_link() -> {ok, pid()}.
start_link() ->
    lager:info("Fair scheduler: FIFO policy with preemption"),
    case gen_server:start_link({local, sched_policy},
                               fair_scheduler_fifo_with_preemption, [],
                               disco:debug_flags("fair_scheduler_fifo_policy_with_preemption"))
    of  {ok, _Server} = Ret -> Ret;
        {error, {already_started, Server}} -> {ok, Server}
    end.

-spec init(_) -> gs_init().
init(_) ->
    {ok, {[], 0}}.

-type cast_msgs() :: policy_cast_msgs().

-spec handle_cast(cast_msgs(), state()) -> gs_noreply().
handle_cast({update_nodes, Nodes}, {Jobs, _}) ->
    NewNumCores = lists:sum([C || {_, C, _} <- Nodes]),
    {noreply, {Jobs, NewNumCores}};
handle_cast({new_job, JobPid, JobName}, {Jobs, NumCores}) ->
    erlang:monitor(process, JobPid),
    {noreply, {[{JobPid, JobName} | Jobs], NumCores}}.


-spec handle_call(current_priorities_msg(), from(), state()) ->
                         gs_reply([{jobname(), priority()}]);
                 (dbg_state_msg(), from(), state()) -> gs_reply(state());
                 (next_job_msg(), from(), state()) -> gs_reply(next_job()).
handle_call(current_priorities, _, {Jobs, _} = State) ->
    RawInitiatedJobs = [{JobName, catch fair_scheduler_job:get_running_tasks(JobPid, 100)} || {JobPid, JobName} <- Jobs],
    SortedJobs = lists:sort(fun({_, RunningA}, {_, RunningB}) -> RunningA < RunningB end, RawInitiatedJobs),
    {reply, {ok, case SortedJobs of
                     [{N, _}|R] -> [{N, -1.0}|[{M, 1.0} || {M, _} <- R]];
                     []         -> []
                 end}, State};

handle_call(dbg_get_state, _, State) ->
    {reply, State, State};

handle_call({next_job, NotJobs}, _, {Jobs, NumCores} = State) ->
    RawInitiatedJobs = [{JobPid, JobName, catch fair_scheduler_job:get_running_tasks(JobPid, 100)} || {JobPid, JobName} <- Jobs],
    Share = ceiling(NumCores / lists:max([1, length(RawInitiatedJobs)])),
    Candidates = [ {JobPid, JobName, gb_trees:size(T)} || {JobPid, JobName, {ok, T}} <- RawInitiatedJobs, gb_trees:size(T) < Share],
    SortedCandidates = lists:sort(fun({_, _, RunningA}, {_, _, RunningB}) -> RunningA < RunningB end, Candidates),
    {reply, dropwhile(SortedCandidates, Jobs, NotJobs), State}.

ceiling(X) when X < 0 ->
    trunc(X);
ceiling(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T + 1
    end.

dropwhile([{JobPid, _, _} | T], Jobs, NotJobs) ->
    V = lists:member(JobPid, NotJobs),
    if V    -> dropwhile(T, Jobs, NotJobs);
       true -> {ok, JobPid}
    end;
dropwhile([], [{JobPid, _} | T], NotJobs) ->
    V = lists:member(JobPid, NotJobs),
    if V    -> dropwhile([], T, NotJobs);
       true -> {ok, JobPid}
    end;
dropwhile([], [], _) -> nojobs.

-spec handle_info({'DOWN', _, _, pid(), _}, state()) -> gs_noreply().
handle_info({'DOWN', _, _, JobPid, _}, {Jobs, NumCores}) ->
    {value, {_, JobName} = E} = lists:keysearch(JobPid, 1, Jobs),
    fair_scheduler:job_done(JobName),
    {noreply, {Jobs -- [E], NumCores}}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
