-module(fair_scheduler_fifo_with_preemption).
-behaviour(gen_server).

-include("common_types.hrl").
-include("gs_util.hrl").
-include("disco.hrl").
-include("pipeline.hrl").
-include("fair_scheduler.hrl").

-export([start_link/0, init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-type state() :: {[{pid(), jobname(), non_neg_integer()}], cores(),  disco_queue({pid(), non_neg_integer()})}.

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
    {ok, {[], 0, queue:new()}}.

-type cast_msgs() :: policy_cast_msgs().

-spec handle_cast(cast_msgs(), state()) -> gs_noreply().
handle_cast({update_nodes, Nodes}, {Jobs, _, Q}) ->
    NewNumCores = lists:sum([C || {_, C, _} <- Nodes]),
    {noreply, {Jobs, NewNumCores, Q}};
handle_cast({new_job, JobPid, JobName}, {Jobs, NumCores, Q}) ->
    erlang:monitor(process, JobPid),
    NTasks = NumCores / (length(Jobs) + 1),
    kill_n_tasks_from_jobs(NTasks, Jobs),
    NewQ = case length(Jobs) of
      0 -> Q;
      _ -> queue:in({JobPid, round(NTasks)}, Q)
    end,
    {noreply, {[{JobPid, JobName, 0} | Jobs], NumCores, NewQ}}.

kill_n_tasks_from_jobs(N, Jobs) ->
    lists:foreach(fun({JobPid, _, _}) ->
                     {ok, X} = fair_scheduler_job:get_running_tasks(JobPid, 100),
                     Workers = gb_trees:keys(X),
                     lager:info("must kill ~p from ~p", [round(N / length(Jobs)), Workers]),
                     preempt_n_workers(round(N / length(Jobs)), Workers)
                  end, Jobs).

-spec handle_call(current_priorities_msg(), from(), state()) ->
                         gs_reply([{jobname(), priority()}]);
                 (dbg_state_msg(), from(), state()) -> gs_reply(state());
                 (next_job_msg(), from(), state()) -> gs_reply(next_job()).
handle_call(current_priorities, _, {Jobs, _, _} = State) ->
    RawInitiatedJobs = [{JobName, catch fair_scheduler_job:get_running_tasks(JobPid, 100)} || {JobPid, JobName, _} <- Jobs],
    SortedJobs = lists:sort(fun({_, RunningA}, {_, RunningB}) -> RunningA < RunningB end, RawInitiatedJobs),
    {reply, {ok, case SortedJobs of
                     [{N, _}|R] -> [{N, -1.0}|[{M, 1.0} || {M, _} <- R]];
                     []         -> []
                 end}, State};

handle_call(dbg_get_state, _, State) ->
    {reply, State, State};

handle_call({next_job, NotJobs}, _, {Jobs, NumCores, Q} = State) ->
    case queue:out(Q) of
        {{value, {JobPid, NTasks}}, NQ} ->
            V = lists:member(JobPid, NotJobs),
            if V    -> {reply, nojobs, State};
               true ->
                   case NTasks of
                     0 -> {reply, {ok, JobPid}, {Jobs, NumCores, NQ}};
                     _ -> {reply, {ok, JobPid}, {Jobs, NumCores, queue:in({JobPid, NTasks - 1}, NQ)}}
                   end
            end;
        {empty, _} ->
            RawInitiatedJobs = [{JobPid, JobName, Stage, catch fair_scheduler_job:get_stats(JobPid, 100)} || {JobPid, JobName, Stage} <- Jobs],
            % Check if there is a job in a transitory phase, eg from map to map_shuffle or from map_shuffle to reduce
            case lists:keyfind({ok,{0,0}}, 4, RawInitiatedJobs) of
                false ->
                    InitiatedJobs = [{JobPid, JobName, N} || {JobPid, JobName, _, {ok, {_, N}}} <- RawInitiatedJobs],
                    Share = NumCores / lists:max([1, length(InitiatedJobs)]),
                    Candidates = [ J || {_, _, N} = J <- InitiatedJobs, N < Share],
                    SortedCandidates = lists:sort(fun({_, _, RunningA}, {_, _, RunningB}) -> RunningA < RunningB end, Candidates),
                    {reply, dropwhile(SortedCandidates, [], NotJobs), State};
                {_, _, 2, _} -> 
                    InitiatedJobs = [{JobPid, JobName, N} || {JobPid, JobName, _, {ok, {_, N}}} <- RawInitiatedJobs],
                    Share = NumCores / lists:max([1, length(InitiatedJobs)]),
                    Candidates = [ J || {_, _, N} = J <- InitiatedJobs, N < Share],
                    SortedCandidates = lists:sort(fun({_, _, RunningA}, {_, _, RunningB}) -> RunningA < RunningB end, Candidates),
                    {reply, dropwhile(SortedCandidates, Jobs, NotJobs), State};
                {Pid, Name, Stage, _} -> 
                    NewJobs = [{Pid, Name, Stage + 1} | Jobs -- [{Pid, Name, Stage}]],
                    {reply, nojobs, {NewJobs, NumCores, Q}}
            end
    end.

preempt_n_workers(0, _Workers) -> do_nothing;
preempt_n_workers(N, [W | R]) ->
    exit(W, "Preempted"),
    preempt_n_workers(N - 1, R).

dropwhile([{JobPid, _, _} | T], Jobs, NotJobs) ->
    V = lists:member(JobPid, NotJobs),
    if V    -> dropwhile(T, Jobs, NotJobs);
       true -> {ok, JobPid}
    end;
dropwhile([], [{JobPid, _, _} | T], NotJobs) ->
    V = lists:member(JobPid, NotJobs),
    if V    -> dropwhile([], T, NotJobs);
       true -> {ok, JobPid}
    end;
dropwhile([], [], _) -> nojobs.

-spec handle_info({'DOWN', _, _, pid(), _}, state()) -> gs_noreply().
handle_info({'DOWN', _, _, JobPid, _}, {Jobs, NumCores, Q}) ->
    % Remove the job done from jobs queue
    {value, {_, JobName, _} = E} = lists:keysearch(JobPid, 1, Jobs),
    fair_scheduler:job_done(JobName),

    % Remove the job done from preemption queue if its there
    L = queue:to_list(Q),
    L2 = case lists:keysearch(JobPid, 1, L) of
        {value, I} -> L -- [I];
        false -> L
    end,

    {noreply, {Jobs -- [E], NumCores, queue:from_list(L2)}}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
