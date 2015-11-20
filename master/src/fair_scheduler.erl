% This module implements the global task scheduler (GTS).  It is
% called by disco_server on any event that indicates a new task could
% perhaps be scheduled on the cluster.  The selection of this task is
% split into two phases:
%
% . First, a candidate job is selected according to the scheduler
%   policy (omitting from consideration any jobs who do not have any
%   tasks that can be scheduled currently on the available cluster
%   nodes).
%
% . Next, the job's task scheduler (JTS) in fair_scheduler_job.erl is
%   called with the list of cluster nodes which have available
%   computing slots.  The task returned by the JTS is then returned by
%   the GTS.  If the job's JTS does not have a candidate task, we
%   re-run the previous step with this job added to the omit list.

-module(fair_scheduler).
-behaviour(gen_server).

-export([start_link/0, new_job/2, job_done/1,
         next_task/1, new_task/2, update_nodes/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("common_types.hrl").
-include("gs_util.hrl").
-include("disco.hrl").
-include("pipeline.hrl").
-include("fair_scheduler.hrl").

-type state() :: [node_info()].

%% ===================================================================
%% API functions

-spec start_link() -> {ok, pid()}.
start_link() ->
    lager:info("Fair scheduler starts"),
    case gen_server:start_link({local, ?MODULE}, fair_scheduler, [],
                               disco:debug_flags("fair_scheduler"))
    of  {ok, _Server} = Ret -> Ret;
        {error, {already_started, Server}} -> {ok, Server}
    end.

-spec update_nodes([node_info()]) -> ok.
update_nodes(NewNodes) ->
    gen_server:cast(?MODULE, {update_nodes, NewNodes}).

-spec job_done(jobname()) -> ok.
job_done(JobName) ->
    gen_server:cast(?MODULE, {job_done, JobName}).

-spec next_task([host()]) -> nojobs | {ok, {pid(), {host(), task()}}}.
next_task(AvailableNodes) ->
    gen_server:call(?MODULE, {next_task, AvailableNodes}, infinity).

% These are calls and not casts, since we don't want to race against
% disco_server. We need to send the new_job and new_task messages to
% the JTS before disco_server sends it a next_task request.

-spec new_job(jobname(), pid()) -> ok.
new_job(JobName, JobCoord) ->
    gen_server:call(?MODULE, {new_job, JobName, JobCoord}).

-spec new_task(task(), loadstats()) -> unknown_job | ok.
new_task(Task, LoadStats) ->
    gen_server:call(?MODULE, {new_task, Task, LoadStats}).

%% ===================================================================
%% gen_server callbacks

-spec init([]) -> gs_init().
init([]) ->
    {ok, _ } =
        case application:get_env(scheduler_opt) of
            {ok, "fifo"} ->
                lager:info("Scheduler uses fifo policy"),
                fair_scheduler_fifo_policy:start_link();
            {ok, "fair_with_preemption"} -> 
                lager:info("Scheduler uses fair policy with preemption"),
                fair_scheduler_fair_policy_with_preemption:start_link();
            _ ->
                lager:info("Scheduler uses fair policy"),
                fair_scheduler_fair_policy:start_link()
        end,
    % jobs: {Key :: jobname(),
    %        JobInfo :: {FairScheduler :: pid(), JobCoord :: pid()}}
    _ = ets:new(jobs, [private, named_table]),
    {ok, []}.

-spec handle_cast(update_nodes_msg() | {job_done, jobname()}, state())
                 -> gs_noreply().
handle_cast({update_nodes, NewNodes}, _) ->
    gen_server:cast(sched_policy, {update_nodes, NewNodes}),
    Hosts = [H || {H, _, _} <- NewNodes],
    ets:foldl(fun({_, {JobPid, JobCoord}}, ok) ->
                      fair_scheduler_job:update_nodes(JobPid, NewNodes),
                      job_coordinator:update_nodes(JobCoord, Hosts)
              end, ok, jobs),
    {noreply, NewNodes};

handle_cast({job_done, JobName}, S) ->
    % We absolutely don't want to have the job coordinator alive after the
    % job has been removed from the scheduler. Make sure that doesn't
    % happen.
    case ets:lookup(jobs, JobName) of
        [] ->
            {noreply, S};
        [{_, {_, JobCoord}}] ->
            ets:delete(jobs, JobName),
            exit(JobCoord, kill_worker),
            {noreply, S}
    end.

-spec handle_call({new_job, jobname(), pid()}, from(), state()) -> gs_reply(ok);
                 (new_task_msg(), from(), state()) -> gs_reply(unknown_job | ok);
                 (dbg_state_msg(), from(), state()) -> gs_reply(state());
                 ({next_task, [host()]}, from(), state()) ->
                         gs_reply(nojobs | {ok, {pid(), {node(), task()}}}).

handle_call({new_job, JobName, JobCoord}, _, Nodes) ->
    {ok, JobPid} = fair_scheduler_job:start(JobName, JobCoord),
    fair_scheduler_job:update_nodes(JobPid, Nodes),
    job_coordinator:update_nodes(JobCoord, [H || {H, _, _} <- Nodes]),
    gen_server:cast(sched_policy, {new_job, JobPid, JobName}),
    ets:insert(jobs, {JobName, {JobPid, JobCoord}}),
    {reply, ok, Nodes};

handle_call({new_task, {#task_spec{jobname = Job}, _} = T, Load}, _, S) ->
    case ets:lookup(jobs, Job) of
        [] ->
            {reply, unknown_job, S};
        [{_, {JobPid, _}}] ->
            fair_scheduler_job:new_task(JobPid, T, Load),
            {reply, ok, S}
    end;

handle_call(dbg_get_state, _, S) ->
    {reply, {S, ets:tab2list(jobs)}, S};

handle_call({next_task, AvailableNodes}, _From, S) ->
    Jobs = [JobPid || {_, {JobPid, _}} <- ets:tab2list(jobs)],
    {reply, next_task(AvailableNodes, Jobs, []), S}.

next_task(AvailableNodes, Jobs, NotJobs) ->
    case gen_server:call(sched_policy, {next_job, NotJobs}, infinity) of
        {ok, JobPid} ->
            case fair_scheduler_job:next_task(JobPid, Jobs, AvailableNodes) of
                {ok, {_Host, _Task} = Res} -> {ok, {JobPid, Res}};
                none -> next_task(AvailableNodes, Jobs, [JobPid|NotJobs])
            end;
        nojobs -> nojobs
    end.

-spec handle_info(term(), state()) -> gs_noreply().
handle_info(_Msg, State) -> {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    _ = [exit(JobPid, kill) || {_, {JobPid, _}} <- ets:tab2list(jobs)],
    ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
