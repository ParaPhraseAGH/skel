%%%----------------------------------------------------------------------------
%%% @author Sam Elliot <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @headerfile "skel.hrl"
%%%
%%% @doc This module contains the simple 'map' skeleton partitioner logic.
%%%
%%% The Map skeleton is a parallel map. The skeleton applies a given function
%%% to the elements within one or more lists.
%%%
%%% The partitioner takes the input list, dispatching each partite element to
%%% a pool of worker processes. These worker processes apply the
%%% developer-defined function to each partite element.
%%%
%%% This module supports both the automatic creation of worker processes, and
%%% the ability to define the exact number to be used. With the former, the
%%% minimal number of workers are created for all inputs. This is given by the
%%% number of elements in the longest list.
%%%
%%% @end
%%%----------------------------------------------------------------------------

-module(sk_map_man_partitioner).

-export([
         start/3,
         start/5
        ]).

%% Privete exports
-export([
         loop/2
        ]).

-include("skel.hrl").


%% @doc Starts the recursive partitioning of inputs.
%%
%% If the number of workers to be used is specified, a list of Pids for those
%% worker processes are received through `WorkerPids' and the second clause
%% used. Alternatively, a workflow is received as `Workflow' and the first
%% clause is used. In the case of the former, the workers have already
%% been initialised with their workflows and so the inclusion of the
%% `WorkFlow' argument is unneeded in this clause.
%%
%% `CombinerPid' specifies the Pid of the process that will recompose
%% the partite elements following their application to the Workflow.
%%
%% @todo Wait, can't this atom be gotten rid of? The types are sufficiently different.
-spec start(workflow(), pos_integer(), pid()) -> pid().
start(WorkFlow, NWorkers, CombinerPid) ->
  sk_tracer:t(75, self(), {?MODULE, start}, [{combiner, CombinerPid}]),
  WorkerPids = sk_utils:start_workers(NWorkers, WorkFlow, CombinerPid),
  proc_lib:spawn_link(?MODULE, loop, [decomp_by(), WorkerPids]).

-spec start( pos_integer(), pos_integer(), workflow(), workflow(), pid()) -> pid().
start(NCPUWorkers, NGPUWorkers, WorkFlowCPU, WorkFlowGPU, CombinerPid) ->
  sk_tracer:t(75, self(), {?MODULE, start}, [{combiner, CombinerPid}]),
  WorkerPids = sk_utils:start_workers_hyb(NCPUWorkers, NGPUWorkers, WorkFlowCPU, WorkFlowGPU, CombinerPid),
  proc_lib:spawn_link(?MODULE, loop, [man, WorkerPids, CombinerPid]).



-spec loop(data_decomp_fun(), [pid()]) -> 'eos'.
%% @doc Recursively receives inputs as messages, which are decomposed, and the
%% resulting messages sent to individual workers. `loop/3' is used in place of
%% {@link loop/4} when the number of workers is set by the developer.
loop(DataPartitionerFun, WorkerPids) ->
  receive
    {data, _, _} = DataMessage ->
      PartitionMessages = DataPartitionerFun(DataMessage),
      Ref = make_ref(),
      sk_tracer:t(60, self(), {?MODULE, data}, [{ref, Ref}, {input, DataMessage}, {partitions, PartitionMessages}]),
      dispatch(Ref, length(PartitionMessages), PartitionMessages, WorkerPids),
      loop(DataPartitionerFun, WorkerPids);
    {system, eos} ->
      sk_utils:stop_workers(?MODULE, WorkerPids),
      eos
    end.


-spec decomp_by() -> data_decomp_fun().
%% @doc Provides the decomposition function and means to split a single input
%% into many. This is based on the identity function, as the Map skeleton is
%% applied to lists.
decomp_by() ->
  fun({data, Value, Ids}) ->
    [{data, X, Ids} || X <- Value]
  end.


-spec dispatch(reference(), pos_integer(), [data_message(),...], [pid()]) -> 'ok'.
%% @doc Partite elements of input stored in `PartitionMessages' are formatted
%% and sent to a worker from `WorkerPids'. The reference argument `Ref'
%% ensures that partite elements from different inputs are not incorrectly
%% included.
dispatch(Ref, NPartitions, PartitionMessages, WorkerPids) ->
  dispatch(Ref, NPartitions, 1, PartitionMessages, WorkerPids).


-spec dispatch(reference(), pos_integer(), pos_integer(), [data_message(),...], [pid()]) -> 'ok'.
%% @doc Inner-function for {@link dispatch/4}. Recursively sends each message
%% to a worker, following the addition of references to allow identification
%% and recomposition.
dispatch(_Ref,_NPartitions, _Idx, [], _) ->
  ok;
dispatch(Ref, NPartitions, Idx, [PartitionMessage|PartitionMessages], [WorkerPid|WorkerPids]) ->
  PartitionMessage1 = sk_data:push({decomp, Ref, Idx, NPartitions}, PartitionMessage),
  sk_tracer:t(50, self(), WorkerPid, {?MODULE, data}, [{partition, PartitionMessage1}]),
  WorkerPid ! PartitionMessage1,
  dispatch(Ref, NPartitions, Idx+1, PartitionMessages, WorkerPids ++ [WorkerPid]).
