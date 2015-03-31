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

-module(sk_map_pull_partitioner).

-export([
         start/3
        ]).

%% Privete exports
-export([
         loop_pull_init/3
        ]).

-include("skel.hrl").


-spec start(atom(), workflow(), pid()) -> pid().
%% @doc Starts the recursive partitioning of inputs.
%%
%% If the number of workers to be used is specified, a list of Pids for those
%% worker processes are received through `WorkerPids' and the second clause
%% used. Alternatively, a workflow is received as `Workflow' and the first
%% clause is used. In the case of the former, the workers have already
%% been initialised with their workflows and so the inclusion of the
%% `WorkFlow' argument is unneeded in this clause.
%%
%% The atoms `auto' and `main' are used to determine whether a list of worker
%% Pids or a workflow is received. `CombinerPid' specifies the Pid of the
%% process that will recompose the partite elements following their
%% application to the Workflow.
%%
%% @todo Wait, can't this atom be gotten rid of? The types are sufficiently different.
start(WorkFlow, NWorkers, CombinerPid) ->
  sk_tracer:t(75, self(), {?MODULE, start}, [{combiner, CombinerPid}]),
  proc_lib:spawn_link(?MODULE, loop_pull_init, [WorkFlow, NWorkers, CombinerPid]).


loop_pull_init(WorkFlow, NWorkers, CombinerPid) ->
  AllWorkers = [sk_map_pulling_worker:start(WorkFlow, CombinerPid) ||
                 _ <- lists:seq(1,NWorkers)],

  loop_pull([], [], AllWorkers).


loop_pull(WorkData, WatingWorkers, AllWorkers) ->
  {WorkData2, WatingWorkers2} =  send_data_to_workers(WorkData, WatingWorkers),
  receive
    {give_me_work, Worker} ->
      loop_pull(WorkData2, [Worker | WatingWorkers2], AllWorkers);
    {data, _, _}  = DataMessage ->
      NewData = partition(DataMessage),
      loop_pull(WorkData2 ++ NewData, WatingWorkers2, AllWorkers);
    {system, eos} ->
      loop_pull_finish(WorkData2, WatingWorkers2, AllWorkers)
    end.



loop_pull_finish([], _, AllWorkers) ->
  sk_utils:stop_workers(?MODULE, AllWorkers),
  eos;

loop_pull_finish(WorkData, WatingWorkers, AllWorkers) ->
  {WorkData2, WatingWorkers2} =
    send_data_to_workers(WorkData, WatingWorkers),
  receive
    {give_me_work, Worker} ->
      loop_pull_finish(WorkData2, [Worker | WatingWorkers2], AllWorkers)
  end.


%% @doc splits given data in data message into many messages.  All
%% messages have to inherit `dm_identifier()`'s.  In addition each
%% messages receive `dm_identifier()`'s needed for connecting them
%% back together.  Those are `Ref` wich is unique for each input data
%% message, number of all messages, and each message index.  Last one
%% is needed only for sake of combiner, who uses them together with
%% `Ref` as `dict()` (hence needed uniqueness).
-spec partition(data_message()) -> [data_message()].

partition({data, Data, Idx}) ->
  DataMessages = [{data, X, Idx} || X <- Data],
  Ref = make_ref(),
  MessageCount = length(Data),
  {_, List} = lists:foldl(
                fun(OneMessage, {Counter, Acc} ) ->
                    {Counter + 1,
                     [sk_data:push({decomp,
                                    Ref,
                                    Counter,
                                    MessageCount}, OneMessage) | Acc]}
                end,
                _CountFrom = {1, []},
                DataMessages),
  List.



-spec send_data_to_workers([data_message()], [pid()]) -> 
                              { [data_message()], [pid()]}.

send_data_to_workers([Data | Datas], [Worker| Workers]) ->
  Worker ! Data,
  send_data_to_workers(Datas, Workers);

%% One of those should be empty
send_data_to_workers(Data, Workers) ->
  {Data, Workers}.

