-module(sk_map_pulling_worker).


-export([
         start/2,
         init/2,
         loop/3
        ]).



start(Workflow, Combiner) ->
  proc_lib:spawn(?MODULE, init, [Workflow, Combiner]).



init(Workflow, Combiner) ->
  Worker = sk_utils:start_worker(Workflow, _SendBack = self()),
  proc_lib:spawn(?MODULE, loop, [_Source = self(),
                                 Worker,
                                 Combiner]).




loop(Source, Worker, Combiner) ->
  Source ! {give_me_work, self()},
  receive
    {data, _, _} = Data -> %% TODO representation
      %% todo 
      Worker ! Data,
      receive
        {data, _, _} = OutputData ->
          Combiner ! OutputData,
          loop(Source, Worker, Combiner)            
      end;
    {system, eos} ->
      sk_utils:stop_workers(?MODULE, [Worker, Combiner]),
      eos
  end.
       
    
  
