-module(sk_map_pulling_worker).


-export([
         start/2,
         init/3,
         loop/3
        ]).



start(Workflow, Combiner) ->
  Source = self(),
  proc_lib:spawn(?MODULE, init, [Source, Workflow, Combiner]).



init(Source, Workflow, Combiner) ->
  Worker = sk_assembler:make(Workflow, _SendBack = self()),
  loop(Source,
       Worker,
       Combiner).


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
       
    
  
