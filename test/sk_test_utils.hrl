
-define( assertSameDataInside( Expected, Given),
         (?assertEqual(lists:sort(Expected),
                       lists:sort(Given)))).

    



-define(random_sleep_for( MaxTime ),
        fun( X ) ->
                timer:sleep( ?random_int_up_to( MaxTime ) ),
                X
        end).


-define(random_int_up_to ( Max ) ,
        erlang:round( random:uniform() * Max )).



