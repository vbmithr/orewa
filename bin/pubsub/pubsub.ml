open Core
open Async
open Orewa

let src = Logs.Src.create "orewa.pubsub_test"
module Log_async = (val Logs_async.src_log src : Logs_async.LOG)

let endp = Host_and_port.create ~host:"localhost" ~port:6379

let ch = "prout"

let random_string n =
  String.init n ~f:(fun _ -> Char.of_int_exn (Random.int 255))

let producer id =
  Tcp.with_connection (Tcp.Where_to_connect.of_host_and_port endp) begin fun _ r w ->
    let conn = create r w in
    let rec inner () =
      publish conn ch (random_string 255) >>= fun nbRead ->
      Log_async.debug (fun m -> m "Pr %d: %d clients read" id nbRead) >>= fun () ->
      inner () in
    inner ()
  end

let consumer id =
  Tcp.with_connection (Tcp.Where_to_connect.of_host_and_port endp) begin fun _ r w ->
    let conn = Sub.create r w in
    Sub.subscribe conn [ch] >>= fun () ->
    let r = Sub.pubsub conn in
    Pipe.iter r ~f:begin function
      | Subscribe _ -> Log_async.debug (fun m -> m "Co %d: subscribed" id)
      | Unsubscribe _ -> Log_async.debug (fun m -> m "Co %d: unsubscribed" id)
      | Message _ -> Log_async.debug (fun m -> m "Co %d: read msg" id)
      | Pong msg -> Log_async.debug (fun m -> m "Co %d: PONG %s" id (Option.value ~default:"PONG" msg))
      | Exit -> Log_async.debug (fun m -> m "Co %d: EXIT" id)
    end
  end

let main nbConsumers nbProducers =
  Deferred.all_unit [
    (Deferred.List.init nbProducers ~f:producer >>= fun _ -> Deferred.unit) ;
    (Deferred.List.init nbConsumers ~f:consumer >>= fun _ -> Deferred.unit) ;
  ]

let () =
  Logs.set_reporter (Logs_async_reporter.reporter ()) ;
  Command.async ~summary:"PubSub test" begin
    let open Command.Let_syntax in
    [%map_open
      let nbConsumers = flag_optional_with_default_doc "nbConsumers" int sexp_of_int ~default:1 ~doc:"nbConsumers"
      and nbProducers = flag_optional_with_default_doc "nbProducers" int sexp_of_int ~default:1 ~doc:"nbProducers"
      and () = Logs_async_reporter.set_level_via_param [] in
      fun () -> main nbConsumers nbProducers
    ] end
  |> Command.run
