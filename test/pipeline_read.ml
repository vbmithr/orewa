open Core
open Async

let key = "testkey1"
let src = Logs.Src.create "orewa.pipeline_read"
module Log_async = (val Logs_async.src_log src : Logs_async.LOG)

let rec req conn =
  Log_async.debug (fun m -> m "get %s" key) >>= fun () ->
  Orewa.get conn key >>= function
  | Ok (Some key) ->
    Log_async.debug (fun m -> m "OK %s" key) >>= fun () ->
    req conn
  | Ok None ->
    Log_async.debug (fun m -> m "OK None") >>= fun () ->
    req conn
  | Error e ->
    Log_async.debug (fun m -> m "Error %a" Error.pp e) >>= fun () ->
    return ()

let endp = Host_and_port.create ~host:"localhost" ~port:6379

let main nbIter =
  Tcp.with_connection (Tcp.Where_to_connect.of_host_and_port endp) begin fun _ r w ->
    let conn = Orewa.create r w in
    Log_async.debug (fun m -> m "set key %s" key) >>= fun () ->
    Deferred.Or_error.return true >>=? fun _b ->
    Orewa.set conn key "test" >>=? fun b ->
    Log_async.debug (fun m -> m "key %s set (%b)" key b) >>= fun () ->
    Deferred.List.init ~how:`Parallel nbIter ~f:(fun _ -> req conn) >>= fun _ ->
    Deferred.Or_error.return ()
  end

let () =
  Logs.set_reporter (Logs_async_reporter.reporter ()) ;
  Command.async ~summary:"Run continous read" begin
    let open Command.Let_syntax in
    [%map_open
      let nbIter = flag_optional_with_default_doc "nbIter" int sexp_of_int ~default:10 ~doc:"nbIter"
      and () = Logs_async_reporter.set_level_via_param [] in
      fun () ->
        main nbIter >>= function
        | Error e -> Error.raise e
        | Ok _ -> Deferred.unit
    ] end
  |> Command.run

