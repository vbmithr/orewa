open Core
open Async

let key = "testkey1"

let rec req conn =
  match%bind Orewa.get conn key with
  | Ok (Some key) ->
    eprintf "OK %s\n%!" key ;
    req conn
  | Ok None ->
    eprintf "OK None\n%!" ;
    req conn
  | Error e ->
      eprintf "Error %s\n" (Error.to_string_hum e) ;
      return ()

let endp = Host_and_port.create ~host:"localhost" ~port:6379

let main nbIter =
  Tcp.with_connection (Tcp.Where_to_connect.of_host_and_port endp) begin fun _ r w ->
    let conn = Orewa.create r w in
    Orewa.set conn key "test" >>|? fun b ->
    eprintf "key %s set (%b)\n%!" key b ;
    Deferred.List.init nbIter ~f:(fun _ -> req conn)
  end

let () =
  Command.async ~summary:"Run continous read" begin
    let open Command.Let_syntax in
    [%map_open
      let nbIter = flag_optional_with_default_doc "nbIter" int sexp_of_int ~default:10 ~doc:"nbIter" in
      fun () ->
        main nbIter >>= function
        | Error e -> Error.raise e
        | Ok _ -> Deferred.unit
    ] end
  |> Command.run

