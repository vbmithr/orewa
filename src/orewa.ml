open Core
open Async

module Resp = struct
  type t =
    | String of string
    | Error of Error.t
    | Integer of int
    | Bulk of string
    | Array of t list
    | Null
  [@@deriving sexp_of]

  let string s = String s
  let error e = Error e
  let error_string s = Error (Error.of_string s)
  let int i = Integer i
  let bulk s = Bulk s
  let array a = Array a
  let null = Null

  let get_bulk = function
    | Bulk s -> s
    | _ -> invalid_arg "get_bulk"

  let string_or_error = function
    | String s -> Ok s
    | Error e -> Error e
    | _ -> assert false
  let bulk_or_error = function
    | Bulk s -> Ok s
    | Error e -> Error e
    | _ -> assert false
  let null_bulk_or_error = function
    | Bulk s -> Ok (Some s)
    | Null -> Ok None
    | Error e -> Error e
    | _ -> assert false
  let null_string_or_error = function
    | String s -> Ok (Some s)
    | Null -> Ok None
    | Error e -> Error e
    | _ -> assert false
  let int_or_error = function
    | Integer s -> Ok s
    | Error e -> Error e
    | _ -> assert false

  let rec pp ppf = function
    | Error e   -> Format.fprintf ppf "-%a\r\n" Error.pp e
    | String s  -> Format.fprintf ppf "+%s\r\n" s
    | Integer n -> Format.fprintf ppf ":%d\r\n" n
    | Bulk s    -> Format.fprintf ppf "$%d\r\n%s\r\n" (String.length s) s
    | Array xs  -> Format.fprintf ppf "*%d\r\n%a" (List.length xs) (Format.pp_print_list ~pp_sep:(fun _ () -> ()) pp) xs
    | Null      -> Format.fprintf ppf "$-1\r\n"

  let to_string t = Format.asprintf "%a" pp t
  (* let show t = Format.asprintf "%a" Sexp.pp (sexp_of_t t) *)

  module Eof = struct
    let return a = return (`Ok a)
    let (>>=?) a f = a >>= function
      | `Eof -> Deferred.return `Eof
      | `Ok v -> f v
    let (>>|?) a f = a >>= function
      | `Eof -> Deferred.return `Eof
      | `Ok v -> Deferred.return (`Ok (f v))
  end

  let read_int r =
    let open Eof in
    Reader.read_line r >>|? Int.of_string

  let read_bulk r =
    let open Eof in
    read_int r >>=? function
    | -1 -> return null
    | len ->
      let buf = Bytes.create len in
      let open Deferred in
      Reader.really_read r buf >>= function
      | `Eof _ -> return `Eof
      | `Ok ->
        Reader.read_line r >>=? fun _ ->
        Eof.return (bulk (Bytes.unsafe_to_string ~no_mutation_while_string_reachable:buf))

  let rec read r =
    let open Eof in
    Reader.read_char r >>=? function
    | '+' -> Reader.read_line r >>|? string
    | '-' -> Reader.read_line r >>|? error_string
    | ':' -> read_int r >>|? int
    | '$' -> read_bulk r
    | '*' -> read_array r
    | _ -> assert false

  and read_array r =
    let open Eof in
    read_int r >>=? fun len ->
    let rec inner acc = function
      | 0 -> return acc
      | n ->
        read r >>=? fun elt ->
        inner (elt :: acc) (pred n)
    in
    inner [] len >>=? fun elts ->
    return (array (List.rev elts))
end

let connection_closed = Error.of_string "Connection closed"

type request = {
  command : string list;
  waiter : Resp.t Ivar.t
}

type t = {
  (* Need a queue of waiter Ivars. Need some way of closing the connection *)
  waiters : Resp.t Ivar.t Queue.t;
  writer : request Pipe.Writer.t
}

let create r w =
  let waiters = Queue.create () in
  let rec recv_loop reader =
    Resp.read reader >>= function
    | `Eof -> Deferred.unit
    | `Ok msg ->
      match Queue.dequeue waiters with
      | None when Reader.is_closed reader -> Deferred.unit
      | None -> Format.kasprintf failwith "No waiters are waiting for this message: %a" Resp.pp msg
      | Some waiter ->
        Ivar.fill waiter msg;
        recv_loop reader in
  (* Requests are posted to a pipe, and requests are processed in sequence *)
  let handle_request {command; waiter} =
    Queue.enqueue waiters waiter;
    Writer.write w Resp.(to_string @@ array (List.map command ~f:Resp.bulk)) in
  let request_writer = Pipe.create_writer begin fun pr ->
      Pipe.iter_without_pushback pr ~f:handle_request >>= fun () ->
      Writer.close w >>= fun () ->
      Reader.close r >>= fun () ->
      (* Signal this to all waiters. As the pipe has been closed, we
         know that no new waiters will arrive *)
      Queue.iter waiters ~f:(fun waiter -> Ivar.fill waiter (Resp.error connection_closed));
      return @@ Queue.clear waiters
    end in
  (* Start redis receiver. Processing ends if the connection is closed. *)
  (recv_loop r >>> fun () -> Pipe.close request_writer) ;
  (* Start processing requests. Once the pipe is closed, we signal
     closed to all outstanding waiters after closing the underlying
     socket *)
  { waiters; writer = request_writer }

let request { writer; _ } command =
  match Pipe.is_closed writer with
  | true -> Deferred.Or_error.fail connection_closed
  | false ->
      let waiter = Ivar.create () in
      Pipe.write writer {command; waiter} >>= fun () ->
      Ivar.read waiter >>= fun v ->
      Deferred.Or_error.return v

let request_string t c = request t c >>|? Resp.string_or_error >>| Or_error.join
let request_bulk t c = request t c >>|? Resp.bulk_or_error >>| Or_error.join
let request_null_bulk t c = request t c >>|? Resp.null_bulk_or_error >>| Or_error.join
let request_null_string t c = request t c >>|? Resp.null_string_or_error >>| Or_error.join
let request_int t c = request t c >>|? Resp.int_or_error >>| Or_error.join

let echo t message =
  request_bulk t ["ECHO"; message]

let set t ~key ?expire ?(exist = `Always) value =
  let expiry = match expire with
    | None -> []
    | Some span -> ["PX"; span |> Time_ns.Span.to_ms |> int_of_float |> string_of_int] in
  let existence =
    match exist with
    | `Always -> []
    | `Not_if_exists -> ["NX"]
    | `Only_if_exists -> ["XX"] in
  let command = ["SET"; key; value] @ expiry @ existence in
  request_null_string t command >>|? function
  | Some _ -> true
  | None -> false

let get t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["GET"; key] with
  | Resp.Bulk v -> return @@ Some v
  | Resp.Null -> return @@ None
  | _ -> assert false

let getrange t ~start ~stop key =
  request_bulk t ["GETRANGE"; key; string_of_int start; string_of_int stop]

let getset t ~key value =
  request_null_bulk t ["GETSET"; key; value]

let strlen t key =
  request_int t ["STRLEN"; key]

let mget t keys =
  let open Deferred.Result.Let_syntax in
  match%bind request t ("MGET" :: keys) with
  | Resp.Array xs ->
    return @@ List.fold_right xs ~init:[] ~f:begin fun item acc ->
      match item with
      | Resp.Null -> None :: acc
      | Resp.Bulk s -> Some s :: acc
      | _ -> assert false
    end
  | _ -> assert false

let mset t alist =
  let payload = alist |> List.map ~f:(fun (k, v) -> [k; v]) |> List.concat in
  Deferred.Or_error.ignore @@
  request_string t ("MSET" :: payload)

let msetnx t alist =
  let open Deferred.Result.Let_syntax in
  let payload = alist |> List.map ~f:(fun (k, v) -> [k; v]) |> List.concat in
  match%bind request t ("MSETNX" :: payload) with
  | Resp.Integer 1 -> return true
  | Resp.Integer 0 -> return false
  | _ -> assert false

let omnidirectional_push command t ?(exist = `Always) ~element ?(elements = []) key =
  let command =
    match exist with
    | `Always -> command
    | `Only_if_exists -> Printf.sprintf "%sX" command
  in
  request_int t ([command; key; element] @ elements)

let lpush = omnidirectional_push "LPUSH"
let rpush = omnidirectional_push "RPUSH"

let lrange t ~key ~start ~stop =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["LRANGE"; key; string_of_int start; string_of_int stop] with
  | Resp.Array xs ->
      List.map xs ~f:(function
          | Resp.Bulk v -> Ok v
          | _ -> assert false)
      |> Result.all
      |> Deferred.return
  | _ -> assert false

let lrem t ~key count ~element = request_int t ["LREM"; key; string_of_int count; element]

let lset t ~key index ~element =
  Deferred.Or_error.ignore (request_string t ["LSET"; key; string_of_int index; element])

let ltrim t ~start ~stop key =
  Deferred.Or_error.ignore (request_string t ["LTRIM"; key; string_of_int start; string_of_int stop])

let rpoplpush t ~source ~destination = request_bulk t ["RPOPLPUSH"; source; destination]
let append t ~key value = request_int t ["APPEND"; key; value]
let auth t password = Deferred.Or_error.ignore (request_string t ["AUTH"; password])

(* the documentation says it returns OK, but that's not true *)
let bgrewriteaof t = request_string t ["BGREWRITEAOF"]
let bgsave t = request_string t ["BGSAVE"]

let bitcount t ?range key =
  let range =
    match range with
    | None -> []
    | Some (start, end_) -> [string_of_int start; string_of_int end_]
  in
  request_int t (["BITCOUNT"; key] @ range)

type overflow =
  | Wrap
  | Sat
  | Fail

let string_of_overflow = function
  | Wrap -> "WRAP"
  | Sat -> "SAT"
  | Fail -> "FAIL"

(* Declaration of type of the integer *)
type intsize =
  | Signed of int
  | Unsigned of int

let string_of_intsize = function
  | Signed v -> Printf.sprintf "i%d" v
  | Unsigned v -> Printf.sprintf "u%d" v

type offset =
  | Absolute of int
  | Relative of int

let string_of_offset = function
  | Absolute v -> string_of_int v
  | Relative v -> Printf.sprintf "#%d" v

type fieldop =
  | Get of intsize * offset
  | Set of intsize * offset * int
  | Incrby of intsize * offset * int

let bitfield t ?overflow key ops =
  let open Deferred.Result.Let_syntax in
  let ops =
    List.map ops ~f:begin function
      | Get (size, offset) -> ["GET"; string_of_intsize size; string_of_offset offset]
      | Set (size, offset, value) ->
        [ "SET";
          string_of_intsize size;
          string_of_offset offset;
          string_of_int value ]
      | Incrby (size, offset, increment) ->
        [ "INCRBY";
          string_of_intsize size;
          string_of_offset offset;
          string_of_int increment ]
    end |> List.concat
  in
  let overflow =
    match overflow with
    | None -> []
    | Some behaviour -> ["OVERFLOW"; string_of_overflow behaviour]
  in
  match%bind request t (["BITFIELD"; key] @ overflow @ ops) with
  | Resp.Array xs ->
      return @@ List.fold_right xs ~init:[] ~f:begin fun v acc ->
        match v with
        | Resp.Integer i -> Some i :: acc
        | Resp.Null -> None :: acc
        | _ -> assert false
      end
  | _ -> assert false

type bitop =
  | AND
  | OR
  | XOR
  | NOT

let string_of_bitop = function
  | AND -> "AND"
  | OR -> "OR"
  | XOR -> "XOR"
  | NOT -> "NOT"

let bitop t ~destkey ?(keys = []) ~key op =
  request_int t (["BITOP"; string_of_bitop op; destkey; key] @ keys)

let string_of_bit = function
  | false -> "0"
  | true -> "1"

let bitpos t ?start ?stop key bit =
  let open Deferred.Result.Let_syntax in
  let%bind range =
    match start, stop with
    | Some s, Some e -> return [string_of_int s; string_of_int e]
    | Some s, None -> return [string_of_int s]
    | None, None -> return []
    | None, Some _ -> invalid_arg "Can't specify end without start"
  in
  match%bind request t (["BITPOS"; key; string_of_bit bit] @ range) with
  | Resp.Integer -1 -> return None
  | Resp.Integer n -> return @@ Some n
  | _ -> assert false

let getbit t key offset =
  let open Deferred.Result.Let_syntax in
  let offset = string_of_int offset in
  match%bind request t ["GETBIT"; key; offset] with
  | Resp.Integer 0 -> return false
  | Resp.Integer 1 -> return true
  | _ -> assert false

let setbit t key offset value =
  let open Deferred.Result.Let_syntax in
  let offset = string_of_int offset in
  let value = string_of_bit value in
  match%bind request t ["SETBIT"; key; offset; value] with
  | Resp.Integer 0 -> return false
  | Resp.Integer 1 -> return true
  | _ -> assert false

let decr t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["DECR"; key] with
  | Resp.Integer n -> return n
  | _ -> assert false

let decrby t key decrement =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["DECRBY"; key; string_of_int decrement] with
  | Resp.Integer n -> return n
  | _ -> assert false

let incr t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["INCR"; key] with
  | Resp.Integer n -> return n
  | _ -> assert false

let incrby t key increment =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["INCRBY"; key; string_of_int increment] with
  | Resp.Integer n -> return n
  | _ -> assert false

let incrbyfloat t key increment =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["INCRBYFLOAT"; key; string_of_float increment] with
  | Resp.Bulk v -> return @@ float_of_string v
  | _ -> assert false

let select t index =
  Deferred.Or_error.ignore @@
    request_string t ["SELECT"; string_of_int index]

let del t ?(keys = []) key =
  let open Deferred.Result.Let_syntax in
  match%bind request t (["DEL"; key] @ keys) with
  | Resp.Integer n -> return n
  | _ -> assert false

let exists t ?(keys = []) key =
  let open Deferred.Result.Let_syntax in
  match%bind request t (["EXISTS"; key] @ keys) with
  | Resp.Integer n -> return n
  | _ -> assert false

let expire t key span =
  let open Deferred.Result.Let_syntax in
  let milliseconds = Time_ns.Span.to_ms span in
  (* rounded to nearest millisecond *)
  let expire = Printf.sprintf "%.0f" milliseconds in
  match%bind request t ["PEXPIRE"; key; expire] with
  | Resp.Integer n -> return n
  | _ -> assert false

let expireat t key dt =
  let open Deferred.Result.Let_syntax in
  let since_epoch = dt |> Time_ns.to_span_since_epoch |> Time_ns.Span.to_ms in
  let expire = Printf.sprintf "%.0f" since_epoch in
  match%bind request t ["PEXPIREAT"; key; expire] with
  | Resp.Integer n -> return n
  | _ -> assert false

let keys t pattern =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["KEYS"; pattern] with
  | Resp.Array xs -> return (List.map ~f:Resp.get_bulk xs)
  | _ -> assert false

let sadd t ~key ?(members = []) member =
  let open Deferred.Result.Let_syntax in
  match%bind request t ("SADD" :: key :: member :: members) with
  | Resp.Integer n -> return n
  | _ -> assert false

let scard t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["SCARD"; key] with
  | Resp.Integer n -> return n
  | _ -> assert false

let generic_setop setop t ?(keys = []) key =
  let open Deferred.Result.Let_syntax in
  match%bind request t (setop :: key :: keys) with
  | Resp.Array res -> return (List.map ~f:Resp.get_bulk res)
  | _ -> assert false

let sdiff = generic_setop "SDIFF"

let generic_setop_store setop t ~destination ?(keys = []) ~key =
  let open Deferred.Result.Let_syntax in
  match%bind request t (setop :: destination :: key :: keys) with
  | Resp.Integer n -> return n
  | _ -> assert false

let sdiffstore = generic_setop_store "SDIFFSTORE"

let sinter = generic_setop "SINTER"

let sinterstore = generic_setop_store "SINTERSTORE"

let sismember t ~key member =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["SISMEMBER"; key; member] with
  | Resp.Integer 0 -> return false
  | Resp.Integer 1 -> return true
  | _ -> assert false

let smembers t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["SMEMBERS"; key] with
  | Resp.Array res -> return (List.map ~f:Resp.get_bulk res)
  | _ -> assert false

let smove t ~source ~destination member =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["SISMEMBER"; source; destination; member] with
  | Resp.Integer 0 -> return false
  | Resp.Integer 1 -> return true
  | _ -> assert false

let spop t ?(count = 1) key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["SPOP"; key; string_of_int count] with
  | Resp.Array res -> return (List.map ~f:Resp.get_bulk res)
  | _ -> assert false

let srandmember t ?(count = 1) key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["SRANDMEMBER"; key; string_of_int count] with
  | Resp.Array res -> return (List.map ~f:Resp.get_bulk res)
  | _ -> assert false

let srem t ~key ?(members = []) member =
  let open Deferred.Result.Let_syntax in
  match%bind request t ("SREM" :: key :: member :: members) with
  | Resp.Integer n -> return n
  | _ -> assert false

let sunion = generic_setop "SUNION"

let sunionstore = generic_setop_store "SUNIONSTORE"

let generic_scan t ?pattern ?count over =
  let pattern =
    match pattern with
    | Some pattern -> ["MATCH"; pattern]
    | None -> []
  in
  let count =
    match count with
    | Some count -> ["COUNT"; string_of_int count]
    | None -> []
  in
  Pipe.create_reader ~close_on_exception:false @@ fun writer ->
  Deferred.repeat_until_finished "0" @@ fun cursor ->
  match%bind request t (over @ [cursor] @ pattern @ count) with
  | Ok (Resp.Array [Resp.Bulk cursor; Resp.Array from]) -> (
      let from =
        from
        |> List.map ~f:(function
               | Resp.Bulk s -> s
               | _ -> failwith "unexpected")
        |> Queue.of_list
      in
      let%bind () = Pipe.transfer_in writer ~from in
      match cursor with
      | "0" -> return @@ `Finished ()
      | cursor -> return @@ `Repeat cursor)
  | _ -> failwith "unexpected"

let scan ?pattern ?count t = generic_scan t ?pattern ?count ["SCAN"]

let sscan t ?pattern ?count key = generic_scan t ?pattern ?count ["SSCAN"; key]

let hscan t ?pattern ?count key =
  let reader = generic_scan t ?pattern ?count ["HSCAN"; key] in
  Pipe.create_reader ~close_on_exception:true (fun writer ->
      let transfer_one_binding () =
        match%bind Pipe.read_exactly reader ~num_values:2 with
        | `Eof -> return @@ `Finished (Pipe.close writer)
        | `Fewer _ -> failwith "Unexpected protocol failure"
        | `Exactly q ->
            let field = Queue.get q 0 in
            let value = Queue.get q 1 in
            let binding = field, value in
            let%bind () = Pipe.write writer binding in
            return (`Repeat ())
      in
      Deferred.repeat_until_finished () transfer_one_binding)

let move t key db =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["MOVE"; key; string_of_int db] with
  | Resp.Integer 0 -> return false
  | Resp.Integer 1 -> return true
  | _ -> assert false

let persist t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["PERSIST"; key] with
  | Resp.Integer 0 -> return false
  | Resp.Integer 1 -> return true
  | _ -> assert false

let randomkey t =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["RANDOMKEY"] with
  | Resp.Bulk s -> return s
  | _ -> assert false

let rename t key newkey =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["RENAME"; key; newkey] with
  | Resp.String "OK" -> return ()
  | _ -> assert false

let renamenx t ~key newkey =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["RENAMENX"; key; newkey] with
  | Resp.Integer 0 -> return false
  | Resp.Integer 1 -> return true
  | _ -> assert false

let sort t ?by ?limit ?get ?(asc=true) ?alpha ?store key =
  let open Deferred.Result.Let_syntax in
  let by =
    match by with
    | None -> []
    | Some by -> ["BY"; by]
  in
  let limit =
    match limit with
    | None -> []
    | Some (offset, count) -> ["LIMIT"; string_of_int offset; string_of_int count]
  in
  let get =
    match get with
    | None -> []
    | Some patterns ->
        patterns |> List.map ~f:(fun pattern -> ["GET"; pattern]) |> List.concat
  in
  let order = if asc then ["ASC"] else ["DESC"] in
  let alpha =
    match alpha with
    | None -> []
    | Some false -> []
    | Some true -> ["ALPHA"]
  in
  let store =
    match store with
    | None -> []
    | Some destination -> ["STORE"; destination]
  in
  let q = [["SORT"; key]; by; limit; get; order; alpha; store] |> List.concat in
  match%bind request t q with
  | Resp.Integer count -> return @@ `Count count
  | Resp.Array sorted ->
    return (`Sorted (List.map sorted ~f:(function Resp.Bulk v -> v | _ -> assert false)))
  | _ -> assert false

let ttl t key =
  let open Deferred.Or_error.Let_syntax in
  match%bind request t ["PTTL"; key] with
  | Resp.Integer -2 -> Deferred.Or_error.errorf "No_such_key %s" key
  | Resp.Integer -1 -> Deferred.Or_error.errorf "Not_expiring %s" key
  | Resp.Integer ms -> return (Time_ns.Span.of_int_ms ms)
  | _ -> assert false

let typ t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["TYPE"; key] with
  | Resp.String "none" -> return None
  | Resp.String s -> return @@ Some s
  | _ -> assert false

let dump t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["DUMP"; key] with
  | Resp.Bulk bulk -> return @@ Some bulk
  | Resp.Null -> return None
  | _ -> assert false

let restore t ~key ?ttl ?replace value =
  let open Deferred.Result.Let_syntax in
  let ttl =
    match ttl with
    | None -> "0"
    | Some span -> span |> Time_ns.Span.to_ms |> Printf.sprintf ".0%f"
  in
  let replace =
    match replace with
    | Some true -> ["REPLACE"]
    | Some false | None -> []
  in
  match%bind request t (["RESTORE"; key; ttl; value] @ replace) with
  | Resp.String "OK" -> return ()
  | _ -> assert false

let lindex t key index =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["LINDEX"; key; string_of_int index] with
  | Resp.Bulk v -> return @@ Some v
  | Resp.Null -> return None
  | _ -> assert false

let string_of_position = function
  | `Before -> "BEFORE"
  | `After -> "AFTER"

let linsert t ~key position ~element ~pivot =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["LINSERT"; key; string_of_position position; pivot; element] with
  | Resp.Integer n -> return n
  | _ -> assert false

let llen t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["LLEN"; key] with
  | Resp.Integer n -> return n
  | _ -> assert false

let omnidirectional_pop command t key =
  request t [command; key] >>=? function
  | Resp.Bulk s -> Deferred.Or_error.return (Some s)
  | Resp.Null -> Deferred.Or_error.return None
  | Resp.Error e -> Deferred.Or_error.fail e
  | _ -> assert false

let rpop = omnidirectional_pop "RPOP"

let lpop = omnidirectional_pop "LPOP"

let hset t ~element ?(elements = []) key =
  let open Deferred.Result.Let_syntax in
  let field_values =
    element :: elements |> List.map ~f:(fun (f, v) -> [f; v]) |> List.concat
  in
  match%bind request t (["HSET"; key] @ field_values) with
  | Resp.Integer n -> return n
  | _ -> assert false

let hget t ~field key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["HGET"; key; field] with
  | Resp.Bulk v -> return v
  | _ -> assert false

let hmget t ~fields key =
  request t (["HMGET"; key] @ fields) >>=? function
  | Resp.Array xs -> begin
      let all_bindings =
        List.map2_exn fields xs ~f:(fun field -> function
            | Resp.Bulk v -> field, Some v
            | Resp.Null -> field, None
            | _ -> assert false) in
      let bound_bindings =
        List.filter_map all_bindings ~f:(fun (k, v) ->
            match v with
            | Some v -> Some (k, v)
            | None -> None)
      in
      Deferred.Or_error.return (String.Map.of_alist_exn bound_bindings)
    end
  | _ -> assert false

let hgetall t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["HGETALL"; key] with
  | Resp.Array xs -> (
      let kvs =
        List.chunks_of xs ~length:2 |>
        List.map ~f:(function
            | [Resp.Bulk key; Resp.Bulk value] -> Ok (key, value)
            | _ -> assert false)
        |> Result.all
      in
      let%bind kvs = Deferred.return kvs in
      return (String.Map.of_alist_exn kvs))
  | _ -> assert false

let hdel t ?(fields = []) ~field key =
  let open Deferred.Result.Let_syntax in
  match%bind request t (["HDEL"; key; field] @ fields) with
  | Resp.Integer n -> return n
  | _ -> assert false

let hexists t ~field key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["HEXISTS"; key; field] with
  | Resp.Integer 1 -> return true
  | Resp.Integer 0 -> return false
  | _ -> assert false

let hincrby t ~field key increment =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["HINCRBY"; key; field; string_of_int increment] with
  | Resp.Integer n -> return n
  | _ -> assert false

let hincrbyfloat t ~field key increment =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["HINCRBYFLOAT"; key; field; string_of_float increment] with
  | Resp.Bulk fl -> return @@ float_of_string fl
  | _ -> assert false

let generic_keyvals command t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t [command; key] with
  | Resp.Array xs ->
    let keys =
      List.map xs ~f:(function
          | Resp.Bulk x -> x
          | _ -> assert false)
    in
    Deferred.Or_error.return keys
  | _ -> assert false

let hkeys = generic_keyvals "HKEYS"

let hvals = generic_keyvals "HVALS"

let hlen t key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["HLEN"; key] with
  | Resp.Integer n -> return n
  | _ -> assert false

let hstrlen t ~field key =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["HSTRLEN"; key; field] with
  | Resp.Integer n -> return n
  | _ -> assert false

let publish t ~channel message =
  let open Deferred.Result.Let_syntax in
  match%bind request t ["PUBLISH"; channel; message] with
  | Resp.Integer n -> return n
  | _ -> assert false
