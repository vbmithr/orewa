open Core
open Async

let src = Logs.Src.create ~doc:"OCaml/Redis interface" "orewa"
module Log_async = (val Logs_async.src_log src : Logs_async.LOG)

module Resp = struct
  type t =
    | String of string
    | Error of Error.t
    | Int of int
    | Bulk of string option
    | Array of t list

  let string s = String s
  (* let error e = Error e *)
  let error_string s = Error (Error.of_string s)
  let int i = Int i
  let null = Bulk None
  let bulk s = Bulk (Some s)
  let bulk_of_int i = Bulk (Some (string_of_int i))
  let bulk_of_float i = Bulk (Some (string_of_float i))
  let array a = Array a

  (* let get_bulk = function
   *   | Bulk s -> s
   *   | _ -> invalid_arg "get_bulk" *)

  let string_or_error = function
    | String s -> Ok s
    | Error e -> Error e
    | _ -> assert false
  let bulk_or_error = function
    | Bulk s -> Ok s
    | Error e -> Error e
    | _ -> assert false
  let bulkarray_or_error = function
    | Array a -> Ok (List.map a ~f:(function Bulk a -> a | _ -> assert false))
    | Error e -> Error e
    | _ -> assert false
  let bulkarraynotnull_or_error = function
    | Array a -> Ok (List.map a ~f:(function Bulk Some a -> a | _ -> assert false))
    | Error e -> Error e
    | _ -> assert false
  (* let null_bulk_or_error = function
   *   | Bulk s -> Ok (Some s)
   *   | Null -> Ok None
   *   | Error e -> Error e
   *   | _ -> assert false *)
  let null_string_or_error = function
    | String s -> Ok (Some s)
    | Bulk None -> Ok None
    | Error e -> Error e
    | _ -> assert false
  let int_or_error = function
    | Int s -> Ok s
    | Error e -> Error e
    | _ -> assert false
  let bool_or_error = function
    | Int 1 -> Ok true
    | Int 0 -> Ok false
    | _ -> assert false
  let floatbulk_or_error = function
    | Bulk (Some s) -> Ok (float_of_string s)
    | Error e -> Error e
    | _ -> assert false

  let rec pp ppf = function
    | Error e   -> Format.fprintf ppf "-%a\r\n" Error.pp e
    | String s  -> Format.fprintf ppf "+%s\r\n" s
    | Int n     -> Format.fprintf ppf ":%d\r\n" n
    | Bulk None      -> Format.fprintf ppf "$-1\r\n"
    | Bulk (Some s)    -> Format.fprintf ppf "$%d\r\n%s\r\n" (String.length s) s
    | Array xs  -> Format.fprintf ppf "*%d\r\n%a" (List.length xs) (Format.pp_print_list ~pp_sep:(fun _ () -> ()) pp) xs

  let to_string t = Format.asprintf "%a" pp t

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

type _ typ =
  | Int : int typ
  | Bool : bool typ
  | Float : float typ
  | String : string typ
  | StringOrNull : string option typ
  | Bulk : string option typ
  | BulkNotNull : string typ
  | BulkArray : string option list typ
  | BulkArrayNotNull : string list typ

let int = Int
let bool = Bool
let float = Float
let string = String
let string_or_null = StringOrNull
let bulk = Bulk
let bulknotnull = BulkNotNull
let bulkarray = BulkArray
let bulkarraynotnull = BulkArrayNotNull

type sub_cmd =
  | Subscribe of string list
  | Unsubscribe of string list
  | PSubscribe of string
  | PUnsubscribe of string

let resp_of_sub_cmd sub =
  let open Resp in
  match sub with
  | Subscribe chns -> array (bulk "SUBSCRIBE" :: List.map ~f:Resp.bulk chns)
  | Unsubscribe chns -> array (bulk "UNSUBSCRIBE" :: List.map ~f:Resp.bulk chns)
  | PSubscribe pat -> array [bulk "PSUBSCRIBE"; bulk pat]
  | PUnsubscribe pat -> array [bulk "PUNSUBSCRIBE"; bulk pat]

type _ cmd =
  | Publish : string * string -> int cmd
  | Echo : string -> string cmd
  | Append : { key: string; value: string } -> int cmd
  | Bitcount : { key: string; range: (int * int) option } -> int cmd
  | Set : { key: string;
            value: string;
            expire: Time_ns.Span.t option;
            flag: [`IfExists|`IfNotExists] option } -> string option cmd
  | Get : string -> string option cmd
  | GetSet : string * string -> string option cmd
  | GetRange : string * int * int -> string cmd

  | Incr : string -> int cmd
  | IncrBy : string * int -> int cmd
  | IncrByFloat : string * float -> float cmd
  | Decr : string -> int cmd
  | DecrBy : string * int -> int cmd
  | StrLen : string -> int cmd
  | MGet : string list -> string option list cmd
  | MSet : (string * string) list -> string cmd
  | MSetNX : (string * string) list -> int cmd

  | HSet : string * (string * string) list -> int cmd
  | HGet : string * string -> string option cmd
  | HMGet : string * string list -> string option list cmd
  | HGetAll : string -> string list cmd
  | HDel : string * string list -> int cmd
  | HExists : string * string -> bool cmd
  | HIncrBy : string * string * int -> int cmd
  | HIncrByFloat : string * string * float -> float cmd
  | HKeys : string -> string list cmd
  | HVals : string -> string list cmd
  | HLen : string -> int cmd
  | HStrLen : string * string -> int cmd
  (* | Quit : string cmd *)

let resp_of_expire t =
  let open Time_ns.Span in
  if t > of_int_sec 1 then Printf.ksprintf Resp.bulk "EX %d" (to_int_sec t)
  else Printf.ksprintf Resp.bulk "PX %d" (to_int_ms t)

let resp_of_flag = function
  | `IfExists -> Resp.bulk "XX"
  | `IfNotExists -> Resp.bulk "NX"

let concat_tuple kvs =
  let open Resp in
  List.(concat (map kvs ~f:(fun (k, v) -> [bulk k; bulk v])))

let resp_of_cmd : type a. a cmd -> Resp.t = fun cmd ->
  let open Resp in
  match cmd with
  (* | Quit -> Array [bulk "QUIT"] *)
  | Echo msg -> array [bulk "ECHO"; bulk msg]
  | Publish (chn, msg) -> array [bulk "PUBLISH"; bulk chn; bulk msg]

  | Append { key; value } ->  array [bulk "APPEND"; bulk key; bulk value]
  | Bitcount { key; range } -> begin
      match range with
      | None -> array [bulk "BITCOUNT"; bulk key]
      | Some (start, stop) ->
        array [bulk "BITCOUNT"; bulk key; bulk_of_int start; bulk_of_int stop]
    end
  | Set { key; value; expire; flag } ->
    Resp.array (List.filter_opt [
        Some (Resp.bulk "SET"); Some (Resp.bulk key); Some (Resp.bulk value);
        Option.map ~f:resp_of_expire expire; Option.map ~f:resp_of_flag flag])
  | Get key -> array [bulk "GET"; bulk key]
  | MGet keys -> array ((bulk "MGET") :: List.map ~f:bulk keys)
  | GetSet (key, v) -> array [bulk "GETSET"; bulk key; bulk v]
  | GetRange (key, start, stop) ->
    array [bulk "GETRANGE"; bulk key; bulk_of_int start; bulk_of_int stop]
  | Incr key -> array [bulk "INCR"; bulk key]
  | IncrBy (key, v) -> array [bulk "INCRBY"; bulk key; bulk_of_int v]
  | IncrByFloat (key, v) -> array [bulk "INCRBY"; bulk key; bulk_of_float v]
  | Decr key -> array [bulk "DECR"; bulk key]
  | DecrBy (key, v) -> array [bulk "DECRBY"; bulk key; bulk_of_int v]
  | StrLen key -> array [bulk "STRLEN"; bulk key]
  | MSet kvs -> array (bulk "MSET" :: concat_tuple kvs)
  | MSetNX kvs -> array (bulk "MSETNX" :: concat_tuple kvs)
  | HSet (h, kvs) -> array (bulk "HSET" :: bulk h :: concat_tuple kvs)
  | HGet (h, k) -> array [bulk "HGET"; bulk h; bulk k]
  | HMGet (h, ks) -> array (bulk "HMGET" :: bulk h :: List.map ~f:bulk ks)
  | HGetAll k -> array [bulk "HGETALL"; bulk k]
  | HDel (h, ks) -> array (bulk "HDEL" :: bulk h :: List.map ~f:bulk ks)
  | HExists (h, k) -> array [bulk "HEXISTS"; bulk h; bulk k]
  | HIncrBy (h, k, v) -> array [bulk "HINCRBY"; bulk h; bulk k; bulk_of_int v]
  | HIncrByFloat (h, k, v) -> array [bulk "HINCRBYFLOAT"; bulk h; bulk k; bulk_of_float v]
  | HKeys h -> array [bulk "HKEYS"; bulk h]
  | HVals h -> array [bulk "HKEYS"; bulk h]
  | HLen h -> array [bulk "HLEN"; bulk h]
  | HStrLen (h, k) -> array [bulk "HSTRLEN"; bulk h; bulk k]

let connection_closed = Error.of_string "Connection closed"

type waiter = Packed_w : { ivar: 'a Or_error.t Ivar.t option; typ: 'a typ } -> waiter

type request = Packed_req : {
    cmd: 'a cmd;
    ivar: 'a Or_error.t Ivar.t option ;
    typ: 'a typ
  } -> request

let waiter : type a. a Or_error.t Ivar.t option -> a typ -> waiter = fun ivar typ ->
  Packed_w { ivar; typ }

let signal_error (Packed_w { ivar; _ }) e =
  Option.iter ivar ~f:(fun ivar -> Ivar.fill ivar (Error e))

module Sub = struct
  type pubsub =
    | Subscribe of { chn: string; nbSubscribed: int }
    | Unsubscribe of { chn: string; nbSubscribed: int }
    | Message of { chn: string; msg: string }
    | Pong of string option
    | Exit

  let pubsub_of_resp resp =
    let open Resp in
    match resp with
    | Array [Bulk Some "subscribe"; Bulk Some chn; Int nbSubscribed] ->
      Some (Subscribe { chn; nbSubscribed })
    | Array [Bulk Some "unsubscribe"; Bulk Some chn; Int nbSubscribed] ->
      Some (Unsubscribe { chn; nbSubscribed })
    | Array [Bulk Some "message"; Bulk Some chn; Bulk Some msg] ->
      Some (Message { chn; msg })
    | String "OK" -> Some Exit
    | String "PONG" -> Some (Pong None)
    | String s -> Some (Pong (Some s))
    | _ -> None

  module T = struct
    module Address = Uri_sexp
    type t = {
      writer  : sub_cmd Pipe.Writer.t ;
      pubsub  : pubsub Pipe.Reader.t ;
    }

    let is_closed { pubsub; _ } = Pipe.is_closed pubsub
    let close { writer; pubsub } =
      Pipe.close_read pubsub ;
      Pipe.close writer ;
      Deferred.unit
    let close_finished { pubsub; _ } = Pipe.closed pubsub
  end
  include T
  module Persistent = Persistent_connection_kernel.Make(T)

  let create r w =
    let pubsub = Pipe.create_reader ~close_on_exception:false begin fun w ->
        let rec recv_loop () =
          Monitor.try_with (fun () -> Resp.read r) >>= function
          | Error exn ->
            Log_async.err (fun m -> m "%a" Exn.pp exn) >>= fun () ->
            Deferred.unit
          | Ok `Eof -> Deferred.unit
          | Ok (`Ok msg) ->
            match pubsub_of_resp msg with
            | None -> assert false
            | Some msg ->
              Pipe.write w msg >>= fun () ->
              recv_loop () in
        recv_loop ()
      end in
    let request_writer =
      Pipe.create_writer begin fun pr ->
        Writer.transfer w pr begin fun cmd ->
          Writer.write w (Resp.to_string (resp_of_sub_cmd cmd))
        end
      end in
    { writer = request_writer; pubsub }

  let pubsub { pubsub; _ } = pubsub
  let dispatch_async ({ writer; _} : t) msg =
    Pipe.write writer msg

  let subscribe t chns = dispatch_async t (Subscribe chns)
  let unsubscribe t chns = dispatch_async t (Unsubscribe chns)
  let psubscribe t pat = dispatch_async t (PSubscribe pat)
  let punsubscribe t pat = dispatch_async t (PUnsubscribe pat)
end

module T = struct
  module Address = Uri_sexp
  type t = {
    r: Reader.t ;
    waiters : waiter Queue.t ;
    writer  : request Pipe.Writer.t ;
  }

  let close { writer; _ } =
    Pipe.close writer ;
    Deferred.unit
  let close_finished { r; _ } = Reader.close_finished r
  let is_closed { r; _ } = Reader.is_closed r
end
include T
module Persistent = Persistent_connection_kernel.Make(T)

let create r w =
  let waiters = Queue.create () in
  let rec recv_loop () =
    Monitor.try_with (fun () -> Resp.read r) >>= function
    | Error exn ->
      Log_async.err (fun m -> m "%a" Exn.pp exn) >>= fun () ->
      Deferred.unit
    | Ok `Eof -> Deferred.unit
    | Ok (`Ok msg) ->
      match Queue.dequeue waiters with
      | None when Reader.is_closed r -> Deferred.unit
      | None -> Format.kasprintf failwith "No waiters are waiting for this message: %a" Resp.pp msg
      | Some (Packed_w { ivar ; typ }) ->
        begin
          match ivar, typ with
          | None, _ -> assert false
          | Some ivar, Bool -> Ivar.fill ivar (Resp.bool_or_error msg)
          | Some ivar, Int -> Ivar.fill ivar (Resp.int_or_error msg)
          | Some ivar, Float -> Ivar.fill ivar (Resp.floatbulk_or_error msg)
          | Some ivar, String -> Ivar.fill ivar (Resp.string_or_error msg)
          | Some ivar, StringOrNull -> Ivar.fill ivar (Resp.null_string_or_error msg)
          | Some ivar, Bulk -> Ivar.fill ivar (Resp.bulk_or_error msg)
          | Some ivar, BulkNotNull -> Ivar.fill ivar (Or_error.map ~f:Caml.Option.get (Resp.bulk_or_error msg))
          | Some ivar, BulkArray -> Ivar.fill ivar (Resp.bulkarray_or_error msg)
          | Some ivar, BulkArrayNotNull -> Ivar.fill ivar (Resp.bulkarraynotnull_or_error msg)
        end ;
        recv_loop () in
  (* Requests are posted to a pipe, and requests are processed in sequence *)
  let handle_request (Packed_req { cmd; ivar; typ }) =
    Queue.enqueue waiters (waiter ivar typ) ;
    try Writer.write w (Resp.to_string (resp_of_cmd cmd)) with _ -> ()
  in
  let request_writer = Pipe.create_writer begin fun pr ->
      Pipe.iter_without_pushback pr ~f:handle_request >>= fun () ->
      Writer.close w >>= fun () ->
      Reader.close r >>= fun () ->
      (* Signal this to all waiters. As the pipe has been closed, we
         know that no new waiters will arrive *)
      Queue.iter waiters ~f:(fun waiter -> signal_error waiter connection_closed);
      return @@ Queue.clear waiters
    end in
  (* Start redis receiver. Processing ends if the connection is closed. *)
  (recv_loop () >>> fun () -> Pipe.close request_writer) ;
  (* Start processing requests. Once the pipe is closed, we signal
     closed to all outstanding waiters after closing the underlying
     socket *)
  { r; waiters; writer = request_writer }

let dispatch { writer; _ } typ cmd =
  let ivar = Ivar.create () in
  let req = Packed_req { ivar = Some ivar; typ; cmd } in
  Pipe.write writer req >>= fun () ->
  Ivar.read ivar

let publish t chn msg = dispatch t int (Publish (chn, msg))
let echo t msg = dispatch t bulknotnull (Echo msg)
let bitcount t ?range key = dispatch t int (Bitcount { key; range })
let append t key value = dispatch t int (Append { key; value })

let get t key = dispatch t bulk (Get key)
let mget t keys = dispatch t bulkarray (MGet keys)

let mset t ?(overwrite=false) kvs =
  match overwrite with
  | true -> dispatch t string (MSet kvs) >>|? fun _ -> true
  | false -> dispatch t int (MSetNX kvs) >>|? function 0 -> false | _ -> true

let set t ?expire ?flag key value =
  dispatch t string_or_null (Set { expire; flag; key; value }) >>|? function
  | Some _ -> true
  | None -> false

let getset t key v =
  dispatch t bulk (GetSet (key, v))

let getrange t key start stop =
  dispatch t bulknotnull (GetRange (key, start, stop))

let incr t ?(by=1) key =
  match by with
  | 1            -> dispatch t int (Incr key)
  | -1           -> dispatch t int (Decr key)
  | n when n > 0 -> dispatch t int (IncrBy (key, n))
  | _            -> dispatch t int (DecrBy (key, by))

let incrbyfloat t key v = dispatch t float (IncrByFloat (key, v))
let strlen t key = dispatch t int (StrLen key)
let hset t h kvs = dispatch t int (HSet (h, kvs))
let hget t h k = dispatch t bulk (HGet (h, k))
let hmget t h ks = dispatch t bulkarray (HMGet (h, ks))
let hgetall t h = dispatch t bulkarraynotnull (HGetAll h) >>|? fun kvs ->
  List.(map ~f:(function
      | [k; v] -> (k, v)
      | _ -> assert false) (chunks_of ~length:2 kvs))

let hdel t h ks = dispatch t int (HDel (h, ks))
let hexists t h k = dispatch t bool (HExists (h, k))
let hincrby t h k i = dispatch t int (HIncrBy (h, k, i))
let hincrbyfloat t h k f = dispatch t float (HIncrByFloat (h, k, f))
let hkeys t h = dispatch t bulkarraynotnull (HKeys h)
let hvals t h = dispatch t bulkarraynotnull (HVals h)
let hlen t h = dispatch t int (HLen h)
let hstrlen t h k = dispatch t int (HStrLen (h, k))

(* let omnidirectional_push command t ?(exist = `Always) ~element ?(elements = []) key =
 *   let command =
 *     match exist with
 *     | `Always -> command
 *     | `Only_if_exists -> Printf.sprintf "%sX" command
 *   in
 *   request_int t ([command; key; element] @ elements) *)

(* let lpush = omnidirectional_push "LPUSH"
 * let rpush = omnidirectional_push "RPUSH" *)

(* let lrange t ~key ~start ~stop =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["LRANGE"; key; string_of_int start; string_of_int stop] with
 *   | Resp.Array xs ->
 *       List.map xs ~f:(function
 *           | Resp.Bulk v -> Ok v
 *           | _ -> assert false)
 *       |> Result.all
 *       |> Deferred.return
 *   | _ -> assert false *)

(* let lrem t ~key count ~element = request_int t ["LREM"; key; string_of_int count; element] *)

(* let lset t ~key index ~element =
 *   Deferred.Or_error.ignore (request_string t ["LSET"; key; string_of_int index; element]) *)

(* let ltrim t ~start ~stop key =
 *   Deferred.Or_error.ignore (request_string t ["LTRIM"; key; string_of_int start; string_of_int stop]) *)

(* let rpoplpush t ~source ~destination = request_bulk t ["RPOPLPUSH"; source; destination]
 * let append t ~key value = request_int t ["APPEND"; key; value]
 * let auth t password = Deferred.Or_error.ignore (request_string t ["AUTH"; password]) *)

(* the documentation says it returns OK, but that's not true *)
(* let bgrewriteaof t = request_string t ["BGREWRITEAOF"]
 * let bgsave t = request_string t ["BGSAVE"] *)

(* type overflow =
 *   | Wrap
 *   | Sat
 *   | Fail
 * 
 * let string_of_overflow = function
 *   | Wrap -> "WRAP"
 *   | Sat -> "SAT"
 *   | Fail -> "FAIL" *)

(* (\* Declaration of type of the integer *\)
 * type intsize =
 *   | Signed of int
 *   | Unsigned of int *)

(* let string_of_intsize = function
 *   | Signed v -> Printf.sprintf "i%d" v
 *   | Unsigned v -> Printf.sprintf "u%d" v *)

(* type offset =
 *   | Absolute of int
 *   | Relative of int *)

(* let string_of_offset = function
 *   | Absolute v -> string_of_int v
 *   | Relative v -> Printf.sprintf "#%d" v *)

(* type fieldop =
 *   | Get of intsize * offset
 *   | Set of intsize * offset * int
 *   | Incrby of intsize * offset * int *)

(* let bitfield t ?overflow key ops =
 *   let open Deferred.Result.Let_syntax in
 *   let ops =
 *     List.map ops ~f:begin function
 *       | Get (size, offset) -> ["GET"; string_of_intsize size; string_of_offset offset]
 *       | Set (size, offset, value) ->
 *         [ "SET";
 *           string_of_intsize size;
 *           string_of_offset offset;
 *           string_of_int value ]
 *       | Incrby (size, offset, increment) ->
 *         [ "INCRBY";
 *           string_of_intsize size;
 *           string_of_offset offset;
 *           string_of_int increment ]
 *     end |> List.concat
 *   in
 *   let overflow =
 *     match overflow with
 *     | None -> []
 *     | Some behaviour -> ["OVERFLOW"; string_of_overflow behaviour]
 *   in
 *   match%bind request t (["BITFIELD"; key] @ overflow @ ops) with
 *   | Resp.Array xs ->
 *       return @@ List.fold_right xs ~init:[] ~f:begin fun v acc ->
 *         match v with
 *         | Resp.Integer i -> Some i :: acc
 *         | Resp.Null -> None :: acc
 *         | _ -> assert false
 *       end
 *   | _ -> assert false *)

(* type bitop =
 *   | AND
 *   | OR
 *   | XOR
 *   | NOT *)

(* let string_of_bitop = function
 *   | AND -> "AND"
 *   | OR -> "OR"
 *   | XOR -> "XOR"
 *   | NOT -> "NOT" *)

(* let bitop t ~destkey ?(keys = []) ~key op =
 *   request_int t (["BITOP"; string_of_bitop op; destkey; key] @ keys)
 * 
 * let string_of_bit = function
 *   | false -> "0"
 *   | true -> "1"
 * 
 * let bitpos t ?start ?stop key bit =
 *   let open Deferred.Result.Let_syntax in
 *   let%bind range =
 *     match start, stop with
 *     | Some s, Some e -> return [string_of_int s; string_of_int e]
 *     | Some s, None -> return [string_of_int s]
 *     | None, None -> return []
 *     | None, Some _ -> invalid_arg "Can't specify end without start"
 *   in
 *   match%bind request t (["BITPOS"; key; string_of_bit bit] @ range) with
 *   | Resp.Integer -1 -> return None
 *   | Resp.Integer n -> return @@ Some n
 *   | _ -> assert false *)

(* let getbit t key offset =
 *   let open Deferred.Result.Let_syntax in
 *   let offset = string_of_int offset in
 *   match%bind request t ["GETBIT"; key; offset] with
 *   | Resp.Integer 0 -> return false
 *   | Resp.Integer 1 -> return true
 *   | _ -> assert false
 * 
 * let setbit t key offset value =
 *   let open Deferred.Result.Let_syntax in
 *   let offset = string_of_int offset in
 *   let value = string_of_bit value in
 *   match%bind request t ["SETBIT"; key; offset; value] with
 *   | Resp.Integer 0 -> return false
 *   | Resp.Integer 1 -> return true
 *   | _ -> assert false *)

(* let select t index =
 *   Deferred.Or_error.ignore @@
 *     request_string t ["SELECT"; string_of_int index]
 * 
 * let del t ?(keys = []) key =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t (["DEL"; key] @ keys) with
 *   | Resp.Integer n -> return n
 *   | _ -> assert false
 * 
 * let exists t ?(keys = []) key =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t (["EXISTS"; key] @ keys) with
 *   | Resp.Integer n -> return n
 *   | _ -> assert false
 * 
 * let expire t key span =
 *   let open Deferred.Result.Let_syntax in
 *   let milliseconds = Time_ns.Span.to_ms span in
 *   (\* rounded to nearest millisecond *\)
 *   let expire = Printf.sprintf "%.0f" milliseconds in
 *   match%bind request t ["PEXPIRE"; key; expire] with
 *   | Resp.Integer n -> return n
 *   | _ -> assert false
 * 
 * let expireat t key dt =
 *   let open Deferred.Result.Let_syntax in
 *   let since_epoch = dt |> Time_ns.to_span_since_epoch |> Time_ns.Span.to_ms in
 *   let expire = Printf.sprintf "%.0f" since_epoch in
 *   match%bind request t ["PEXPIREAT"; key; expire] with
 *   | Resp.Integer n -> return n
 *   | _ -> assert false
 * 
 * let keys t pattern =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["KEYS"; pattern] with
 *   | Resp.Array xs -> return (List.map ~f:Resp.get_bulk xs)
 *   | _ -> assert false
 * 
 * let sadd t ~key ?(members = []) member =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ("SADD" :: key :: member :: members) with
 *   | Resp.Integer n -> return n
 *   | _ -> assert false
 * 
 * let scard t key =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["SCARD"; key] with
 *   | Resp.Integer n -> return n
 *   | _ -> assert false
 * 
 * let generic_setop setop t ?(keys = []) key =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t (setop :: key :: keys) with
 *   | Resp.Array res -> return (List.map ~f:Resp.get_bulk res)
 *   | _ -> assert false
 * 
 * let sdiff = generic_setop "SDIFF"
 * 
 * let generic_setop_store setop t ~destination ?(keys = []) ~key =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t (setop :: destination :: key :: keys) with
 *   | Resp.Integer n -> return n
 *   | _ -> assert false
 * 
 * let sdiffstore = generic_setop_store "SDIFFSTORE"
 * 
 * let sinter = generic_setop "SINTER"
 * 
 * let sinterstore = generic_setop_store "SINTERSTORE"
 * 
 * let sismember t ~key member =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["SISMEMBER"; key; member] with
 *   | Resp.Integer 0 -> return false
 *   | Resp.Integer 1 -> return true
 *   | _ -> assert false
 * 
 * let smembers t key =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["SMEMBERS"; key] with
 *   | Resp.Array res -> return (List.map ~f:Resp.get_bulk res)
 *   | _ -> assert false
 * 
 * let smove t ~source ~destination member =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["SISMEMBER"; source; destination; member] with
 *   | Resp.Integer 0 -> return false
 *   | Resp.Integer 1 -> return true
 *   | _ -> assert false
 * 
 * let spop t ?(count = 1) key =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["SPOP"; key; string_of_int count] with
 *   | Resp.Array res -> return (List.map ~f:Resp.get_bulk res)
 *   | _ -> assert false
 * 
 * let srandmember t ?(count = 1) key =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["SRANDMEMBER"; key; string_of_int count] with
 *   | Resp.Array res -> return (List.map ~f:Resp.get_bulk res)
 *   | _ -> assert false
 * 
 * let srem t ~key ?(members = []) member =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ("SREM" :: key :: member :: members) with
 *   | Resp.Integer n -> return n
 *   | _ -> assert false
 * 
 * let sunion = generic_setop "SUNION"
 * 
 * let sunionstore = generic_setop_store "SUNIONSTORE"
 * 
 * let generic_scan t ?pattern ?count over =
 *   let pattern =
 *     match pattern with
 *     | Some pattern -> ["MATCH"; pattern]
 *     | None -> []
 *   in
 *   let count =
 *     match count with
 *     | Some count -> ["COUNT"; string_of_int count]
 *     | None -> []
 *   in
 *   Pipe.create_reader ~close_on_exception:false @@ fun writer ->
 *   Deferred.repeat_until_finished "0" @@ fun cursor ->
 *   match%bind request t (over @ [cursor] @ pattern @ count) with
 *   | Ok (Resp.Array [Resp.Bulk cursor; Resp.Array from]) -> (
 *       let from =
 *         from
 *         |> List.map ~f:(function
 *                | Resp.Bulk s -> s
 *                | _ -> failwith "unexpected")
 *         |> Queue.of_list
 *       in
 *       let%bind () = Pipe.transfer_in writer ~from in
 *       match cursor with
 *       | "0" -> return @@ `Finished ()
 *       | cursor -> return @@ `Repeat cursor)
 *   | _ -> failwith "unexpected"
 * 
 * let scan ?pattern ?count t = generic_scan t ?pattern ?count ["SCAN"]
 * 
 * let sscan t ?pattern ?count key = generic_scan t ?pattern ?count ["SSCAN"; key]
 * 
 * let hscan t ?pattern ?count key =
 *   let reader = generic_scan t ?pattern ?count ["HSCAN"; key] in
 *   Pipe.create_reader ~close_on_exception:true (fun writer ->
 *       let transfer_one_binding () =
 *         match%bind Pipe.read_exactly reader ~num_values:2 with
 *         | `Eof -> return @@ `Finished (Pipe.close writer)
 *         | `Fewer _ -> failwith "Unexpected protocol failure"
 *         | `Exactly q ->
 *             let field = Queue.get q 0 in
 *             let value = Queue.get q 1 in
 *             let binding = field, value in
 *             let%bind () = Pipe.write writer binding in
 *             return (`Repeat ())
 *       in
 *       Deferred.repeat_until_finished () transfer_one_binding)
 * 
 * let move t key db =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["MOVE"; key; string_of_int db] with
 *   | Resp.Integer 0 -> return false
 *   | Resp.Integer 1 -> return true
 *   | _ -> assert false
 * 
 * let persist t key =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["PERSIST"; key] with
 *   | Resp.Integer 0 -> return false
 *   | Resp.Integer 1 -> return true
 *   | _ -> assert false
 * 
 * let randomkey t =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["RANDOMKEY"] with
 *   | Resp.Bulk s -> return s
 *   | _ -> assert false
 * 
 * let rename t key newkey =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["RENAME"; key; newkey] with
 *   | Resp.String "OK" -> return ()
 *   | _ -> assert false
 * 
 * let renamenx t ~key newkey =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["RENAMENX"; key; newkey] with
 *   | Resp.Integer 0 -> return false
 *   | Resp.Integer 1 -> return true
 *   | _ -> assert false
 * 
 * let sort t ?by ?limit ?get ?(asc=true) ?alpha ?store key =
 *   let open Deferred.Result.Let_syntax in
 *   let by =
 *     match by with
 *     | None -> []
 *     | Some by -> ["BY"; by]
 *   in
 *   let limit =
 *     match limit with
 *     | None -> []
 *     | Some (offset, count) -> ["LIMIT"; string_of_int offset; string_of_int count]
 *   in
 *   let get =
 *     match get with
 *     | None -> []
 *     | Some patterns ->
 *         patterns |> List.map ~f:(fun pattern -> ["GET"; pattern]) |> List.concat
 *   in
 *   let order = if asc then ["ASC"] else ["DESC"] in
 *   let alpha =
 *     match alpha with
 *     | None -> []
 *     | Some false -> []
 *     | Some true -> ["ALPHA"]
 *   in
 *   let store =
 *     match store with
 *     | None -> []
 *     | Some destination -> ["STORE"; destination]
 *   in
 *   let q = [["SORT"; key]; by; limit; get; order; alpha; store] |> List.concat in
 *   match%bind request t q with
 *   | Resp.Integer count -> return @@ `Count count
 *   | Resp.Array sorted ->
 *     return (`Sorted (List.map sorted ~f:(function Resp.Bulk v -> v | _ -> assert false)))
 *   | _ -> assert false
 * 
 * let ttl t key =
 *   let open Deferred.Or_error.Let_syntax in
 *   match%bind request t ["PTTL"; key] with
 *   | Resp.Integer -2 -> Deferred.Or_error.errorf "No_such_key %s" key
 *   | Resp.Integer -1 -> Deferred.Or_error.errorf "Not_expiring %s" key
 *   | Resp.Integer ms -> return (Time_ns.Span.of_int_ms ms)
 *   | _ -> assert false
 * 
 * let typ t key =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["TYPE"; key] with
 *   | Resp.String "none" -> return None
 *   | Resp.String s -> return @@ Some s
 *   | _ -> assert false
 * 
 * let dump t key =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["DUMP"; key] with
 *   | Resp.Bulk bulk -> return @@ Some bulk
 *   | Resp.Null -> return None
 *   | _ -> assert false
 * 
 * let restore t ~key ?ttl ?replace value =
 *   let open Deferred.Result.Let_syntax in
 *   let ttl =
 *     match ttl with
 *     | None -> "0"
 *     | Some span -> span |> Time_ns.Span.to_ms |> Printf.sprintf ".0%f"
 *   in
 *   let replace =
 *     match replace with
 *     | Some true -> ["REPLACE"]
 *     | Some false | None -> []
 *   in
 *   match%bind request t (["RESTORE"; key; ttl; value] @ replace) with
 *   | Resp.String "OK" -> return ()
 *   | _ -> assert false
 * 
 * let lindex t key index =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["LINDEX"; key; string_of_int index] with
 *   | Resp.Bulk v -> return @@ Some v
 *   | Resp.Null -> return None
 *   | _ -> assert false
 * 
 * let string_of_position = function
 *   | `Before -> "BEFORE"
 *   | `After -> "AFTER"
 * 
 * let linsert t ~key position ~element ~pivot =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["LINSERT"; key; string_of_position position; pivot; element] with
 *   | Resp.Integer n -> return n
 *   | _ -> assert false
 * 
 * let llen t key =
 *   let open Deferred.Result.Let_syntax in
 *   match%bind request t ["LLEN"; key] with
 *   | Resp.Integer n -> return n
 *   | _ -> assert false *)

(* let omnidirectional_pop command t key =
 *   request t [command; key] >>=? function
 *   | Resp.Bulk s -> Deferred.Or_error.return (Some s)
 *   | Resp.Null -> Deferred.Or_error.return None
 *   | Resp.Error e -> Deferred.Or_error.fail e
 *   | _ -> assert false *)

(* let rpop = omnidirectional_pop "RPOP"
 * let lpop = omnidirectional_pop "LPOP" *)
