open Core
open Async

type t

type pubsub =
  | Subscribe   of { chn: string; nbSubscribed: int }
  | Unsubscribe of { chn: string; nbSubscribed: int }
  | Message     of { chn: string; msg: string }

val pubsub : t -> pubsub Pipe.Reader.t

val create : Reader.t -> Writer.t -> t

val publish      : t -> string -> string -> int Deferred.Or_error.t
val subscribe    : t -> string list -> unit Deferred.t
val unsubscribe  : t -> string list -> unit Deferred.t
val psubscribe   : t -> string -> unit Deferred.t
val punsubscribe : t -> string -> unit Deferred.t

val echo         : t -> string -> string Deferred.Or_error.t
val append       : t -> string -> string -> int Deferred.Or_error.t
val bitcount     : t -> ?range:(int * int) -> string -> int Deferred.Or_error.t

val set : t ->
  ?expire:Time_ns.Span.t -> ?flag:[`IfExists|`IfNotExists] ->
  string -> string -> bool Deferred.Or_error.t

val get : t -> string -> string option Deferred.Or_error.t

(* val getrange : t -> start:int -> stop:int -> string -> string Deferred.Or_error.t
 * val getset : t -> key:string -> string -> string option Deferred.Or_error.t
 * 
 * val strlen : t -> string -> int Deferred.Or_error.t
 * val mget : t -> string list -> string option list Deferred.Or_error.t
 * val mset : t -> (string * string) list -> unit Deferred.Or_error.t
 * val msetnx : t -> (string * string) list -> bool Deferred.Or_error.t
 * 
 * val lpush : t ->
 *   ?exist:[`Always | `Only_if_exists] ->
 *   element:string ->
 *   ?elements:string list ->
 *   string ->
 *   int Deferred.Or_error.t
 * 
 * val rpush : t ->
 *   ?exist:[`Always | `Only_if_exists] ->
 *   element:string ->
 *   ?elements:string list ->
 *   string ->
 *   int Deferred.Or_error.t
 * 
 * val lpop : t -> string -> string option Deferred.Or_error.t
 * val rpop : t -> string -> string option Deferred.Or_error.t
 * 
 * val lrange : t -> key:string -> start:int -> stop:int -> string list Deferred.Or_error.t
 * val rpoplpush : t -> source:string -> destination:string -> string Deferred.Or_error.t
 * 
 * val append : t -> key:string -> string -> int Deferred.Or_error.t
 * val auth : t -> string -> unit Deferred.Or_error.t
 * val bgrewriteaof : t -> string Deferred.Or_error.t
 * val bgsave : t -> string Deferred.Or_error.t
 * val bitcount : t -> ?range:int * int -> string -> int Deferred.Or_error.t
 * 
 * type overflow =
 *   | Wrap
 *   | Sat
 *   | Fail
 * 
 * type intsize =
 *   | Signed of int
 *   | Unsigned of int
 * 
 * type offset =
 *   | Absolute of int
 *   | Relative of int
 * 
 * type fieldop =
 *   | Get of intsize * offset
 *   | Set of intsize * offset * int
 *   | Incrby of intsize * offset * int
 * 
 * val bitfield : t ->
 *   ?overflow:overflow ->
 *   string ->
 *   fieldop list ->
 *   int option list Deferred.Or_error.t
 * 
 * type bitop =
 *   | AND
 *   | OR
 *   | XOR
 *   | NOT
 * 
 * val bitop : t -> destkey:string -> ?keys:string list -> key:string -> bitop -> int Deferred.Or_error.t
 * val bitpos : t -> ?start:int -> ?stop:int -> string -> bool -> int option Deferred.Or_error.t
 * val getbit : t -> string -> int -> bool Deferred.Or_error.t
 * val setbit : t -> string -> int -> bool -> bool Deferred.Or_error.t
 * 
 * val decr : t -> string -> int Deferred.Or_error.t
 * val decrby : t -> string -> int -> int Deferred.Or_error.t
 * val incr : t -> string -> int Deferred.Or_error.t
 * val incrby : t -> string -> int -> int Deferred.Or_error.t
 * val incrbyfloat : t -> string -> float -> float Deferred.Or_error.t
 * val select : t -> int -> unit Deferred.Or_error.t
 * val del : t -> ?keys:string list -> string -> int Deferred.Or_error.t
 * val exists : t -> ?keys:string list -> string -> int Deferred.Or_error.t
 * val expire : t -> string -> Time_ns.Span.t -> int Deferred.Or_error.t
 * val expireat : t -> string -> Time_ns.t -> int Deferred.Or_error.t
 * val keys : t -> string -> string list Deferred.Or_error.t
 * 
 * val sadd : t ->
 *   key:string ->
 *   ?members:string list ->
 *   string ->
 *   int Deferred.Or_error.t
 * 
 * val scan : ?pattern:string -> ?count:int -> t -> string Pipe.Reader.t
 * val scard : t -> string -> int Deferred.Or_error.t
 * 
 * val sdiff : t ->
 *   ?keys:string list ->
 *   string ->
 *   string list Deferred.Or_error.t
 * 
 * val sdiffstore : t ->
 *   destination:string ->
 *   ?keys:string list ->
 *   key:string ->
 *   int Deferred.Or_error.t
 * 
 * val sinter : t ->
 *   ?keys:string list ->
 *   string ->
 *   string list Deferred.Or_error.t
 * 
 * val sinterstore : t ->
 *   destination:string ->
 *   ?keys:string list ->
 *   key:string ->
 *   int Deferred.Or_error.t
 * 
 * val sismember : t -> key:string -> string -> bool Deferred.Or_error.t
 * 
 * val smembers : t -> string -> string list Deferred.Or_error.t
 * 
 * val smove : t ->
 *   source:string ->
 *   destination:string ->
 *   string ->
 *   bool Deferred.Or_error.t
 * 
 * val spop : t -> ?count:int -> string -> string list Deferred.Or_error.t
 * 
 * val srandmember : t ->
 *   ?count:int ->
 *   string ->
 *   string list Deferred.Or_error.t
 * 
 * val srem : t ->
 *   key:string ->
 *   ?members:string list ->
 *   string ->
 *   int Deferred.Or_error.t
 * 
 * val sunion : t ->
 *   ?keys:string list ->
 *   string ->
 *   string list Deferred.Or_error.t
 * 
 * val sunionstore : t ->
 *   destination:string ->
 *   ?keys:string list ->
 *   key:string ->
 *   int Deferred.Or_error.t
 * 
 * val sscan : t -> ?pattern:string -> ?count:int -> string -> string Pipe.Reader.t
 * val move : t -> string -> int -> bool Deferred.Or_error.t
 * val persist : t -> string -> bool Deferred.Or_error.t
 * val randomkey : t -> string Deferred.Or_error.t
 * val rename : t -> string -> string -> unit Deferred.Or_error.t
 * val renamenx : t -> key:string -> string -> bool Deferred.Or_error.t
 * 
 * val sort : t ->
 *   ?by:string ->
 *   ?limit:int * int ->
 *   ?get:string list ->
 *   ?asc:bool ->
 *   ?alpha:bool ->
 *   ?store:string ->
 *   string ->
 *   [> `Count of int | `Sorted of string list] Deferred.Or_error.t
 * 
 * val ttl : t -> string -> Time_ns.Span.t Deferred.Or_error.t
 * val typ : t -> string -> string option Deferred.Or_error.t
 * val dump : t -> string -> string option Deferred.Or_error.t
 * 
 * val restore : t ->
 *   key:string ->
 *   ?ttl:Time_ns.Span.t ->
 *   ?replace:bool ->
 *   string ->
 *   unit Deferred.Or_error.t
 * 
 * val lindex : t -> string -> int -> string option Deferred.Or_error.t
 * 
 * val linsert : t ->
 *   key:string ->
 *   [`Before|`After] ->
 *   element:string ->
 *   pivot:string ->
 *   int Deferred.Or_error.t
 * 
 * val llen : t -> string -> int Deferred.Or_error.t
 * val lrem : t -> key:string -> int -> element:string -> int Deferred.Or_error.t
 * val lset : t -> key:string -> int -> element:string -> unit Deferred.Or_error.t
 * val ltrim : t -> start:int -> stop:int -> string -> unit Deferred.Or_error.t
 * 
 * val hset : t ->
 *   element:string * string ->
 *   ?elements:(string * string) list ->
 *   string ->
 *   int Deferred.Or_error.t
 * 
 * val hget : t -> field:string -> string -> string Deferred.Or_error.t
 * val hmget : t -> fields:string list -> string -> string String.Map.t Deferred.Or_error.t
 * val hgetall : t -> string -> string String.Map.t Deferred.Or_error.t
 * 
 * val hdel :  t -> ?fields:string list -> field:string -> string -> int Deferred.Or_error.t
 * val hexists : t -> field:string -> string -> bool Deferred.Or_error.t
 * val hincrby : t -> field:string -> string -> int -> int Deferred.Or_error.t
 * val hincrbyfloat : t -> field:string -> string -> float -> float Deferred.Or_error.t
 * val hkeys : t -> string -> string list Deferred.Or_error.t
 * val hvals : t -> string -> string list Deferred.Or_error.t
 * val hlen : t -> string -> int Deferred.Or_error.t
 * val hstrlen : t -> field:string -> string -> int Deferred.Or_error.t
 * val hscan : t -> ?pattern:string -> ?count:int -> string -> (string * string) Pipe.Reader.t
 * 
 * val publish : t -> channel:string -> string -> int Deferred.Or_error.t *)
