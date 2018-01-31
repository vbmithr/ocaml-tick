open Core

type side = [
  | `buy
  | `sell
  | `buy_sell_unset
]

type t = {
  ts: Time_ns.t;
  side: side;
  p: Int63.t;
  v: Int63.t;
} [@@deriving sexp]

val create : ts:Time_ns.t -> side:side -> p:Int63.t -> v:Int63.t -> unit -> t

module TickBytes : sig
  val write : ?key_pos:int -> ?data_pos:int -> buf_key:Bytes.t -> buf_data:Bytes.t -> t -> unit
  val read : ?pos:int -> Bytes.t -> t
  val read' : ?pos:int -> ts:Time_ns.t -> data:Bytes.t -> unit -> t
end

module TickBigstring : sig
  val write : ?key_pos:int -> ?data_pos:int -> buf_key:Bigstring.t -> buf_data:Bigstring.t -> t -> unit
  val read : ?pos:int -> Bigstring.t -> t
  val read' : ?pos:int -> ts:Time_ns.t -> data:Bigstring.t -> unit -> t
end

val hdr : string
val hdr_size : int
val size : int
val version : int

val of_scid_record : Scid.R.t -> t

module type LDB = sig
  exception Error of string
  type db
  type iterator
  type writebatch
  type comparator
  type env
  val destroy : string -> bool
  val repair : string -> bool
  val default_env : env
  val lexicographic_comparator : comparator
  val open_db :
    ?write_buffer_size:int ->
    ?max_open_files:int ->
    ?block_size:int ->
    ?block_restart_interval:int ->
    ?comparator:comparator -> ?cache_size:int -> ?env:env -> string -> db
  val close : db -> unit
  val get : db -> string -> string option
  val get_exn : db -> string -> string
  val mem : db -> string -> bool
  val put : db -> ?sync:bool -> string -> string -> unit
  val delete : db -> ?sync:bool -> string -> unit
  val iter : (string -> string -> bool) -> db -> unit
  val rev_iter : (string -> string -> bool) -> db -> unit
  val iter_from : (string -> string -> bool) -> db -> string -> unit
  val rev_iter_from : (string -> string -> bool) -> db -> string -> unit
  module Batch :
  sig
    val make : unit -> writebatch
    val put : writebatch -> string -> string -> unit
    val put_substring :
      writebatch -> string -> int -> int -> string -> int -> int -> unit
    val delete : writebatch -> string -> unit
    val delete_substring : writebatch -> string -> int -> int -> unit
    val write : db -> ?sync:bool -> writebatch -> unit
  end
  module Iterator : sig
    val make : ?fill_cache:bool -> db -> iterator
    val close : iterator -> unit
    val seek_to_first : iterator -> unit
    val seek_to_last : iterator -> unit
    val seek: iterator -> string -> int -> int -> unit
    val next : iterator -> unit
    val prev : iterator -> unit
    val valid : iterator -> bool
    val fill_key : iterator -> Bytes.t ref -> int
    val fill_value : iterator -> Bytes.t ref -> int
    val get_key : iterator -> string
    val get_value : iterator -> string
    val iter : (string -> string -> bool) -> iterator -> unit
    val rev_iter : (string -> string -> bool) -> iterator -> unit
    val iter_from : (string -> string -> bool) -> iterator -> string -> unit
    val rev_iter_from : (string -> string -> bool) -> iterator -> string -> unit
  end
end

module type LDB_WITH_TICK = sig
  include LDB

  val with_db : string -> f:(db -> 'a) -> 'a
  val length : db -> int
  val bounds : db -> (t * t) option
  val mem_tick : db -> t -> bool
  val put_tick : ?sync:bool -> db -> t -> unit
  val put_tick_batch : writebatch -> t -> unit
  val put_ticks : ?sync:bool -> db -> t list -> unit
  val get_tick : db -> int64 -> t option

  module HL : sig
    val iter :
      ?start:Time_ns.t ->
      ?stop:Time_ns.t -> db ->
      f:(t -> unit) -> unit

    val rev_iter :
      ?start:Time_ns.t ->
      ?stop:Time_ns.t -> db ->
      f:(t -> unit) -> unit

    val fold_left :
      ?start:Time_ns.t ->
      ?stop:Time_ns.t -> db ->
      init:'a ->
      f:('a -> t -> 'a) -> 'a

    val fold_right :
      ?start:Time_ns.t ->
      ?stop:Time_ns.t -> db ->
      init:'a ->
      f:('a -> t -> 'a) -> 'a
  end
end

module MakeLDB(DB : LDB) : LDB_WITH_TICK with type db = DB.db

module File : sig
  (** Functions that operatate on tick files. *)

  (** Convenience functions *)

  val bounds : In_channel.t -> (t * t, exn) Result.t
  (** [bounds ic] is the first and the last tick found in [ic], if
      any. If there is no error, [In_channel.t] will be positioned at
      the beginning of the file. *)

  (** Conversion from/to the SCID file format. *)

  val of_scid : In_channel.t -> Out_channel.t -> int

  val db_of_scid :
    (module LDB_WITH_TICK with type db = 'a) ->
    ?offset:Time_ns.t -> 'a -> string -> int

  (** Conversion of tick files from and to LevelDB. *)

  val to_db :
    (module LDB_WITH_TICK with type db = 'a) ->
    In_channel.t -> 'a -> (int * (t * t) option, exn) Result.t

  val of_db :
    (module LDB_WITH_TICK with type db = 'a) ->
    ?start_ts:int64 -> ?end_ts:int64 -> 'a ->
    Out_channel.t -> (int * (t * t) option, exn) Result.t
end
