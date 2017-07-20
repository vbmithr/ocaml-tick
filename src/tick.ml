open Core

type side = [
  | `buy
  | `sell
  | `buy_sell_unset
] [@@deriving sexp]

type t = {
  ts: Time_ns.t;
  side: side;
  p: Int63.t;
  v: Int63.t;
} [@@deriving sexp]

let create ~ts ~side ~p ~v () = { ts ; side ; p ; v }

let side_of_int64 = function
  | 0L -> `buy_sell_unset
  | 1L -> `buy
  | 2L -> `sell
  | _ -> invalid_arg "side_of_int64"

let int64_of_side = function
  | `buy_sell_unset -> 0L
  | `buy -> 1L
  | `sell -> 2L

let int64_of_v_side v side =
  let open Int64 in
  let side_i = int64_of_side side in
  let v = Int63.to_int64 v in
  bit_or (bit_and v (shift_left one 62 - one)) (shift_left side_i 62)

let v_side_of_int64 i =
  try
    let open Int64 in
    let side = side_of_int64 @@ shift_right_logical i 62 in
    let v = bit_and i (shift_left 1L 62 - 1L) in
    Int63.of_int64_exn v, side
  with _ -> invalid_arg @@ Printf.sprintf "%Lx\n%!" i

module type IO = sig
  type t
  val length : t -> int
  val get_int64_be : t -> int -> int64
  val set_int64_be : t -> int -> int64 -> unit
  val get_int64_le : t -> int -> int64
  val set_int64_le : t -> int -> int64 -> unit
end

module TickIO (IO: IO) = struct
  open IO

  let write ?(key_pos=0) ?(data_pos=0) ~buf_key ~buf_data { p; v; side; ts } =
    set_int64_be buf_key key_pos @@ Int63.to_int64 @@ Time_ns.to_int63_ns_since_epoch ts;
    set_int64_le buf_data data_pos @@ Int63.to_int64 p;
    set_int64_le buf_data (data_pos+8) @@ int64_of_v_side v side

  let read ?(pos=0) b =
    let ts = get_int64_be b pos |> Int63.of_int64_exn |> Time_ns.of_int63_ns_since_epoch in
    let p = Int63.of_int64_exn @@ get_int64_le b (pos+8) in
    let v = get_int64_le b (pos+16) in
    let v, side = v_side_of_int64 v in
    create ~ts ~p ~v ~side ()

  let read' ?(pos=0) ~ts ~data () =
    let p = Int63.of_int64_exn @@ get_int64_le data pos in
    let v = get_int64_le data (pos+8) in
    let v, side = v_side_of_int64 v in
    create ~ts ~p ~v ~side ()
end

module BytesIO = struct
  include String
  let get_int64_be buf pos = Binary_packing.unpack_signed_64_big_endian ~buf ~pos
  let set_int64_be buf pos = Binary_packing.pack_signed_64_big_endian ~buf ~pos
  let get_int64_le buf pos = Binary_packing.unpack_signed_64_little_endian ~buf ~pos
  let set_int64_le buf pos = Binary_packing.pack_signed_64_little_endian ~buf ~pos
end

module BigstringIO = struct
  include Bigstring
  let get_int64_be buf pos = unsafe_get_int64_t_be buf ~pos
  let set_int64_be buf pos = unsafe_set_int64_t_be buf ~pos
  let get_int64_le buf pos = unsafe_get_int64_t_le buf ~pos
  let set_int64_le buf pos = unsafe_set_int64_t_le buf ~pos
end

module B = Bytes

module Bytes = TickIO(BytesIO)
module Bigstring = TickIO(BigstringIO)

let hdr_size = 16
let size = 24
let version = 1

let hdr =
  let b = String.create 16 in
  String.blit "TICK" 0 b 0 4;
  Binary_packing.pack_unsigned_16_little_endian b 4 hdr_size;
  Binary_packing.pack_unsigned_16_little_endian b 6 size;
  Binary_packing.pack_unsigned_16_little_endian b 8 version;
  b

let of_scid_record r =
  let ts = (r.Scid.R.datetime -. 25569.) *.  86400. *. 1e9 in (* unix ts in nanoseconds *)
  create
    ~ts:(ts |> Int63.of_float |> Time_ns.of_int63_ns_since_epoch)
    ~p:(Int63.of_float @@ r.Scid.R.c *. 1e8)
    ~v:(Int63.(of_int64_exn r.Scid.R.total_volume * of_int 10_000))
    ~side:(if r.Scid.R.bid_volume = 0L then `buy else `sell) ()

let key = B.create 8
let data = B.create 16

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
  module Batch : sig
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
    val fill_key : iterator -> string ref -> int
    val fill_value : iterator -> string ref -> int
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

module MakeLDB(DB : LDB) = struct
  include DB

  let with_db path ~f =
    let db = open_db path in
    Exn.protect ~finally:(fun () -> close db) ~f:(fun () -> f db)

  let length db =
    let cnt = ref 0 in
    iter (fun _ _ -> incr cnt; true) db;
    !cnt

  let bounds db =
    if length db = 0 then None
    else
      let min_elt = String.create size in
      let max_elt = String.create size in
      iter (fun k v ->
          String.blit k 0 min_elt 0 8;
          String.blit v 0 min_elt 8 16;
          false) db;
      rev_iter (fun k v ->
          String.blit k 0 max_elt 0 8;
          String.blit v 0 min_elt 8 16;
          false) db;
      Some (Bytes.read min_elt,
            Bytes.read max_elt)

  let write_key buf ts =
    Time_ns.to_int_ns_since_epoch ts |> Int.to_int64 |>
    Binary_packing.pack_signed_64_big_endian ~buf ~pos:0

  let mem_tick db t =
    write_key key t.ts ;
    mem db key

  let put_tick ?sync db t =
    Bytes.write ~buf_key:key ~buf_data:data t;
    put ?sync db key data

  let put_tick_batch batch t =
    Bytes.write ~buf_key:key ~buf_data:data t;
    Batch.put batch key data

  let put_ticks ?sync db ts =
    let batch = Batch.make () in
    List.iter ts ~f:begin fun t ->
      Bytes.write ~buf_key:key ~buf_data:data t;
      Batch.put batch key data
    end;
    Batch.write ?sync db batch

  let ts_of_int64 =
    Fn.compose Time_ns.of_int63_ns_since_epoch Int63.of_int64_exn

  let get_ts buf =
    Binary_packing.unpack_signed_64_big_endian ~buf ~pos:0 |>
    Int63.of_int64_exn |>
    Time_ns.of_int63_ns_since_epoch

  let get_tick db ts =
    let open Option.Monad_infix in
    Binary_packing.pack_signed_64_big_endian key 0 ts;
    get db key >>| fun data ->
    let ts = ts_of_int64 ts in
    Bytes.read' ~ts ~data ()

  module HL = struct
    let iter ?(start=Time_ns.epoch) ?(stop=Time_ns.max_value) db ~f =
      let start_key = B.create 8 in
      write_key start_key start ;
      iter_from begin fun k v ->
        let ts = get_ts k in
        let tick = Bytes.read' ~ts ~data:v () in
        f tick ;
        Time_ns.(ts < stop)
      end db start_key

    let rev_iter ?(start=Time_ns.max_value) ?(stop=Time_ns.epoch) db ~f =
      let start_key = B.create 8 in
      write_key start_key start ;
      rev_iter_from begin fun k v ->
        let ts = get_ts k in
        let tick = Bytes.read' ~ts ~data:v () in
        f tick ;
        Time_ns.(ts > stop)
      end db start_key

    let fold_left ?start ?stop db ~init ~f =
      let acc = ref init in
      iter ?start ?stop db ~f:(fun tick -> acc := f !acc tick) ;
      !acc

    let fold_right ?start ?stop db ~init ~f =
      let acc = ref init in
      rev_iter ?start ?stop db ~f:(fun tick -> acc := f !acc tick) ;
      !acc
  end
end

module File = struct
  let of_scid ic oc =
    let open Scid in
    let buf = B.create size in
    let d = D.make @@ Channel ic in
    let rec loop nb_records = match D.decode d with
      | R r ->
        let t = of_scid_record r in
        Bytes.write ~buf_key:key ~buf_data:data t;
        Out_channel.output oc buf 0 size;
        loop (succ nb_records)
      | End -> nb_records
      | Error e -> failwith (Format.asprintf "%a" D.pp_error e)
      | Await -> failwith "Await"
    in
    Out_channel.output_string oc hdr;
    loop 0

  let bounds ic =
    let buf = B.create size in
    let nb_read = In_channel.input ic ~buf ~pos:0 ~len:hdr_size in
    if nb_read <> hdr_size || String.sub buf 0 hdr_size <> hdr
    then Error (Invalid_argument
                  (Printf.sprintf "corrupted header: read %S"
                     (String.sub buf 0 nb_read)))
    else
      let nb_read_fst = In_channel.input ic ~buf ~pos:0 ~len:size in
      let min_elt = Bytes.read buf in
      In_channel.(Int64.(seek ic @@ length ic - of_int size));
      let nb_read_snd = In_channel.input ic ~buf ~pos:0 ~len:size in
      let max_elt = Bytes.read buf in
      if nb_read_fst + nb_read_snd <> 2 * size
      then Error End_of_file
      else (In_channel.seek ic 0L; Result.return (min_elt, max_elt))

  let of_db
      (type db)
      (ldb : (module LDB_WITH_TICK with type db = db))
      ?start_ts ?end_ts (db:db) oc =
    let module DB = (val ldb : LDB_WITH_TICK with type db = db) in
    let open Result in
    Out_channel.output_string oc hdr;
    let cnt = ref 0 in
    Result.of_option
      ~error:(Failure "bounds") (DB.bounds db) >>= fun (min_elt, max_elt) ->
    let nb_ticks = ref 0 in
    DB.iter (fun k v ->
        incr nb_ticks;
        Out_channel.output oc ~buf:k ~pos:0 ~len:8;
        Out_channel.output oc ~buf:v ~pos:0 ~len:16;
        true) db;
    return
      (if (!cnt = 2) then !nb_ticks, Some (min_elt, max_elt) else 0, None)

  let to_db
    (type db)
    (ldb : (module LDB_WITH_TICK with type db = db))
    ic (db:db) =
    let module DB = (val ldb : LDB_WITH_TICK with type db = db) in
    let open Result in
    bounds ic >>= fun (min_elt, max_elt) ->
    let buf = B.create size in
    let cnt = Int64.((In_channel.length ic - of_int hdr_size) / (of_int size) |> to_int_exn) in
    if cnt = 0 then
      Ok (0, None)
    else begin
      In_channel.seek ic 16L;
      let rec loop () =
        In_channel.really_input ic ~buf ~pos:0 ~len:size |> function
        | None -> Ok (cnt, Some (min_elt, max_elt))
        | Some () -> DB.put db (String.sub buf 0 8) (String.sub buf 8 16); loop ()
      in loop ()
    end

  let db_of_scid
    (type db)
    (ldb : (module LDB_WITH_TICK with type db = db))
    ?(offset=Time_ns.epoch) db fn =
    let module DB = (val ldb : LDB_WITH_TICK with type db = db) in
    let open Scid in
    let buf = B.create size in
    In_channel.with_file ~binary:true fn ~f:(fun ic ->
        let d = D.make @@ Channel ic in
        let rec loop nb_records = match D.decode d with
          | R r ->
            let t = of_scid_record r in
            if Time_ns.(t.ts > offset) then begin
              Bytes.write ~buf_key:key ~buf_data:data t;
              DB.put db (String.sub buf 0 8) (String.sub buf 8 16);
              loop @@ succ nb_records
            end
            else loop nb_records
          | End -> nb_records
          | Error e -> failwith (Format.asprintf "%a" D.pp_error e)
          | Await -> failwith "Await"
        in
        loop 0
      )
end
