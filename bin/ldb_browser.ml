open Core

module DB = Tick.MakeLDB(LevelDB)

module Stats = struct
  type t = {
    nb_records: int;
    ts_start: Time_ns.t;
    ts_stop: Time_ns.t;
    avg_p: int;
    avg_v: int;
    stddev_v: float;
    avg_cluster_size: float;
    stddev_cluster_size: float;
  } [@@deriving sexp]

  let create
      ~nb_records ~ts_start ~ts_stop ~avg_p ~avg_v ~stddev_v
      ~avg_cluster_size ~stddev_cluster_size = {
    nb_records ; ts_start ; ts_stop ; avg_p ; avg_v ;
    stddev_v ; avg_cluster_size ; stddev_cluster_size ;
  }
end

module Display = struct
  type t =
    | Rows
    | Distrib
    | Stats

  let of_string = function
    | "rows" -> Rows
    | "distrib" -> Distrib
    | "stats" -> Stats
    | _ -> invalid_arg "Display.of_string"
end

let show tail max_ticks binsize (dbpath, show) () =
  let show = Display.of_string show in
  let nb_read = ref 0 in
  let sum_p = ref 0 in
  let sum_v = ref 0 in
  let avg_v = ref 0 in
  let var_v = ref 0 in
  let vdistrib = ref Int.Map.empty in
  let cur_cluster_size = ref 0 in
  let nb_clusters = ref 0 in
  let avg_cluster_size = ref 0. in
  let var_nb_clusters = ref 0. in
  let prev_side = ref `buy in
  let ts_start = ref Time_ns.max_value in
  let ts_stop = ref Time_ns.min_value in
  let iter_f a tick =
    ts_start := Time_ns.min !ts_start tick.Tick.ts;
    ts_stop := Time_ns.max !ts_stop tick.ts;
    if show = Rows then Format.printf "%d %a@." !nb_read Sexp.pp @@ Tick.sexp_of_t tick;
    let p = Int63.to_int_exn tick.p in
    let v = Int63.to_int_exn tick.v in
    incr nb_read;
    vdistrib := Int.Map.update !vdistrib (v / binsize) ~f:(function None -> 1 | Some n -> succ n);
    sum_p := !sum_p + p;
    sum_v := !sum_v + v;
    avg_v := !sum_v / !nb_read;
    if !nb_read > 0 then begin
      if !prev_side <> tick.side then begin
        incr nb_clusters;
        avg_cluster_size := !nb_read // !nb_clusters;
        var_nb_clusters := !var_nb_clusters +. (Float.of_int !cur_cluster_size -. !avg_cluster_size) ** 2.;
        cur_cluster_size := 0
      end
      else
        incr cur_cluster_size
    end;
    prev_side := tick.side;
    succ a
  in
  let nb_records_read = DB.with_db dbpath ~f:begin fun db ->
      Format.printf "DB has %d records.@." (DB.length db) ;
      let fold_f = if tail then DB.HL.fold_right else DB.HL.fold_left in
      fold_f db ~init:0 ~f:iter_f
    end in
  match show with
  | Rows -> Format.printf "Parsed %d records.@." nb_records_read
  | Distrib -> Int.Map.iteri !vdistrib ~f:(fun ~key ~data -> Format.printf "%d %d@." (key * binsize) data)
  | Stats ->
    let avg_p = !sum_p / !nb_read in
    let avg_v = !sum_v / !nb_read in
    let stddev_v = Float.(sqrt @@ of_int !var_v /. of_int !nb_read) in
    let stddev_cluster_size = Float.(sqrt @@ !var_nb_clusters /. of_int !nb_read) in
    let stats =
      Stats.create ~nb_records:!nb_read ~ts_start:!ts_start
        ~ts_stop:!ts_stop ~avg_p ~avg_v ~stddev_v
        ~avg_cluster_size:!avg_cluster_size ~stddev_cluster_size in
    if show = Stats then Format.printf "%a@." Sexp.pp @@ Stats.sexp_of_t stats

let create offset scid db () =
  let db = LevelDB.open_db db in
  let offset = Option.map offset ~f:(fun days ->
      Time_ns.(sub (now ()) @@ Span.of_day (Float.of_int days))
    )
  in
  let nb_records =
    Exn.protectx
      ~finally:LevelDB.close
      ~f:(fun db -> Tick.File.db_of_scid (module DB) ?offset db scid)
      db
  in
  Printf.printf "%d record written.\n" nb_records

let show =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-tail" no_arg ~doc:" Show latest records"
    +> flag "-n" (optional int) ~doc:"n Number of ticks to display (default: all)"
    +> flag "-binsize" (optional_with_default 1 int) ~doc:"int binsize for histograms"
    +> anon (t2 ("db" %: string) ("section" %: string))
  in
  Command.basic_spec ~summary:"Show LevelDB tick databases" spec show

let create =
  let spec =
    let open Command.Spec in
    empty
    +> flag "-offset" (optional int) ~doc:"n number of days in the past"
    +> anon ("scid" %: string)
    +> anon ("db" %: string)
  in
  Command.basic_spec ~summary:"Create LevelDB tick dbs from scid files" spec create

let command =
  Command.group
    ~summary:"Manipulate LevelDB tick databases" [
    "show", show;
    "create", create
  ]

let () = Command.run command
