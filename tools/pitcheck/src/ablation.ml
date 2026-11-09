type bar = { symbol : string; day : string; close : float; volume : float; knowable_at : int }

let window = 20
let tolerance = 1e-9

let series_of = function
  | "unrate" -> Some "UNRATE"
  | "cpi" | "cpi_yoy" -> Some "CPIAUCSL"
  | "fedfunds" -> Some "FEDFUNDS"
  | _ -> None

let owner (r : As_of.row) =
  match r.source_table with
  | Some "bars_1d" -> Some (Printf.sprintf "bar:%s/%s" r.entity r.source_ref)
  | Some "edgar_company_facts" -> Some ("filing:" ^ r.source_ref)
  | Some "fred_series_vintages" ->
      Option.map (fun s -> Printf.sprintf "release:%s/%s" s r.source_ref) (series_of r.feature)
  | _ -> None

let bar_id b = Printf.sprintf "bar:%s/%s" b.symbol b.day

let stddev_samp xs =
  let n = float_of_int (List.length xs) in
  let mean = List.fold_left ( +. ) 0.0 xs /. n in
  sqrt (List.fold_left (fun acc x -> acc +. ((x -. mean) ** 2.0)) 0.0 xs /. (n -. 1.0))

let mean xs = List.fold_left ( +. ) 0.0 xs /. float_of_int (List.length xs)

let slice a first last = List.init (last - first + 1) (fun k -> a.(first + k))

let prices bars =
  let a = Array.of_list (List.sort (fun x y -> compare x.day y.day) bars) in
  let found = Hashtbl.create 64 in
  Array.iteri
    (fun i b ->
      let knowable =
        List.fold_left (fun m x -> max m x.knowable_at) min_int (slice a (max 0 (i - window)) i)
      in
      let put feature value = Hashtbl.replace found (feature, b.day) (value, knowable) in
      put "close" b.close;
      if i >= 1 then put "return_1d" (log (b.close /. a.(i - 1).close));
      if i >= window then begin
        let returns =
          List.init window (fun k ->
              let j = i - window + 1 + k in
              log (a.(j).close /. a.(j - 1).close))
        in
        put "volatility_20d" (stddev_samp returns);
        put "dollar_volume_20d"
          (mean (List.map (fun x -> x.close *. x.volume) (slice a (i - window + 1) i)))
      end)
    a;
  found

let near a b = a = b || Float.abs (a -. b) <= tolerance *. Float.max (Float.abs a) (Float.abs b)

let check ~withheld ~bars (rows : As_of.row list) =
  let gone = Hashtbl.create 16 in
  List.iter (fun w -> Hashtbl.replace gone w ()) withheld;
  let problems = ref [] in
  let problem fmt = Printf.ksprintf (fun s -> problems := s :: !problems) fmt in
  List.iter
    (fun (r : As_of.row) ->
      match owner r with
      | Some o when Hashtbl.mem gone o ->
          problem "%s of %s for %s belongs to withheld %s" r.feature r.entity r.source_ref o
      | _ -> ())
    rows;
  List.iter (fun b -> if Hashtbl.mem gone (bar_id b) then problem "%s is still a bar" (bar_id b)) bars;
  let symbols = List.sort_uniq compare (List.map (fun b -> b.symbol) bars) in
  let checked = ref 0 in
  List.iter
    (fun symbol ->
      let expected = prices (List.filter (fun b -> b.symbol = symbol) bars) in
      let mine =
        List.filter
          (fun (r : As_of.row) -> r.entity = symbol && r.source_table = Some "bars_1d")
          rows
      in
      let features = List.sort_uniq compare (List.map (fun (r : As_of.row) -> r.feature) mine) in
      let seen = Hashtbl.create 64 in
      List.iter
        (fun (r : As_of.row) ->
          incr checked;
          Hashtbl.replace seen (r.feature, r.source_ref) ();
          match (Hashtbl.find_opt expected (r.feature, r.source_ref), r.value) with
          | None, _ -> problem "%s of %s for %s has no bars to come from" r.feature symbol r.source_ref
          | Some (value, knowable), Some v ->
              if not (near value v) then
                problem "%s of %s for %s is %.17g, the bars give %.17g" r.feature symbol
                  r.source_ref v value;
              if knowable <> r.knowable_at then
                problem "%s of %s for %s is knowable at %s, the bars at %s" r.feature symbol
                  r.source_ref (Instant.iso r.knowable_at) (Instant.iso knowable)
          | Some _, None -> problem "%s of %s for %s has no value" r.feature symbol r.source_ref)
        mine;
      Hashtbl.iter
        (fun (feature, day) _ ->
          if List.mem feature features && not (Hashtbl.mem seen (feature, day)) then
            problem "%s of %s for %s is missing" feature symbol day)
        expected)
    symbols;
  (!checked, List.rev !problems)
