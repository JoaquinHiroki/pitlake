type row = {
  entity : string;
  feature : string;
  event_time : int;
  knowable_at : int;
  value : float option;
  source_table : string option;
  source_ref : string;
}

type seen = { entity : string; at : int; feature : string; value : float; event_time : int }

(* The same order as the struct pit.as_of takes the running maximum of; None sorts first. *)
let rank (r : row) = (r.event_time, r.knowable_at, r.source_table, r.source_ref, r.value)

let latest_known rows times =
  let rows = List.sort (fun (a : row) b -> compare a.knowable_at b.knowable_at) rows in
  let rec go rows best = function
    | [] -> []
    | t :: later ->
        let rec take rows best =
          match rows with
          | (r : row) :: rest when r.knowable_at <= t ->
              let best =
                match best with Some b when compare (rank b) (rank r) >= 0 -> best | _ -> Some r
              in
              take rest best
          | _ -> (rows, best)
        in
        let rows, best = take rows best in
        (t, best) :: go rows best later
  in
  go rows None (List.sort compare times)

let observations ~universe ~global rows times =
  let features_of entity =
    List.filter (fun (r : row) -> r.entity = entity || r.entity = global) rows
    |> List.map (fun (r : row) -> r.feature)
    |> List.sort_uniq compare
  in
  List.concat_map
    (fun entity ->
      List.concat_map
        (fun feature ->
          let rows =
            List.filter
              (fun (r : row) ->
                r.feature = feature && (r.entity = entity || (r.entity = global && entity <> global)))
              rows
          in
          latest_known rows times
          |> List.filter_map (fun (at, best) ->
                 match best with
                 | Some ({ value = Some value; event_time; _ } : row) ->
                     Some { entity; at; feature; value; event_time }
                 | _ -> None))
        (features_of entity))
    universe

let payload seen =
  let entry s =
    Printf.sprintf "[%s,%s,%s,%s,%s]" (Pyjson.string_literal s.entity)
      (Pyjson.string_literal (Instant.iso s.at))
      (Pyjson.string_literal s.feature) (Pyjson.float_repr s.value)
      (Pyjson.string_literal (Instant.iso s.event_time))
  in
  seen
  |> List.map (fun s -> ((s.entity, Instant.iso s.at, s.feature), entry s))
  |> List.sort (fun (a, _) (b, _) -> compare a b)
  |> List.map snd |> String.concat "," |> Printf.sprintf "[%s]"

let fingerprint seen = Sha256.hex (payload seen)
