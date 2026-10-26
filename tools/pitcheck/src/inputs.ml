type meta = {
  backtest_id : string;
  version : string;
  period_start : string;
  period_end : string;
  input_hash : string;
  universe : string list;
  global : string;
  decision_hour : int;
}

let read_lines path =
  In_channel.with_open_text path In_channel.input_all
  |> String.split_on_char '\n'
  |> List.filter (fun l -> String.trim l <> "")

let fields line = String.split_on_char '\t' line

let meta path =
  let pairs =
    read_lines path
    |> List.filter_map (fun l -> match fields l with [ k; v ] -> Some (k, v) | _ -> None)
  in
  let get k =
    match List.assoc_opt k pairs with
    | Some v -> v
    | None -> failwith (Printf.sprintf "%s has no %s" path k)
  in
  {
    backtest_id = get "backtest_id";
    version = get "version";
    period_start = get "period_start";
    period_end = get "period_end";
    input_hash = get "input_hash";
    universe = String.split_on_char ',' (get "universe");
    global = get "global_entity";
    decision_hour = int_of_string (get "decision_hour");
  }

let optional = function "" -> None | s -> Some s

let row line : As_of.row =
  match fields line with
  | [ entity; feature; event_time; knowable_at; value; source_table; source_ref ] ->
      {
        entity;
        feature;
        event_time = int_of_string event_time;
        knowable_at = int_of_string knowable_at;
        value = Option.map float_of_string (optional value);
        source_table = optional source_table;
        source_ref;
      }
  | _ -> failwith (Printf.sprintf "expected 7 tab-separated fields: %S" line)

let rows path = List.map row (read_lines path)

let decision_times m =
  let first = Instant.days_of_iso_date m.period_start in
  let last = Instant.days_of_iso_date m.period_end in
  List.init (last - first + 1) (fun i -> Instant.at_hour (first + i) m.decision_hour)
