type question = { id : string; symbol : string; as_of : int; relevant : string list }
type chunk = { chunk_symbol : string; knowable_at : int }

type checked = {
  metrics : (string * float) list;
  future : int;
  other_company : int;
  missing : string list;
}

let top_k = 10
let candidates = 20

let rec take n = function x :: rest when n > 0 -> x :: take (n - 1) rest | _ -> []

let position pred items =
  let rec go n = function
    | [] -> None
    | x :: rest -> if pred x then Some n else go (n + 1) rest
  in
  go 1 items

let evaluate questions chunks ranked =
  let n = float_of_int (List.length questions) in
  let firsts, any, future, other, missing =
    List.fold_left
      (fun (firsts, any, future, other, missing) q ->
        match Hashtbl.find_opt ranked q.id with
        | None -> (firsts, any, future, other, ("question " ^ q.id) :: missing)
        | Some ids ->
            let known = List.filter_map (fun c -> Hashtbl.find_opt chunks c) ids in
            let missing =
              List.filter (fun c -> not (Hashtbl.mem chunks c)) ids
              |> List.map (fun c -> "chunk " ^ c)
              |> List.rev_append missing
            in
            let relevant c = List.mem c q.relevant in
            let late = List.filter (fun c -> c.knowable_at > q.as_of) known in
            let elsewhere = List.filter (fun c -> c.chunk_symbol <> q.symbol) known in
            ( position relevant (take top_k ids) :: firsts,
              (if List.exists relevant ids then any + 1 else any),
              future + List.length late,
              other + List.length elsewhere,
              missing ))
      ([], 0, 0, 0, []) questions
  in
  let hits k =
    float_of_int (List.length (List.filter (function Some p -> p <= k | None -> false) firsts))
    /. n
  in
  let reciprocal =
    List.fold_left (fun s -> function Some p -> s +. (1. /. float_of_int p) | None -> s) 0. firsts
  in
  let future_top =
    List.fold_left
      (fun s q ->
        match Hashtbl.find_opt ranked q.id with
        | None -> s
        | Some ids ->
            take top_k ids
            |> List.filter_map (Hashtbl.find_opt chunks)
            |> List.filter (fun c -> c.knowable_at > q.as_of)
            |> List.length |> ( + ) s)
      0 questions
  in
  {
    metrics =
      [
        ("hit_at_1", hits 1);
        ("hit_at_5", hits 5);
        ("hit_at_10", hits 10);
        (Printf.sprintf "mrr_at_%d" top_k, reciprocal /. n);
        ("future_chunks", float_of_int future_top);
        ("questions", n);
        (Printf.sprintf "hit_at_%d" candidates, float_of_int any /. n);
      ];
    future;
    other_company = other;
    missing = List.rev missing;
  }

let tolerance = 1e-9

let compare_metrics ~recorded ~rebuilt =
  List.map
    (fun (key, value) ->
      match List.assoc_opt key rebuilt with
      | None -> (key, value, None, false)
      | Some mine -> (key, value, Some mine, Float.abs (mine -. value) <= tolerance))
    recorded

let split_tabs line = String.split_on_char '\t' line

let read_lines path =
  In_channel.with_open_bin path In_channel.input_all
  |> String.split_on_char '\n'
  |> List.filter (fun l -> l <> "")

let questions_of lines =
  List.map
    (fun line ->
      match split_tabs line with
      | [ id; symbol; as_of; relevant ] ->
          {
            id;
            symbol;
            as_of = int_of_string as_of;
            relevant = List.filter (( <> ) "") (String.split_on_char ',' relevant);
          }
      | _ -> failwith ("bad question line: " ^ line))
    lines

let chunks_of lines =
  let table = Hashtbl.create 16384 in
  List.iter
    (fun line ->
      match split_tabs line with
      | [ id; chunk_symbol; knowable_at ] ->
          Hashtbl.replace table id { chunk_symbol; knowable_at = int_of_string knowable_at }
      | _ -> failwith ("bad chunk line: " ^ line))
    lines;
  table

let rerankings_of lines =
  let models = Hashtbl.create 2 in
  List.iter
    (fun line ->
      match split_tabs line with
      | [ question; model; ids ] ->
          let table =
            match Hashtbl.find_opt models model with
            | Some t -> t
            | None ->
                let t = Hashtbl.create 256 in
                Hashtbl.replace models model t;
                t
          in
          Hashtbl.replace table question (String.split_on_char ',' ids)
      | _ -> failwith ("bad reranking line: " ^ line))
    lines;
  models

let metrics_of lines =
  List.map
    (fun line ->
      match split_tabs line with
      | [ model; key; value ] -> (model, key, float_of_string value)
      | _ -> failwith ("bad metric line: " ^ line))
    lines

let meta_of lines =
  List.map
    (fun line ->
      match split_tabs line with
      | [ key; value ] -> (key, value)
      | _ -> failwith ("bad meta line: " ^ line))
    lines
