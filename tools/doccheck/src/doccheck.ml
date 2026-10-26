type problem = { file : string; line : int; message : string }

type facts = {
  make_targets : string list;
  bundle_targets : string list;
  runnable : (string * string list) list;
  secrets : string list;
}

let indent s =
  let n = String.length s in
  let rec go i = if i < n && s.[i] = ' ' then go (i + 1) else i in
  go 0

let is_blank_or_comment s =
  let t = String.trim s in
  t = "" || t.[0] = '#'

let is_name_char = function
  | 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' | '-' -> true
  | _ -> false

let key_of line =
  let t = String.trim line in
  let n = String.length t in
  if n > 1 && t.[n - 1] = ':' && String.for_all is_name_char (String.sub t 0 (n - 1))
  then Some (String.sub t 0 (n - 1))
  else None

let blocks ~parent ~at lines =
  let add line = function
    | None -> None
    | Some (key, body) -> Some (key, line :: body)
  in
  let close current acc =
    match current with
    | None -> acc
    | Some (key, body) -> (key, List.rev body) :: acc
  in
  let rec find = function
    | [] -> []
    | l :: rest when indent l = at && String.trim l = parent ^ ":" -> gather None [] rest
    | _ :: rest -> find rest
  and gather current acc = function
    | [] -> List.rev (close current acc)
    | l :: rest when is_blank_or_comment l -> gather (add l current) acc rest
    | l :: _ when indent l <= at -> List.rev (close current acc)
    | l :: rest when indent l = at + 2 -> (
        match key_of l with
        | Some key -> gather (Some (key, [])) (close current acc) rest
        | None -> gather (add l current) acc rest)
    | l :: rest -> gather (add l current) acc rest
  in
  find lines

let list_names ~parent ~at lines =
  let prefix = "- name: " in
  let rec find = function
    | [] -> []
    | l :: rest when indent l = at && String.trim l = parent ^ ":" -> gather rest
    | _ :: rest -> find rest
  and gather = function
    | [] -> []
    | l :: _ when (not (is_blank_or_comment l)) && indent l <= at -> []
    | l :: rest ->
        let t = String.trim l in
        if indent l = at + 2 && String.starts_with ~prefix t then
          let p = String.length prefix in
          String.trim (String.sub t p (String.length t - p)) :: gather rest
        else gather rest
  in
  find lines

let make_targets lines =
  List.filter_map
    (fun line ->
      match String.index_opt line ':' with
      | Some i when i > 0 && String.for_all is_name_char (String.sub line 0 i) ->
          Some (String.sub line 0 i)
      | _ -> None)
    lines

let find_sub s sub =
  let n = String.length s and m = String.length sub in
  let rec go i =
    if i + m > n then None else if String.sub s i m = sub then Some i else go (i + 1)
  in
  go 0

let secret_names lines =
  let marker = "_SECRET = \"" in
  List.filter_map
    (fun line ->
      match find_sub line marker with
      | None -> None
      | Some i ->
          let start = i + String.length marker in
          Option.map
            (fun j -> String.sub line start (j - start))
            (String.index_from_opt line start '"'))
    lines

let tokens s =
  let buf = Buffer.create 16 in
  let out = ref [] in
  let quote = ref None in
  let flush () =
    if Buffer.length buf > 0 then begin
      out := Buffer.contents buf :: !out;
      Buffer.clear buf
    end
  in
  String.iter
    (fun c ->
      match (!quote, c) with
      | Some q, c when c = q -> quote := None
      | Some _, c -> Buffer.add_char buf c
      | None, ('\'' | '"') -> quote := Some c
      | None, (' ' | '\t') -> flush ()
      | None, c -> Buffer.add_char buf c)
    s;
  flush ();
  List.rev !out

let split_commands toks =
  let rec go current acc = function
    | [] -> List.rev (List.rev current :: acc)
    | ("&&" | "||" | ";" | "|") :: rest -> go [] (List.rev current :: acc) rest
    | t :: rest -> go (t :: current) acc rest
  in
  List.filter (fun c -> c <> []) (go [] [] toks)

type fence = Outside | Bash | Other

let commands lines =
  let rec go n fence pending acc = function
    | [] -> List.rev acc
    | l :: rest ->
        let t = String.trim l in
        if String.starts_with ~prefix:"```" t then
          let fence =
            match fence with
            | Outside -> if t = "```bash" then Bash else Other
            | Bash | Other -> Outside
          in
          go (n + 1) fence None acc rest
        else if fence <> Bash || (pending = None && String.starts_with ~prefix:"#" t) then
          go (n + 1) fence pending acc rest
        else
          let start, text =
            match pending with Some (s, p) -> (s, p ^ " " ^ t) | None -> (n, t)
          in
          let len = String.length text in
          if len > 0 && text.[len - 1] = '\\' then
            go (n + 1) fence (Some (start, String.sub text 0 (len - 1))) acc rest
          else go (n + 1) fence None ((start, text) :: acc) rest
  in
  go 1 Outside None [] lines

let placeholder s = String.contains s '<'
let value_flags = [ "-t"; "--target"; "--params"; "--var"; "-p"; "--profile"; "--json" ]

let rec flag_value flags = function
  | f :: v :: _ when List.mem f flags -> Some v
  | _ :: rest -> flag_value flags rest
  | [] -> None

let rec first_positional = function
  | f :: _ :: rest when List.mem f value_flags -> first_positional rest
  | t :: rest -> if String.starts_with ~prefix:"-" t then first_positional rest else Some t
  | [] -> None

let param_name pair =
  match String.index_opt pair '=' with Some i -> String.sub pair 0 i | None -> pair

let check facts ~file ~line toks =
  let problem message = { file; line; message } in
  match toks with
  | "make" :: target :: _ when not (String.contains target '=') ->
      if List.mem target facts.make_targets then []
      else [ problem (Printf.sprintf "make target %S is not in the Makefile" target) ]
  | "databricks" :: "bundle" :: verb :: rest ->
      let target =
        match flag_value [ "-t"; "--target" ] rest with
        | Some t when (not (placeholder t)) && not (List.mem t facts.bundle_targets) ->
            [ problem (Printf.sprintf "bundle target %S is not in databricks.yml" t) ]
        | _ -> []
      in
      let run =
        if verb <> "run" then []
        else
          match first_positional rest with
          | None -> [ problem "bundle run names no job or app" ]
          | Some key when placeholder key -> []
          | Some key -> (
              match List.assoc_opt key facts.runnable with
              | None -> [ problem (Printf.sprintf "%S is not a job or app in resources/" key) ]
              | Some params ->
                  flag_value [ "--params" ] rest
                  |> Option.fold ~none:[] ~some:(String.split_on_char ',')
                  |> List.map param_name
                  |> List.filter (fun name -> not (List.mem name params))
                  |> List.map (fun name ->
                         problem (Printf.sprintf "job %S has no parameter %S" key name)))
      in
      target @ run
  | "databricks" :: "secrets" :: "put-secret" :: _ :: name :: _ ->
      if List.mem name facts.secrets then []
      else [ problem (Printf.sprintf "no code reads the secret %S" name) ]
  | _ -> []

let check_lines facts ~file lines =
  let found = commands lines in
  let problems =
    List.concat_map
      (fun (line, text) ->
        tokens text |> split_commands |> List.concat_map (check facts ~file ~line))
      found
  in
  (List.length found, problems)

let read_lines path =
  In_channel.with_open_text path In_channel.input_all |> String.split_on_char '\n'

let files_with_suffix dir suffix =
  Sys.readdir dir |> Array.to_list
  |> List.filter (fun f -> Filename.check_suffix f suffix)
  |> List.sort compare

let rec python_files dir =
  Sys.readdir dir |> Array.to_list |> List.sort compare
  |> List.concat_map (fun f ->
         let path = Filename.concat dir f in
         if Sys.is_directory path then python_files path
         else if Filename.check_suffix f ".py" then [ path ]
         else [])

let load_facts root =
  let path p = Filename.concat root p in
  let resources =
    files_with_suffix (path "resources") ".yml"
    |> List.map (fun f -> read_lines (Filename.concat (path "resources") f))
  in
  let runnable =
    List.concat_map
      (fun lines ->
        blocks ~parent:"jobs" ~at:2 lines @ blocks ~parent:"apps" ~at:2 lines
        |> List.map (fun (key, body) -> (key, list_names ~parent:"parameters" ~at:6 body)))
      resources
  in
  {
    make_targets = make_targets (read_lines (path "Makefile"));
    bundle_targets = List.map fst (blocks ~parent:"targets" ~at:0 (read_lines (path "databricks.yml")));
    runnable;
    secrets = List.concat_map (fun f -> secret_names (read_lines f)) (python_files (path "collector/src"));
  }
