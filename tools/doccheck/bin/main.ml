let runbooks = "docs/runbooks"

let () =
  let root = if Array.length Sys.argv > 1 then Sys.argv.(1) else "." in
  let facts = Doccheck.load_facts root in
  let results =
    Doccheck.files_with_suffix (Filename.concat root runbooks) ".md"
    |> List.map (fun name ->
           let file = Filename.concat runbooks name in
           Doccheck.check_lines facts ~file (Doccheck.read_lines (Filename.concat root file)))
  in
  let problems = List.concat_map snd results in
  List.iter
    (fun (p : Doccheck.problem) -> Printf.printf "%s:%d: %s\n" p.file p.line p.message)
    problems;
  Printf.printf "checked %d commands in %d runbooks: %d problems\n"
    (List.fold_left (fun n (count, _) -> n + count) 0 results)
    (List.length results) (List.length problems);
  if problems <> [] then exit 1
