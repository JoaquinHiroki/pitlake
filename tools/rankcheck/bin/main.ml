open Rankcheck

let () =
  if Array.length Sys.argv <> 2 then begin
    prerr_endline "usage: rankcheck <folder from scripts/export_rerank_inputs.py>";
    exit 2
  end;
  let file name = Filename.concat Sys.argv.(1) name in
  let meta = meta_of (read_lines (file "meta.tsv")) in
  let get key = List.assoc key meta in
  let questions = questions_of (read_lines (file "questions.tsv")) in
  let chunks = chunks_of (read_lines (file "chunks.tsv")) in
  let rerankings = rerankings_of (read_lines (file "rerankings.tsv")) in
  let recorded = metrics_of (read_lines (file "metrics.tsv")) in
  let digest =
    Pitcheck.Sha256.hex (In_channel.with_open_bin (file "rerankings.tsv") In_channel.input_all)
  in
  Printf.printf "reranker run %s: %d test questions (gold.rag_questions version %s), %d chunks \
                 (gold.chunk_embeddings version %s)\n"
    (get "run_id") (List.length questions) (get "questions_version") (Hashtbl.length chunks)
    (get "embeddings_version");
  Printf.printf "recorded rerankings hash %s\nrebuilt rerankings hash  %s\n" (get "rankings_hash")
    digest;
  let models = List.sort_uniq compare (List.map (fun (m, _, _) -> m) recorded) in
  let ok =
    List.fold_left
      (fun ok model ->
        let ranked =
          match Hashtbl.find_opt rerankings model with Some t -> t | None -> Hashtbl.create 1
        in
        let checked = evaluate questions chunks ranked in
        let mine = List.filter_map (fun (m, k, v) -> if m = model then Some (k, v) else None) recorded in
        let rows = compare_metrics ~recorded:mine ~rebuilt:checked.metrics in
        Printf.printf "%s:\n" model;
        List.iter
          (fun (key, value, rebuilt, same) ->
            Printf.printf "  %-14s recorded %.12g rebuilt %s%s\n" key value
              (match rebuilt with Some v -> Printf.sprintf "%.12g" v | None -> "(not computed)")
              (if same then "" else "  <- differs"))
          rows;
        Printf.printf "  %d of %d ranked passages knowable after the question, %d of another company\n"
          checked.future
          (Hashtbl.fold (fun _ ids n -> n + List.length ids) ranked 0)
          checked.other_company;
        List.iter (Printf.printf "  missing %s\n") checked.missing;
        ok
        && List.for_all (fun (_, _, _, same) -> same) rows
        && checked.future = 0 && checked.other_company = 0 && checked.missing = [])
      (digest = get "rankings_hash" && models <> [])
      models
  in
  if ok then
    print_endline
      "IDENTICAL: an independent reading of Gold gives the recorded metrics, and no reranked \
       passage was knowable after its question or belongs to another company"
  else begin
    print_endline "DIFFERENT: see the lines above";
    exit 1
  end
