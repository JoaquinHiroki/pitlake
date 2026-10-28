open Rankcheck

let chunks =
  chunks_of
    [ "c1\tAAPL\t100"; "c2\tAAPL\t100"; "c3\tAAPL\t100"; "late\tAAPL\t999"; "msft\tMSFT\t100" ]

let questions =
  questions_of [ "q1\tAAPL\t500\tc2"; "q2\tAAPL\t500\tc3,c1"; "q3\tAAPL\t500\t" ]

let ranked lines = Hashtbl.find (rerankings_of lines) "lora"
let close a b = Float.abs (a -. b) < 1e-12

let test_metrics_follow_the_retriever_definitions () =
  let checked =
    evaluate questions chunks
      (ranked [ "q1\tlora\tc1,c2,c3"; "q2\tlora\tc1,c2,c3"; "q3\tlora\tc2,c1,c3" ])
  in
  let m key = List.assoc key checked.metrics in
  assert (close (m "hit_at_1") (1. /. 3.));
  assert (close (m "hit_at_5") (2. /. 3.));
  assert (close (m "mrr_at_10") ((0.5 +. 1.) /. 3.));
  assert (close (m "hit_at_20") (2. /. 3.));
  assert (m "questions" = 3. && m "future_chunks" = 0.);
  assert (checked.future = 0 && checked.other_company = 0 && checked.missing = [])

let test_only_the_top_ten_count_for_hits () =
  let eleven = String.concat "," (List.init 10 (fun _ -> "c1") @ [ "c2" ]) in
  let checked = evaluate [ List.hd questions ] chunks (ranked [ "q1\tlora\t" ^ eleven ]) in
  assert (List.assoc "hit_at_10" checked.metrics = 0.);
  assert (List.assoc "hit_at_20" checked.metrics = 1.)

let test_leaks_other_companies_and_gaps_are_found () =
  let checked =
    evaluate questions chunks
      (ranked [ "q1\tlora\tlate,c2"; "q2\tlora\tmsft,nowhere,c3" ])
  in
  assert (checked.future = 1);
  assert (List.assoc "future_chunks" checked.metrics = 1.);
  assert (checked.other_company = 1);
  assert (checked.missing = [ "chunk nowhere"; "question q3" ])

let test_recorded_metrics_are_compared_within_tolerance () =
  let rows =
    compare_metrics
      ~recorded:[ ("hit_at_1", 0.5); ("mrr_at_10", 0.6); ("new_metric", 1.) ]
      ~rebuilt:[ ("hit_at_1", 0.5 +. 1e-12); ("mrr_at_10", 0.61) ]
  in
  assert (
    List.map (fun (k, _, _, same) -> (k, same)) rows
    = [ ("hit_at_1", true); ("mrr_at_10", false); ("new_metric", false) ])

let test_files_parse () =
  assert (metrics_of [ "lora\tmrr_at_10\t0.6802884615384616"; "base\tquestions\t208.0" ]
          = [ ("lora", "mrr_at_10", 0.6802884615384616); ("base", "questions", 208.) ]);
  assert (meta_of [ "run_id\t42" ] = [ ("run_id", "42") ]);
  assert ((List.nth questions 2).relevant = [])

let () =
  test_metrics_follow_the_retriever_definitions ();
  test_only_the_top_ten_count_for_hits ();
  test_leaks_other_companies_and_gaps_are_found ();
  test_recorded_metrics_are_compared_within_tolerance ();
  test_files_parse ();
  print_endline "rankcheck: 5 tests passed"
