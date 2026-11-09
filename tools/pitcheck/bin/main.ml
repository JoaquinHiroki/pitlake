open Pitcheck

let () =
  if Array.length Sys.argv <> 2 then begin
    prerr_endline "usage: pitcheck <folder with meta.tsv and features.tsv>";
    exit 2
  end;
  let dir = Sys.argv.(1) in
  let file = Filename.concat dir in
  let m = Inputs.meta (file "meta.tsv") in
  let rows = Inputs.rows (file "features.tsv") in
  let times = Inputs.decision_times m in
  let seen = As_of.observations ~universe:m.universe ~global:m.global rows times in
  let computed = As_of.fingerprint seen in
  Printf.printf "backtest %s read gold.features version %s from %s to %s\n" m.backtest_id
    m.version m.period_start m.period_end;
  Printf.printf "rebuilt %d values at %d decisions from %d Gold rows, for %s\n"
    (List.length seen) (List.length times) (List.length rows)
    (String.concat ", " m.universe);
  Printf.printf "recorded input hash %s\nrebuilt input hash  %s\n" m.input_hash computed;
  let identical = computed = m.input_hash in
  if identical then
    print_endline "IDENTICAL: an independent implementation of as-of sees exactly what the backtest saw"
  else print_endline "DIFFERENT: the rebuilt inputs do not match the recorded hash";
  let coherent =
    if not (Sys.file_exists (file "withheld.tsv")) then true
    else begin
      let withheld = Inputs.withheld (file "withheld.tsv") in
      let bars = Inputs.bars (file "bars.tsv") in
      let checked, problems = Ablation.check ~withheld ~bars rows in
      Printf.printf "withheld %s; %d bars left; %d price rows recomputed from them\n"
        (String.concat " " withheld) (List.length bars) checked;
      List.iteri (fun i p -> if i < 20 then Printf.printf "  %s\n" p) problems;
      if problems = [] then
        print_endline "COHERENT: no row comes from a withheld item and every price feature equals its bars"
      else Printf.printf "INCOHERENT: %d problems\n" (List.length problems);
      problems = []
    end
  in
  if not (identical && coherent) then exit 1
