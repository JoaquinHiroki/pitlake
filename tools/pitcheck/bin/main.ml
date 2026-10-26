open Pitcheck

let () =
  if Array.length Sys.argv <> 2 then begin
    prerr_endline "usage: pitcheck <folder with meta.tsv and features.tsv>";
    exit 2
  end;
  let dir = Sys.argv.(1) in
  let m = Inputs.meta (Filename.concat dir "meta.tsv") in
  let rows = Inputs.rows (Filename.concat dir "features.tsv") in
  let times = Inputs.decision_times m in
  let seen = As_of.observations ~universe:m.universe ~global:m.global rows times in
  let computed = As_of.fingerprint seen in
  Printf.printf "backtest %s read gold.features version %s from %s to %s\n" m.backtest_id
    m.version m.period_start m.period_end;
  Printf.printf "rebuilt %d values at %d decisions from %d Gold rows, for %s\n"
    (List.length seen) (List.length times) (List.length rows)
    (String.concat ", " m.universe);
  Printf.printf "recorded input hash %s\nrebuilt input hash  %s\n" m.input_hash computed;
  if computed = m.input_hash then
    print_endline "IDENTICAL: an independent implementation of as-of sees exactly what the backtest saw"
  else begin
    print_endline "DIFFERENT: the rebuilt inputs do not match the recorded hash";
    exit 1
  end
