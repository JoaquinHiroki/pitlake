open Pitcheck

let test_sha256_vectors () =
  assert (Sha256.hex "" = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  assert (Sha256.hex "abc" = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  assert (
    Sha256.hex "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
    = "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
  assert (
    Sha256.hex (String.make 1000 'a')
    = "41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3")

let test_floats_print_like_python () =
  List.iter
    (fun (x, python) -> assert (Pyjson.float_repr x = python))
    [
      (0.1, "0.1");
      (4.3, "4.3");
      (123.0, "123.0");
      (-0.0, "-0.0");
      (90753000000.0, "90753000000.0");
      (-6.302e9, "-6302000000.0");
      (0.03022699626172387, "0.03022699626172387");
      (1e-05, "1e-05");
      (1.5e-07, "1.5e-07");
      (0.0001, "0.0001");
      (1234567890123456.0, "1234567890123456.0");
      (1e16, "1e+16");
      (12345678901234567.0, "1.2345678901234568e+16");
    ]

let test_instants_print_like_python () =
  let day = Instant.days_of_iso_date "2025-02-01" in
  assert (Instant.iso (Instant.at_hour day 6) = "2025-02-01T06:00:00+00:00");
  assert (Instant.iso 0 = "1970-01-01T00:00:00+00:00");
  assert (Instant.iso (-1) = "1969-12-31T23:59:59.999999+00:00");
  assert (Instant.iso (Instant.at_hour (Instant.days_of_iso_date "1947-01-01") 0)
          = "1947-01-01T00:00:00+00:00");
  assert (Instant.iso 1_722_549_814_000_000 = "2024-08-01T22:03:34+00:00")

let row ?(entity = "AAPL") ?(value = Some 1.0) feature event_time knowable_at : As_of.row =
  { entity; feature; event_time; knowable_at; value; source_table = Some "t"; source_ref = "r" }

let test_nothing_is_seen_before_it_is_knowable () =
  let rows = [ row "net_income_q" 10 50 ~value:(Some 5.0); row "net_income_q" 20 100 ~value:(Some 7.0) ] in
  let seen = As_of.observations ~universe:[ "AAPL" ] ~global:"US" rows [ 40; 50; 99; 100 ] in
  assert (
    List.map (fun (s : As_of.seen) -> (s.at, s.value)) seen
    = [ (50, 5.0); (99, 5.0); (100, 7.0) ])

let test_a_restatement_wins_and_a_later_event_wins_over_it () =
  let rows =
    [ row "revenue_q" 10 50 ~value:(Some 1.0); row "revenue_q" 10 60 ~value:(Some 2.0);
      row "revenue_q" 5 70 ~value:(Some 9.0) ]
  in
  let seen = As_of.observations ~universe:[ "AAPL" ] ~global:"US" rows [ 55; 65; 75 ] in
  assert (List.map (fun (s : As_of.seen) -> s.value) seen = [ 1.0; 2.0; 2.0 ])

let test_global_rows_apply_to_every_entity () =
  let rows = [ row ~entity:"US" "cpi_yoy" 1 1; row ~entity:"SPY" "close" 1 1 ] in
  let seen = As_of.observations ~universe:[ "AAPL"; "SPY" ] ~global:"US" rows [ 2 ] in
  assert (
    List.map (fun (s : As_of.seen) -> (s.entity, s.feature)) seen
    = [ ("AAPL", "cpi_yoy"); ("SPY", "close"); ("SPY", "cpi_yoy") ])

let test_payload_matches_python_json () =
  let seen : As_of.seen list =
    [ { entity = "B"; at = 0; feature = "close"; value = 2.0; event_time = 0 };
      { entity = "A"; at = 0; feature = "close"; value = 0.5; event_time = -1 } ]
  in
  assert (
    As_of.payload seen
    = "[[\"A\",\"1970-01-01T00:00:00+00:00\",\"close\",0.5,\"1969-12-31T23:59:59.999999+00:00\"],"
      ^ "[\"B\",\"1970-01-01T00:00:00+00:00\",\"close\",2.0,\"1970-01-01T00:00:00+00:00\"]]")

let bars n =
  List.init n (fun i : Ablation.bar ->
      {
        symbol = "AAPL";
        day = Printf.sprintf "2025-01-%02d" (i + 1);
        close = 100.0 +. float_of_int (i * i mod 7);
        volume = 10.0;
        knowable_at = (i + 1) * 100;
      })

let price_rows found : As_of.row list =
  Hashtbl.fold
    (fun (feature, day) (value, knowable_at) acc ->
      if feature = "close" || feature = "volatility_20d" then
        ({ entity = "AAPL"; feature; event_time = 0; knowable_at; value = Some value;
           source_table = Some "bars_1d"; source_ref = day } : As_of.row)
        :: acc
      else acc)
    found []

let test_owners_name_the_item () =
  let r feature table ref : As_of.row =
    { entity = "AAPL"; feature; event_time = 0; knowable_at = 0; value = None;
      source_table = Some table; source_ref = ref }
  in
  assert (Ablation.owner (r "close" "bars_1d" "2025-01-02") = Some "bar:AAPL/2025-01-02");
  assert (Ablation.owner (r "net_income_q" "edgar_company_facts" "a-1") = Some "filing:a-1");
  assert (Ablation.owner (r "cpi_yoy" "fred_series_vintages" "2025-02-12")
          = Some "release:CPIAUCSL/2025-02-12");
  assert (Ablation.owner (r "x" "elsewhere" "1") = None)

let test_prices_follow_gold () =
  let found = Ablation.prices (bars 22) in
  let value f d = fst (Hashtbl.find found (f, d)) in
  assert (value "close" "2025-01-03" = 104.0);
  assert (not (Hashtbl.mem found ("volatility_20d", "2025-01-20")));
  let returns = List.init 20 (fun k ->
    let c i = 100.0 +. float_of_int (i * i mod 7) in log (c (k + 1) /. c k)) in
  assert (value "volatility_20d" "2025-01-21" = Ablation.stddev_samp returns);
  assert (snd (Hashtbl.find found ("close", "2025-01-22")) = 2200)

let test_a_coherent_world_passes_and_traces_are_found () =
  let left = List.filter (fun (b : Ablation.bar) -> b.day <> "2025-01-05") (bars 24) in
  let rows = price_rows (Ablation.prices left) in
  let withheld = [ "bar:AAPL/2025-01-05" ] in
  assert (snd (Ablation.check ~withheld ~bars:left rows) = []);
  let stale = price_rows (Ablation.prices (bars 24)) in
  let _, problems = Ablation.check ~withheld ~bars:left stale in
  assert (List.exists (fun p -> String.starts_with ~prefix:"close of AAPL for 2025-01-05 belongs" p) problems);
  assert (List.exists (fun p -> String.starts_with ~prefix:"volatility_20d of AAPL for 2025-01-24 is" p) problems);
  let _, problems = Ablation.check ~withheld ~bars:left (List.tl rows) in
  assert (List.length problems = 1);
  let _, problems = Ablation.check ~withheld ~bars:(bars 24) rows in
  assert (List.mem "bar:AAPL/2025-01-05 is still a bar" problems)

let () =
  test_sha256_vectors ();
  test_floats_print_like_python ();
  test_instants_print_like_python ();
  test_nothing_is_seen_before_it_is_knowable ();
  test_a_restatement_wins_and_a_later_event_wins_over_it ();
  test_global_rows_apply_to_every_entity ();
  test_payload_matches_python_json ();
  test_owners_name_the_item ();
  test_prices_follow_gold ();
  test_a_coherent_world_passes_and_traces_are_found ();
  print_endline "pitcheck: 10 tests passed"
