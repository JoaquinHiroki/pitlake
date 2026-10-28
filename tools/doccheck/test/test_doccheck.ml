open Doccheck

let lines s = String.split_on_char '\n' s

let jobs_yml =
  lines
    {|resources:
  jobs:
    ingest:
      name: pitlake-ingest-${bundle.target}
      parameters:
        - name: max_files
          default: "30"
        - name: datasets
          default: ""
      tasks:
        - task_key: plan
          python_wheel_task:
            parameters:
              - "--catalog"

    transform:
      name: pitlake-transform-${bundle.target}
  apps:
    console:
      name: pitlake-console|}

let facts =
  {
    make_targets = [ "check"; "deploy"; "genie-benchmarks" ];
    bundle_targets = [ "dev"; "prod" ];
    runnable =
      List.map
        (fun (key, body) -> (key, list_names ~parent:"parameters" ~at:6 body))
        (blocks ~parent:"jobs" ~at:2 jobs_yml @ blocks ~parent:"apps" ~at:2 jobs_yml);
    secrets = [ "fred-api-key" ];
    layers = [ "silver"; "gold"; "control" ];
    tables = [ "binance_spot_trades"; "bars_1d"; "silver_load_log" ];
  }

let messages markdown =
  let _, _, problems = check_lines facts ~file:"x.md" (lines markdown) in
  problems
  |> List.map (fun p -> Printf.sprintf "%d: %s" p.line p.message)

let test_jobs_and_their_parameters () =
  assert (
    facts.runnable
    = [ ("ingest", [ "max_files"; "datasets" ]); ("transform", []); ("console", []) ])

let test_make_targets () =
  let makefile = lines "export X ?= y\n.PHONY: a b\nsetup:\n\tuv sync\ndeploy-prod:\nTARGET ?= dev" in
  assert (make_targets makefile = [ "setup"; "deploy-prod" ])

let test_tokens_keep_quoted_words () =
  assert (
    tokens "put-secret pitlake k --string-value '<FRED key>'"
    = [ "put-secret"; "pitlake"; "k"; "--string-value"; "<FRED key>" ])

let test_commands_join_continuations_and_skip_other_fences () =
  let markdown = "```sql\nmake nope\n```\n```bash\n# a comment\nmake a \\\n  b\nmake c\n```" in
  assert (commands (lines markdown) = [ (6, "make a  b"); (8, "make c") ])

let test_good_commands_pass () =
  assert (
    messages
      "```bash\n\
       make check && make deploy\n\
       make genie-benchmarks TARGET=dev\n\
       databricks bundle run ingest -t dev --params max_files=0,datasets=a\n\
       databricks bundle run console -t prod\n\
       databricks bundle run <job> -t <target>\n\
       databricks secrets put-secret pitlake fred-api-key --string-value '<key>'\n\
       ```"
    = [])

let test_drift_is_reported () =
  assert (
    messages
      "```bash\n\
       make deploy-staging\n\
       databricks bundle deploy -t staging\n\
       databricks bundle run ingest -t dev --params period=2025-01-01\n\
       databricks bundle run nightly -t dev\n\
       databricks secrets put-secret pitlake alpaca-key --string-value x\n\
       ```"
    = [
        "2: make target \"deploy-staging\" is not in the Makefile";
        "3: bundle target \"staging\" is not in databricks.yml";
        "4: job \"ingest\" has no parameter \"period\"";
        "5: \"nightly\" is not a job or app in resources/";
        "6: no code reads the secret \"alpaca-key\"";
      ])

let test_table_names_come_from_the_code () =
  let datasets =
    lines "X = Dataset(\n    source=\"binance\",\n    name=\"spot_trades\",\n    columns=(\"a\",),\n)"
  in
  assert (dataset_tables datasets = [ "binance_spot_trades" ]);
  assert (
    quoted_words (lines "    return f\"{catalog}.gold.chunk_embeddings_index\"  # \"x\"")
    = [ "catalog"; "gold"; "chunk_embeddings_index"; "x" ]);
  assert (
    layer_names (lines "LAYERS = (\"raw\", \"bronze\")\nX = \"y\"") = [ "raw"; "bronze" ])

let test_table_references () =
  assert (
    table_refs [ "dev"; "prod" ]
      "FROM pitlake_dev.control.silver_load_log, `pitlake_prod.gold.bars_1d`. pitlake_dev.<schema>.<t> \
       pitlake_collector.sources.x pitlake_dev.control"
    = [ ("control", "silver_load_log"); ("gold", "bars_1d") ]);
  assert (
    messages
      "SELECT * FROM pitlake_dev.silver.binance_spot_trades;\n\
       Open `pitlake_prod.control.silver_load_logs`.\n\
       ```sql\nSELECT 1 FROM pitlake_dev.golden.bars_1d\n```"
    = [
        "2: no code creates the table control.silver_load_logs";
        "4: schema \"golden\" is not a layer";
      ])

let () =
  test_jobs_and_their_parameters ();
  test_make_targets ();
  test_tokens_keep_quoted_words ();
  test_commands_join_continuations_and_skip_other_fences ();
  test_good_commands_pass ();
  test_drift_is_reported ();
  test_table_names_come_from_the_code ();
  test_table_references ();
  print_endline "doccheck: 8 tests passed"
