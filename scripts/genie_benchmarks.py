import argparse
import time

from databricks.sdk import WorkspaceClient

SPACES = ("PITLake markets", "PITLake operations")
POLL_SECONDS = 15


def find_spaces(w: WorkspaceClient, target: str) -> dict[str, str]:
    titles = {f"{name} · {target}" for name in SPACES}
    found, token = {}, None
    while True:
        page = w.genie.list_spaces(page_token=token)
        found |= {s.title: s.space_id for s in page.spaces or [] if s.title in titles}
        token = page.next_page_token
        if not token:
            break
    missing = sorted(titles - set(found))
    if missing:
        raise SystemExit(f"not deployed: {missing}")
    return found


def latest_run(w: WorkspaceClient, space_id: str) -> str:
    runs = w.genie.genie_list_eval_runs(space_id).eval_runs or []
    if not runs:
        raise SystemExit(f"space {space_id} has no benchmark runs; use --start")
    return max(runs, key=lambda r: r.created_timestamp or 0).eval_run_id


def wait(w: WorkspaceClient, space_id: str, run_id: str):
    while True:
        run = w.genie.genie_get_eval_run(space_id, run_id)
        if run.num_done == run.num_questions and run.num_questions:
            return run
        time.sleep(POLL_SECONDS)


def shape(response) -> str:
    result = response.sql_execution_result if response else None
    if result is None or result.manifest is None:
        return "no result"
    columns = [c.name for c in (result.manifest.schema.columns or [])]
    return f"{result.manifest.total_row_count} rows ({', '.join(columns)})"


def report(w: WorkspaceClient, title: str, space_id: str, run_id: str) -> None:
    run = wait(w, space_id, run_id)
    print(
        f"\n{title}: {run.num_correct} correct, {run.num_needs_review} to review, "
        f"of {run.num_questions} (run {run_id})"
    )
    for result in w.genie.genie_list_eval_results(space_id, run_id).eval_results or []:
        d = w.genie.genie_get_eval_result_details(space_id, run_id, result.result_id)
        actual = (d.actual_response or [None])[0]
        expected = (d.expected_response or [None])[0]
        assessment = d.assessment.value if d.assessment else "?"
        reasons = ", ".join(r.value for r in d.assessment_reasons or [])
        print(f"\n[{assessment}] {result.question}" + (f"  ({reasons})" if reasons else ""))
        print(f"  Genie:    {shape(actual)}")
        print(f"  expected: {shape(expected)}")
        if actual and actual.response:
            print("  " + "\n  ".join(actual.response.strip().splitlines()))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("target", choices=("dev", "prod"))
    parser.add_argument("--start", action="store_true", help="Start a new run of every benchmark")
    args = parser.parse_args()
    w = WorkspaceClient()
    for title, space_id in sorted(find_spaces(w, args.target).items()):
        if args.start:
            run_id = w.genie.genie_create_eval_run(space_id).eval_run_id
        else:
            run_id = latest_run(w, space_id)
        report(w, title, space_id, run_id)


if __name__ == "__main__":
    main()
