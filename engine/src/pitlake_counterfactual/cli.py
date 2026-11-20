import argparse
import json
import os
import re
import sys
from pathlib import Path

from pitlake_counterfactual import __version__
from pitlake_counterfactual.ablate import ALPHA, UNSETTLED, Report, Tested, ablate
from pitlake_counterfactual.agent import Session, agent_config, answered_by_tool, loop
from pitlake_counterfactual.endpoint import chat, load
from pitlake_counterfactual.world import Bundle, World, iso

SHORT = 12


def items_of(text: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(i for i in re.split(r"[\s,;]+", text) if i))


def describe_bundle(bundle: Bundle) -> str:
    kinds = ", ".join(f"{k} {n:,}" for k, n in bundle.by_kind().items())
    source = bundle.source
    origin = (
        " ".join(str(source[k]) for k in ("catalog", "backtest_id") if source.get(k))
        or "unknown origin"
    )
    return (
        f"bundle {bundle.name} ({bundle.id[:SHORT]}) from {origin} at "
        f"{bundle.at:%Y-%m-%d %H:%M} UTC: {len(bundle.items):,} items ({kinds}), "
        f"{len(bundle.rows):,} rows, symbols {', '.join(bundle.symbols)}"
    )


def terms_lines(bundle: Bundle) -> list[str]:
    terms = bundle.terms
    if not terms:
        return ["terms: none recorded; ask whoever sent the bundle before sharing it"]
    lines = [f"terms: {terms.get('licence', 'unknown')}"]
    lines += [f"  {feed}: {text}" for feed, text in sorted(terms.get("sources", {}).items())]
    return lines


def check_line(bundle: Bundle, problems: list[str]) -> list[str]:
    if not problems:
        return [
            f"VERIFIED: the items hash to the recorded set {bundle.recorded_set_hash[:SHORT]} and "
            f"the engine rebuilds all {len(bundle.rows):,} rows from them"
        ]
    return [f"BROKEN: {len(problems)} problems with the bundle"] + [f"  {p}" for p in problems]


def world_line(world: World) -> str:
    withheld = " ".join(world.withheld) or "nothing"
    state = "coherent" if world.coherent else f"NOT coherent ({len(world.problems)} problems)"
    return (
        f"withheld {withheld}: {len(world.rows):,} rows, {world.removed:,} removed and "
        f"{world.recomputed:,} recomputed; {state}; world {world.hash[:SHORT]}, as-of inputs "
        f"{world.inputs_hash()[:SHORT]}"
    )


def settled_text(settled: dict) -> str:
    return ", ".join(f"{k} {v}" for k, v in sorted(settled.items())) or "nothing"


def tested_line(t: Tested) -> str:
    parts = []
    for part, outcome in sorted(t.effect.outcomes.items()):
        p = t.effect.p_values.get(part)
        shown = f"{part} {outcome}"
        if p is not None:
            shown += f" (effect {t.effect.effects[part]:+.2f}, p {p:.3g})"
        parts.append(shown)
    inputs = "inputs changed" if t.inputs_changed else "the base calls read the same"
    label = "null world" if t.name == "null" else f"without {t.name}"
    return (
        f"{t.verdict}: {label}: {'; '.join(parts) or 'no settled part'}; "
        f"{sum(1 for s in t.samples if not s.valid)} of {len(t.samples)} invalid; {inputs}; "
        f"{world_line(t.world)}"
    )


def report_lines(report: Report) -> list[str]:
    base = report.base
    unsettled = [k for k in sorted(base.parts) if k not in base.settled]
    lines = [
        f"real world: settled {settled_text(base.settled)} ({len(base.settled)} of "
        f"{len(base.parts)} parts at {report.min_agreement:.2f}), "
        f"{'stable' if base.stable else 'NOT stable'} over {len(report.base_rounds)} rounds; "
        f"{UNSETTLED} {', '.join(unsettled) or 'nothing'}; {base.samples - base.valid} of "
        f"{base.samples} samples invalid"
    ]
    errors: dict[str, int] = {}
    for found in report.base_rounds:
        for s in found:
            if s.error:
                errors[s.error[:200]] = errors.get(s.error[:200], 0) + 1
    lines += [f"  invalid: {n} x {e}" for e, n in sorted(errors.items(), key=lambda kv: -kv[1])[:5]]
    if report.null is not None:
        lines.append(tested_line(report.null))
    lines += [tested_line(t) for t in report.worlds]
    return lines


def write(report: Report, out: Path) -> Path:
    out.mkdir(parents=True, exist_ok=True)
    report.bundle.withhold(()).export(out / "worlds" / "real", "real")
    for k, t in enumerate(report.worlds, start=1):
        t.world.export(out / "worlds" / f"world-{k}", f"world-{k}")
    path = out / "result.json"
    path.write_text(json.dumps(report.to_json(__version__), indent=1, sort_keys=True) + "\n")
    return path


def decider_of(args, symbols):
    if args.decider:
        return load(args.decider), {
            "decider": "custom",
            "function": args.decider,
            "symbols": list(symbols),
        }
    if args.agent:
        complete = load(args.agent)
        return loop(complete), agent_config(args.agent, symbols)
    if not args.endpoint or not args.model:
        raise SystemExit("name an agent: --endpoint and --model, --agent or --decider")
    key = os.environ.get(args.key_env) if args.key_env else None
    call = chat(args.endpoint, args.model, key)
    if args.answer_by_tool:
        complete = answered_by_tool(call, symbols)
    else:

        def complete(messages, tools):
            return call(messages, tools, None)

    return loop(complete), agent_config(args.model, symbols, args.answer_by_tool)


def inspect(args) -> int:
    bundle = Bundle.read(args.bundle)
    print(describe_bundle(bundle))
    for line in terms_lines(bundle):
        print(line)
    problems = bundle.check()
    for line in check_line(bundle, problems):
        print(line)
    found = sorted(bundle.items.values(), key=lambda i: (i.kind, i.entity, i.knowable_at, i.id))
    if args.kind:
        found = [i for i in found if i.kind == args.kind]
    if args.entity:
        found = [i for i in found if i.entity == args.entity]
    for item in found[-args.limit :] if args.limit else found:
        print(f"{item.id}\t{item.entity}\tknowable {iso(item.knowable_at)}")
    return 1 if problems else 0


def world(args) -> int:
    bundle = Bundle.read(args.bundle)
    print(describe_bundle(bundle))
    problems = bundle.check()
    for line in check_line(bundle, problems):
        print(line)
    found = bundle.withhold(items_of(args.withhold))
    print(world_line(found))
    for p in found.problems:
        print(f"  {p}")
    if args.show:
        session = Session.of(found)
        for symbol in bundle.symbols:
            print(session.answer("symbol_snapshot", {"symbol": symbol}))
        print(session.answer("macro", {}))
    if args.out:
        print(f"wrote {found.export(Path(args.out), 'world')}")
    return 0 if found.coherent and not problems else 1


def run_ablation(args) -> int:
    bundle = Bundle.read(args.bundle)
    print(describe_bundle(bundle))
    worlds = [items_of(w) for w in args.withhold]
    if not all(worlds):
        raise SystemExit("every --withhold names at least one item")
    for w in worlds:
        unknown = [i for i in w if i not in bundle.items]
        if unknown:
            raise SystemExit(f"not items of {bundle.name}: {', '.join(unknown)}")
    decider, config = decider_of(args, bundle.symbols)
    report = ablate(
        bundle,
        worlds,
        decider,
        config,
        args.samples,
        args.min_agreement,
        args.workers,
        args.rounds,
        not args.no_null,
    )
    for line in check_line(bundle, list(report.problems)):
        print(line)
    for line in report_lines(report):
        print(line)
    path = write(report, Path(args.out))
    if report.passed:
        changed = [t.name for t in report.worlds if t.effect.caused]
        print(
            f"OK: {len(report.worlds)} worlds decided against the real one, "
            f"{len(changed)} changed a part at p < {ALPHA}; result in {path}"
        )
        return 0
    reasons = []
    if report.problems:
        reasons.append("the bundle does not verify")
    if not report.base.reproducible:
        reasons.append("no part settles in the real world")
    if report.null_passed is False:
        reasons.append("the null world moved the decision")
    if any(not (t.world.coherent and t.effect.valid) for t in report.worlds):
        reasons.append("a world is not coherent or has too many invalid samples")
    print(f"FAILED: {'; '.join(reasons)}; result in {path}")
    return 1


def parser() -> argparse.ArgumentParser:
    found = argparse.ArgumentParser(
        prog="pitlake-counterfactual",
        description="Withhold information from a PITLake world and see what an agent decides.",
    )
    found.add_argument("--version", action="version", version=__version__)
    commands = found.add_subparsers(dest="command", required=True)

    shown = commands.add_parser("inspect", help="Verify a bundle and list its items")
    shown.add_argument("bundle")
    shown.add_argument("--kind", choices=("bar", "filing", "release"))
    shown.add_argument("--entity", default="")
    shown.add_argument("--limit", type=int, default=0)
    shown.set_defaults(handle=inspect)

    built = commands.add_parser("world", help="Build one world and check it, without deciding")
    built.add_argument("bundle")
    built.add_argument("--withhold", default="", help="Item IDs withheld together")
    built.add_argument("--show", action="store_true", help="Print what the tools return")
    built.add_argument("--out", default="", help="Folder for the independent check")
    built.set_defaults(handle=world)

    tested = commands.add_parser("ablate", help="Decide in the real world and without items")
    tested.add_argument("bundle")
    tested.add_argument(
        "--withhold",
        action="append",
        required=True,
        help="Item IDs withheld together in one world; repeat for more worlds",
    )
    tested.add_argument("--endpoint", default="", help="OpenAI-compatible base URL")
    tested.add_argument("--model", default="")
    tested.add_argument("--key-env", default="OPENAI_API_KEY", help="Variable with the API key")
    tested.add_argument("--answer-by-tool", action="store_true")
    tested.add_argument("--agent", default="", help="module:function that completes a chat")
    tested.add_argument("--decider", default="", help="module:function that decides a session")
    tested.add_argument("--samples", type=int, default=10)
    tested.add_argument("--rounds", type=int, default=2)
    tested.add_argument("--min-agreement", type=float, default=0.8)
    tested.add_argument("--workers", type=int, default=2)
    tested.add_argument("--no-null", action="store_true")
    tested.add_argument("--out", required=True)
    tested.set_defaults(handle=run_ablation)
    return found


def main(argv: list[str] | None = None) -> None:
    args = parser().parse_args(argv)
    if args.command == "ablate":
        if args.samples < 1 or args.rounds < 1 or args.workers < 1:
            raise SystemExit("--samples, --rounds and --workers must be at least 1")
        if not 0 <= args.min_agreement <= 1:
            raise SystemExit("--min-agreement is a share between 0 and 1")
    sys.exit(args.handle(args))
