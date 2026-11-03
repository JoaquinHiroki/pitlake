import { FormEvent, KeyboardEvent, useEffect, useRef, useState } from "react";
import { AgentRun, AgentStep, api, ApiError, DigestSymbol } from "../api";
import { LineChart } from "../components/LineChart";
import { Markdown } from "../components/Markdown";
import { minute, nextDay, price } from "../format";
import { stored } from "../holdings";

type Turn = { question: string; run?: AgentRun; error?: string };

const STARTERS = [
  "How did my portfolio do over the last week, and what drove it?",
  "Compare Apple, NVIDIA and the S&P 500 ETF over the last three months.",
  "What did Apple last report, and how has the stock moved since?",
  "Will bitcoin go up next week?",
];
const POLL_LIMIT_MS = 5 * 60 * 1000;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function signature(step: AgentStep) {
  const values = Object.values(step.arguments).map((v) => (Array.isArray(v) ? v.join(", ") : String(v)));
  return `${step.tool}(${values.join(", ")})`;
}

function Step({ step }: { step: AgentStep }) {
  const view = step.view;
  return (
    <li className={step.error ? "step failed" : "step"}>
      <code>{signature(step)}</code>{" "}
      <span className="muted">{!step.done ? "running…" : step.error ? `failed: ${step.error}` : "✓"}</span>
      {step.result && (
        <details>
          <summary className="muted">What the tool returned</summary>
          <pre>{step.result}</pre>
        </details>
      )}
      {view && view.series.some((s) => s.points.length > 1) && (
        <div className="step-chart">
          <h4>{view.title}</h4>
          <LineChart
            height={220}
            series={view.series.map((s) => ({
              name: s.name,
              points: s.points.map(([day, value]) => ({ x: Date.parse(day), y: value })),
            }))}
            formatY={view.indexed ? (v) => v.toFixed(0) : price}
          />
        </div>
      )}
    </li>
  );
}

function Reply({ turn }: { turn: Turn }) {
  if (turn.error) return <p className="error">{turn.error}</p>;
  const run = turn.run;
  const running = !run || !run.done;
  const busy = run?.steps.find((s) => !s.done);
  return (
    <div className="reply">
      {run && run.steps.length > 0 && (
        <ol className="trace">
          {run.steps.map((step, i) => (
            <Step key={i} step={step} />
          ))}
        </ol>
      )}
      {running && (
        <p className="status">
          {!run || run.steps.length === 0
            ? "Planning…"
            : busy
              ? `Calling ${busy.tool}…`
              : "Reading the results…"}
        </p>
      )}
      {run?.error && <p className="error">The agent stopped: {run.error}</p>}
      {run?.answer && <Markdown text={run.answer} />}
      {run?.warnings.map((w) => (
        <p key={w} className="error">
          ⚠ {w}
        </p>
      ))}
    </div>
  );
}

export function Agent() {
  const [symbols, setSymbols] = useState<DigestSymbol[]>([]);
  const [day, setDay] = useState("");
  const [time, setTime] = useState("00:00");
  const [turns, setTurns] = useState<Turn[]>([]);
  const [draft, setDraft] = useState("");
  const bottom = useRef<HTMLDivElement>(null);

  const asOf = day && `${day}T${time}`;
  const last = turns[turns.length - 1];
  const pending = !!last && !last.error && !last.run?.done;

  useEffect(() => {
    api.digestSymbols().then(setSymbols, () => setSymbols([]));
  }, []);

  useEffect(() => {
    if (day || symbols.length === 0) return;
    const latest = symbols
      .map((s) => s.last_session)
      .filter((s): s is string => !!s)
      .sort()
      .pop();
    if (latest) setDay(nextDay(latest));
  }, [symbols, day]);

  useEffect(() => {
    bottom.current?.scrollIntoView({ behavior: "smooth" });
  }, [turns.length, last?.run?.steps.length, last?.run?.done]);

  const updateLast = (change: Partial<Turn>) =>
    setTurns((all) => all.map((t, i) => (i === all.length - 1 ? { ...t, ...change } : t)));

  async function send(question: string) {
    const text = question.trim();
    if (!text || pending || !asOf) return;
    const history = turns.flatMap((t) =>
      t.run?.answer
        ? [
            { role: "user" as const, content: t.question },
            { role: "assistant" as const, content: t.run.answer },
          ]
        : [],
    );
    setDraft("");
    setTurns((all) => [...all, { question: text }]);
    try {
      const started = await api.agent(asOf, [...history, { role: "user", content: text }], stored());
      const began = Date.now();
      for (;;) {
        const run = await api.agentRun(started.run_id);
        updateLast({ run });
        if (run.done) break;
        if (Date.now() - began > POLL_LIMIT_MS) throw new Error("The agent took more than 5 minutes.");
        await sleep(1000);
      }
    } catch (e) {
      const limited = e instanceof ApiError && e.status === 429;
      updateLast({
        error: limited
          ? "The model endpoint is rate limited. Wait a moment and ask again."
          : (e as Error).message,
      });
    }
  }

  function changeInstant(nextDayValue: string, nextTime: string) {
    setDay(nextDayValue);
    setTime(nextTime || "00:00");
    setTurns([]);
  }

  function submit(event: FormEvent) {
    event.preventDefault();
    send(draft);
  }

  function onKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      send(draft);
    }
  }

  return (
    <div className="agent">
      <section>
        <h2>Time-locked agent</h2>
        <p className="muted">
          A research assistant that plans, calls PITLake's tools and answers, at one instant. The
          instant is fixed on the server, no tool takes a date, and every tool reads only what was
          public by then, so the agent cannot see the future even when asked (ADR 0021). Your
          positions come from the Portfolio page.
        </p>
        <div className="controls">
          <label className="inline">
            <span className="lock">🔒 Locked to (UTC)</span>
            <input
              type="date"
              value={day}
              disabled={pending}
              onChange={(e) => changeInstant(e.target.value, time)}
            />
            <input
              type="time"
              value={time}
              disabled={pending}
              onChange={(e) => changeInstant(day, e.target.value)}
            />
          </label>
          <button onClick={() => setTurns([])} disabled={pending || turns.length === 0}>
            New conversation
          </button>
          {asOf && <span className="muted">Changing the instant starts a new conversation.</span>}
        </div>
      </section>

      <div className="thread">
        {turns.length === 0 && (
          <div className="starters">
            {STARTERS.map((q) => (
              <button key={q} onClick={() => send(q)} disabled={!asOf}>
                {q}
              </button>
            ))}
          </div>
        )}
        {turns.map((turn, i) => (
          <div key={i} className="turn">
            <div className="question">{turn.question}</div>
            <Reply turn={turn} />
            {turn.run?.done && !turn.run.error && (
              <p className="muted small">
                Answered at {minute(turn.run.as_of)} UTC with {turn.run.steps.length} tool call
                {turn.run.steps.length === 1 ? "" : "s"}. Informational only, not advice.
              </p>
            )}
          </div>
        ))}
        <div ref={bottom} />
      </div>

      <form className="composer" onSubmit={submit}>
        <textarea
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={onKeyDown}
          placeholder={turns.length ? "Ask a follow-up" : "Ask the agent"}
          rows={2}
        />
        <button type="submit" disabled={pending || !draft.trim() || !asOf}>
          {pending ? "Working…" : "Ask"}
        </button>
      </form>
    </div>
  );
}
