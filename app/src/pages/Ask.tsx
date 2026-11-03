import { FormEvent, KeyboardEvent, useCallback, useEffect, useRef, useState } from "react";
import { api, ApiError, Attachment, ConversationSummary, Message, Space, Table } from "../api";
import { DataTable } from "../components/DataTable";
import { LineChart, Series } from "../components/LineChart";
import { Markdown } from "../components/Markdown";

type Turn = { question: string; message?: Message; error?: string };
type Thread = { conversationId?: string; turns: Turn[] };

const STATUS: Record<string, string> = {
  SUBMITTED: "Submitted",
  FILTERING_CONTEXT: "Reading the space",
  FETCHING_METADATA: "Reading table metadata",
  ASKING_AI: "Writing the query",
  PENDING_WAREHOUSE: "Waiting for the warehouse",
  EXECUTING_QUERY: "Running the query",
};
const POLL_LIMIT_MS = 10 * 60 * 1000;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export function Ask({ spaces }: { spaces: Space[] }) {
  const [spaceKey, setSpaceKey] = useState(spaces[0].key);
  const [threads, setThreads] = useState<Record<string, Thread>>({});
  const [history, setHistory] = useState<ConversationSummary[]>([]);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [draft, setDraft] = useState("");
  const bottom = useRef<HTMLDivElement>(null);

  const space = spaces.find((s) => s.key === spaceKey)!;
  const thread = threads[spaceKey] ?? { turns: [] };
  const last = thread.turns[thread.turns.length - 1];
  const pending = !!last && !last.error && !last.message?.done;

  const loadHistory = useCallback(async () => {
    setHistoryError(null);
    try {
      setHistory(await api.conversations(spaceKey));
    } catch (e) {
      setHistoryError((e as Error).message);
    }
  }, [spaceKey]);

  useEffect(() => {
    loadHistory();
  }, [loadHistory]);

  useEffect(() => {
    bottom.current?.scrollIntoView({ behavior: "smooth" });
  }, [thread.turns.length, last?.message?.status]);

  const update = (key: string, change: (thread: Thread) => Thread) =>
    setThreads((all) => ({ ...all, [key]: change(all[key] ?? { turns: [] }) }));

  const updateTurn = (key: string, index: number, turn: Partial<Turn>) =>
    update(key, (t) => ({
      ...t,
      turns: t.turns.map((existing, i) => (i === index ? { ...existing, ...turn } : existing)),
    }));

  async function send(question: string) {
    const text = question.trim();
    if (!text || pending) return;
    const key = spaceKey;
    const index = thread.turns.length;
    const conversationId = thread.conversationId;
    setDraft("");
    update(key, (t) => ({ ...t, turns: [...t.turns, { question: text }] }));
    try {
      const started = await api.ask(key, text, conversationId);
      update(key, (t) => ({ ...t, conversationId: started.conversation_id }));
      if (!conversationId) loadHistory();
      const began = Date.now();
      for (let attempt = 0; ; attempt++) {
        const message = await api.message(key, started.conversation_id, started.message_id);
        updateTurn(key, index, { message });
        if (message.done) break;
        if (Date.now() - began > POLL_LIMIT_MS) throw new Error("Genie took more than 10 minutes.");
        await sleep(Math.min(1000 * 1.5 ** attempt, 5000));
      }
    } catch (e) {
      const limited = e instanceof ApiError && e.status === 429;
      const reason = limited
        ? "Genie's free tier takes about five questions a minute. Wait a moment and ask again."
        : (e as Error).message;
      updateTurn(key, index, { error: reason });
    }
  }

  async function open(conversation: ConversationSummary) {
    try {
      const messages = await api.messages(spaceKey, conversation.conversation_id);
      update(spaceKey, () => ({
        conversationId: conversation.conversation_id,
        turns: messages.map((m) => ({ question: m.question, message: m })),
      }));
    } catch (e) {
      setHistoryError((e as Error).message);
    }
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
    <div className="ask">
      <aside className="history">
        <div className="space-picker">
          {spaces.map((s) => (
            <button
              key={s.key}
              className={s.key === spaceKey ? "active" : undefined}
              onClick={() => setSpaceKey(s.key)}
            >
              {s.title}
            </button>
          ))}
        </div>
        <button onClick={() => update(spaceKey, () => ({ turns: [] }))} disabled={pending}>
          New conversation
        </button>
        <h3>Earlier conversations</h3>
        {historyError && <p className="error">{historyError}</p>}
        <ul>
          {history.map((c) => (
            <li key={c.conversation_id}>
              <button
                className={c.conversation_id === thread.conversationId ? "link active" : "link"}
                onClick={() => open(c)}
                disabled={pending}
              >
                {c.title}
              </button>
            </li>
          ))}
        </ul>
      </aside>

      <div className="chat">
        <p className="muted">{space.caption}</p>
        <div className="thread">
          {thread.turns.length === 0 && (
            <div className="starters">
              {space.starters.map((q) => (
                <button key={q} onClick={() => send(q)}>
                  {q}
                </button>
              ))}
            </div>
          )}
          {thread.turns.map((turn, i) => (
            <div key={i} className="turn">
              <div className="question">{turn.question}</div>
              <Reply
                space={spaceKey}
                turn={turn}
                isLast={i === thread.turns.length - 1}
                onAsk={send}
              />
            </div>
          ))}
          <div ref={bottom} />
        </div>
        <form className="composer" onSubmit={submit}>
          <textarea
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={onKeyDown}
            placeholder={
              thread.conversationId ? "Ask a follow-up" : `Ask the ${space.title.toLowerCase()} space`
            }
            rows={2}
          />
          <button type="submit" disabled={pending || !draft.trim()}>
            {pending ? "Answering…" : "Ask"}
          </button>
        </form>
      </div>
    </div>
  );
}

type ReplyProps = { space: string; turn: Turn; isLast: boolean; onAsk: (q: string) => void };

function Reply({ space, turn, isLast, onAsk }: ReplyProps) {
  const message = turn.message;
  if (turn.error) return <p className="error">{turn.error}</p>;
  if (!message || !message.done)
    return <p className="status">{STATUS[message?.status ?? "SUBMITTED"] ?? message?.status}…</p>;
  return (
    <div className="reply">
      {message.error && <p className="error">{message.error}</p>}
      {message.status !== "COMPLETED" && !message.error && (
        <p className="error">Genie stopped: {message.status.toLowerCase()}.</p>
      )}
      {message.attachments.map((attachment, i) => (
        <AttachmentView
          key={i}
          space={space}
          message={message}
          attachment={attachment}
          isLast={isLast}
          onAsk={onAsk}
        />
      ))}
      <Feedback space={space} message={message} />
    </div>
  );
}

type AttachmentProps = {
  space: string;
  message: Message;
  attachment: Attachment;
  isLast: boolean;
  onAsk: (q: string) => void;
};

function AttachmentView({ space, message, attachment, isLast, onAsk }: AttachmentProps) {
  if (attachment.kind === "text") return <Markdown text={attachment.content} />;
  if (attachment.kind === "suggestions")
    return isLast ? (
      <div className="suggestions">
        {attachment.questions.map((q) => (
          <button key={q} onClick={() => onAsk(q)}>
            {q}
          </button>
        ))}
      </div>
    ) : null;
  return (
    <div className="query">
      {attachment.title && <h4>{attachment.title}</h4>}
      {attachment.description && <p>{attachment.description}</p>}
      <details>
        <summary>SQL</summary>
        <pre>{attachment.sql}</pre>
      </details>
      {message.status === "COMPLETED" && (
        <QueryResult space={space} message={message} attachmentId={attachment.id} />
      )}
    </div>
  );
}

function QueryResult({
  space,
  message,
  attachmentId,
}: {
  space: string;
  message: Message;
  attachmentId: string;
}) {
  const [table, setTable] = useState<Table | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api
      .result(space, message.conversation_id, message.message_id, attachmentId)
      .then(setTable)
      .catch((e: Error) => setError(e.message));
  }, [space, message.conversation_id, message.message_id, attachmentId]);

  if (error) return <p className="error">{error}</p>;
  if (!table) return <p className="status">Loading the result…</p>;
  return <ResultView table={table} />;
}

const TIMES = new Set(["DATE", "TIMESTAMP", "TIMESTAMP_NTZ"]);
const NUMBERS = new Set(["BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DECIMAL"]);
const PREFERRED_X = ["session_date", "day", "usage_date", "period_end", "event_time", "bar_time"];
const GROUPS = /^(symbol|entity|venue|ticker|pair|feature|metric|workload|job_name)$/;
const MAX_SERIES = 3;

type Shape = { x: number; measures: number[]; group: number | null };

function chartShape(table: Table): Shape | null {
  const types = table.columns.map((c) => (c.type ?? "").toUpperCase());
  const times = types.flatMap((t, i) => (TIMES.has(t) ? [i] : []));
  const named = PREFERRED_X.map((n) => table.columns.findIndex((c) => c.name === n));
  const x = named.find((i) => times.includes(i)) ?? times[0];
  if (x === undefined) return null;
  const measures = types.flatMap((t, i) => (NUMBERS.has(t) && i !== x ? [i] : []));
  if (measures.length === 0) return null;
  const distinct = (i: number) => new Set(table.rows.map((r) => r[i])).size;
  const groups = table.columns.flatMap((c, i) =>
    types[i] === "STRING" && GROUPS.test(c.name) && distinct(i) > 1 ? [i] : [],
  );
  if (groups.length > 1 || (groups.length === 1 && distinct(groups[0]) > MAX_SERIES)) return null;
  const group = groups[0] ?? null;
  const points = new Set(table.rows.map((r) => `${r[x]}|${group === null ? "" : r[group]}`));
  if (points.size !== table.rows.length || distinct(x) < 3) return null;
  return { x, measures, group };
}

function chartSeries(table: Table, shape: Shape, measure: number): Series[] {
  const groups = new Map<string, Series>();
  for (const row of table.rows) {
    const time = Date.parse(String(row[shape.x]));
    const value = row[measure];
    if (Number.isNaN(time) || typeof value !== "number") continue;
    const name = shape.group === null ? table.columns[measure].name : String(row[shape.group]);
    if (!groups.has(name)) groups.set(name, { name, points: [] });
    groups.get(name)!.points.push({ x: time, y: value });
  }
  return [...groups.values()].map((s) => ({ ...s, points: s.points.sort((a, b) => a.x - b.x) }));
}

function ResultView({ table }: { table: Table }) {
  const shape = chartShape(table);
  const [asChart, setAsChart] = useState(true);
  const [measure, setMeasure] = useState(shape?.measures[0] ?? 0);
  if (!shape) return <DataTable table={table} />;
  return (
    <div className="result">
      <div className="controls">
        <div className="toggle">
          <button className={asChart ? "active" : undefined} onClick={() => setAsChart(true)}>
            Chart
          </button>
          <button className={asChart ? undefined : "active"} onClick={() => setAsChart(false)}>
            Table
          </button>
        </div>
        {asChart && shape.measures.length > 1 && (
          <select value={measure} onChange={(e) => setMeasure(Number(e.target.value))}>
            {shape.measures.map((i) => (
              <option key={i} value={i}>
                {table.columns[i].name}
              </option>
            ))}
          </select>
        )}
      </div>
      {asChart ? (
        <LineChart series={chartSeries(table, shape, measure)} />
      ) : (
        <DataTable table={table} />
      )}
    </div>
  );
}

function Feedback({ space, message }: { space: string; message: Message }) {
  const [rating, setRating] = useState(message.feedback ?? "NONE");
  const [error, setError] = useState<string | null>(null);

  async function rate(next: "positive" | "negative") {
    const value = rating === next.toUpperCase() ? "none" : next;
    try {
      await api.feedback(space, message.conversation_id, message.message_id, value);
      setRating(value.toUpperCase());
    } catch (e) {
      setError((e as Error).message);
    }
  }

  return (
    <div className="feedback">
      <button className={rating === "POSITIVE" ? "active" : undefined} onClick={() => rate("positive")}>
        Helpful
      </button>
      <button className={rating === "NEGATIVE" ? "active" : undefined} onClick={() => rate("negative")}>
        Not helpful
      </button>
      {error && <span className="error">{error}</span>}
    </div>
  );
}
