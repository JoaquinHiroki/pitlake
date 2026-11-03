import { useEffect, useState } from "react";
import { api, DigestSummary, DigestSymbol, Holding, Portfolio as PortfolioData } from "../api";
import { LineChart } from "../components/LineChart";
import { minute, nextDay, percent, price } from "../format";

const STORED = "pitlake.portfolio";
const STARTER: Holding[] = [
  { symbol: "AAPL", quantity: 10 },
  { symbol: "NVDA", quantity: 20 },
  { symbol: "SPY", quantity: 5 },
  { symbol: "BTCUSDT", quantity: 0.05 },
];
const SESSIONS = [1, 20];

type Row = { symbol: string; quantity: string };

function stored(): Holding[] {
  try {
    const found = JSON.parse(localStorage.getItem(STORED) ?? "null");
    return Array.isArray(found) && found.length ? found : STARTER;
  } catch {
    return STARTER;
  }
}

function remember(holdings: Holding[]) {
  try {
    localStorage.setItem(STORED, JSON.stringify(holdings));
  } catch {
    return;
  }
}

function Change({ value }: { value: number | undefined }) {
  if (value === undefined) return <span className="muted">—</span>;
  return (
    <span className={value >= 0 ? "success" : "error"}>
      {value >= 0 ? "▲" : "▼"} {percent(value)}
    </span>
  );
}

export function Portfolio() {
  const [symbols, setSymbols] = useState<DigestSymbol[]>([]);
  const [holdings, setHoldings] = useState<Holding[]>(stored);
  const [rows, setRows] = useState<Row[]>(() =>
    stored().map((h) => ({ symbol: h.symbol, quantity: String(h.quantity) })),
  );
  const [day, setDay] = useState("");
  const [time, setTime] = useState("00:00");
  const [data, setData] = useState<PortfolioData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [reveal, setReveal] = useState(false);
  const [summary, setSummary] = useState<DigestSummary | null>(null);
  const [writing, setWriting] = useState(false);
  const [summaryError, setSummaryError] = useState<string | null>(null);

  const asOf = day && `${day}T${time}`;
  const key = JSON.stringify(holdings);

  useEffect(() => {
    api.digestSymbols().then(setSymbols, (e: Error) => setError(e.message));
  }, []);

  useEffect(() => {
    if (day || symbols.length === 0) return;
    const held = new Set(holdings.map((h) => h.symbol));
    const last = symbols
      .filter((s) => held.has(s.symbol) && s.last_session)
      .map((s) => s.last_session as string)
      .sort()
      .pop();
    if (last) setDay(nextDay(last));
  }, [symbols, holdings, day]);

  useEffect(() => {
    if (!asOf) return;
    let current = true;
    setLoading(true);
    setError(null);
    setSummary(null);
    setSummaryError(null);
    api
      .portfolio(JSON.parse(key), asOf)
      .then((found) => current && setData(found))
      .catch((e: Error) => current && setError(e.message))
      .finally(() => current && setLoading(false));
    return () => {
      current = false;
    };
  }, [key, asOf]);

  function apply() {
    const parsed = rows.map((r) => ({ symbol: r.symbol, quantity: Number(r.quantity) }));
    const wrong = parsed.find((h) => !(h.quantity > 0));
    if (wrong) {
      setError(`The quantity of ${wrong.symbol} must be a number above 0.`);
      return;
    }
    remember(parsed);
    setHoldings(parsed);
  }

  async function write() {
    setWriting(true);
    setSummaryError(null);
    try {
      setSummary(await api.portfolioSummary(holdings, asOf));
    } catch (e) {
      setSummaryError((e as Error).message);
    } finally {
      setWriting(false);
    }
  }

  const free = symbols.filter((s) => !rows.some((r) => r.symbol === s.symbol));
  const changed = JSON.stringify(rows.map((r) => [r.symbol, Number(r.quantity)])) !==
    JSON.stringify(holdings.map((h) => [h.symbol, h.quantity]));
  const series = data?.series ?? [];
  const points = series
    .filter((p) => reveal || p.known)
    .map((p) => ({ x: Date.parse(p.date), y: p.value }));
  const hidden = series.filter((p) => !p.known).length;
  const missing = data?.positions.filter((p) => p.value === null) ?? [];

  return (
    <>
      <section>
        <h2>Portfolio digest</h2>
        <p className="muted">
          A portfolio valued and described as it could have been at one instant: each position at
          its last close public by then, and a digest written only from facts known then
          (ADR 0020). Holdings stay in this browser.
        </p>
        <div className="holdings">
          {rows.map((row, i) => (
            <div className="controls" key={i}>
              <select
                value={row.symbol}
                onChange={(e) =>
                  setRows(rows.map((r, j) => (j === i ? { ...r, symbol: e.target.value } : r)))
                }
              >
                {[{ symbol: row.symbol, name: "" } as DigestSymbol, ...free].map((s) => (
                  <option key={s.symbol} value={s.symbol}>
                    {s.symbol}
                  </option>
                ))}
              </select>
              <input
                type="number"
                min="0"
                step="any"
                value={row.quantity}
                aria-label={`Quantity of ${row.symbol}`}
                onChange={(e) =>
                  setRows(rows.map((r, j) => (j === i ? { ...r, quantity: e.target.value } : r)))
                }
                onKeyDown={(e) => e.key === "Enter" && apply()}
              />
              <button onClick={() => setRows(rows.filter((_, j) => j !== i))} disabled={rows.length < 2}>
                Remove
              </button>
            </div>
          ))}
          <div className="controls">
            <button
              onClick={() => setRows([...rows, { symbol: free[0].symbol, quantity: "1" }])}
              disabled={free.length === 0}
            >
              Add position
            </button>
            <button className={changed ? "active" : undefined} onClick={apply} disabled={!changed}>
              Update portfolio
            </button>
            <label className="inline">
              As of (UTC)
              <input type="date" value={day} onChange={(e) => setDay(e.target.value)} />
              <input
                type="time"
                value={time}
                onChange={(e) => setTime(e.target.value || "00:00")}
              />
            </label>
          </div>
        </div>
        {error && <p className="error">{error}</p>}
      </section>

      {data && (
        <div className={loading ? "refreshing" : undefined}>
          <section>
            <div className="tiles">
              <div className="tile">
                <span className="muted">Value known</span>
                <strong>{data.value === null ? "—" : `$${price(data.value)}`}</strong>
                <span className="muted">{data.last_day ?? "no close yet"}</span>
              </div>
              {data.changes.map((c) => (
                <div className="tile" key={c.period}>
                  <span className="muted">{c.period}</span>
                  <strong>
                    <Change value={c.change} />
                  </strong>
                </div>
              ))}
            </div>
            {missing.length > 0 && (
              <p className="muted">
                No close of {missing.map((p) => p.symbol).join(", ")} was public at the instant,
                so the value leaves {missing.length === 1 ? "it" : "them"} out.
              </p>
            )}
          </section>

          <section>
            <header className="section-head">
              <h2>Value over time</h2>
              <label className="inline">
                <input
                  type="checkbox"
                  checked={reveal}
                  onChange={(e) => setReveal(e.target.checked)}
                  disabled={hidden === 0}
                />
                Reveal the {hidden} day{hidden === 1 ? "" : "s"} after the instant
              </label>
            </header>
            <p className="muted">
              {reveal
                ? "The shaded part became public after the instant: the digest never sees it."
                : `Each day's value from the closes public by ${minute(data.as_of)} UTC.`}
            </p>
            <LineChart
              series={[{ name: "Portfolio value", points }]}
              marker={{ x: Date.parse(data.as_of), label: "as of" }}
              dimAfter={reveal && data.last_day ? Date.parse(data.last_day) : undefined}
              formatY={(v) => `$${price(v)}`}
            />
          </section>

          <section>
            <h2>Positions</h2>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th className="num">Quantity</th>
                    <th className="num">Last close known</th>
                    <th className="num">Value</th>
                    <th>Weight</th>
                    {SESSIONS.map((n) => (
                      <th key={n} className="num">
                        {n} session{n > 1 ? "s" : ""}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.positions.map((p) => (
                    <tr key={p.symbol}>
                      <td>
                        <strong>{p.symbol}</strong> <span className="muted">{p.name}</span>
                      </td>
                      <td className="num">{p.quantity.toLocaleString()}</td>
                      <td className="num">
                        {p.last_close === null ? "—" : price(p.last_close)}{" "}
                        <span className="muted">{p.last_session ?? ""}</span>
                      </td>
                      <td className="num">{p.value === null ? "—" : `$${price(p.value)}`}</td>
                      <td>
                        {p.weight !== null && (
                          <span className="weight">
                            <span className="track">
                              <i style={{ width: `${p.weight * 100}%` }} />
                            </span>
                            {(p.weight * 100).toFixed(1)}%
                          </span>
                        )}
                      </td>
                      {SESSIONS.map((n) => (
                        <td key={n} className="num">
                          <Change value={p.changes.find((c) => c.sessions === n)?.change} />
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section>
            <header className="section-head">
              <h2>Digest</h2>
              <button onClick={write} disabled={writing || !data.chat || changed}>
                {writing ? "Writing…" : summary ? "Written" : "Write the digest"}
              </button>
            </header>
            {!data.chat && (
              <p className="muted">The app has no chat endpoint, so it shows the facts only.</p>
            )}
            {summaryError && <p className="error">{summaryError}</p>}
            {summary && (
              <div className="digest">
                <p>{summary.text}</p>
                <details>
                  <summary className="muted">
                    Written by {summary.endpoint} from {summary.facts.length} facts, all public by{" "}
                    {minute(data.as_of)} UTC. Informational only, not advice.
                  </summary>
                  <ul>
                    {summary.facts.map((f) => (
                      <li key={f}>{f}</li>
                    ))}
                  </ul>
                </details>
              </div>
            )}
          </section>
        </div>
      )}
    </>
  );
}
