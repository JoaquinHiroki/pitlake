import { useEffect, useState } from "react";
import { api, Digest as DigestData, DigestSummary, DigestSymbol } from "../api";
import { LineChart } from "../components/LineChart";
import { minute, nextDay, percent, price } from "../format";

export function Digest() {
  const [symbols, setSymbols] = useState<DigestSymbol[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [symbol, setSymbol] = useState("AAPL");
  const [day, setDay] = useState("");
  const [time, setTime] = useState("00:00");
  const [digest, setDigest] = useState<DigestData | null>(null);
  const [loading, setLoading] = useState(false);
  const [reveal, setReveal] = useState(false);
  const [summary, setSummary] = useState<DigestSummary | null>(null);
  const [writing, setWriting] = useState(false);
  const [summaryError, setSummaryError] = useState<string | null>(null);

  const chosen = symbols.find((s) => s.symbol === symbol);
  const asOf = day && `${day}T${time}`;

  useEffect(() => {
    api.digestSymbols().then(setSymbols, (e: Error) => setError(e.message));
  }, []);

  useEffect(() => {
    if (!day && chosen?.last_session) setDay(nextDay(chosen.last_session));
  }, [chosen, day]);

  useEffect(() => {
    if (!asOf) return;
    let current = true;
    setLoading(true);
    setError(null);
    setSummary(null);
    setSummaryError(null);
    api
      .digest(symbol, asOf)
      .then((found) => current && setDigest(found))
      .catch((e: Error) => current && setError(e.message))
      .finally(() => current && setLoading(false));
    return () => {
      current = false;
    };
  }, [symbol, asOf]);

  async function write() {
    setWriting(true);
    setSummaryError(null);
    try {
      setSummary(await api.digestSummary(symbol, asOf));
    } catch (e) {
      setSummaryError((e as Error).message);
    } finally {
      setWriting(false);
    }
  }

  const asOfMs = digest ? Date.parse(digest.as_of) : 0;
  const points = (digest?.bars ?? [])
    .filter((b) => b.close !== null && (reveal || b.known))
    .map((b) => ({ x: Date.parse(b.session_date), y: b.close as number }));
  const lastKnown = (digest?.bars ?? []).filter((b) => b.known).slice(-1)[0];
  const hidden = (digest?.bars ?? []).filter((b) => !b.known).length;
  const upcoming = digest?.filings.find((f) => !f.known);
  const known = digest?.snapshot.filter((r) => r.group !== "prices") ?? [];
  const priceRows = digest?.snapshot.filter((r) => r.group === "prices") ?? [];

  return (
    <>
      <section>
        <h2>Point-in-time digest</h2>
        <p className="muted">
          What could be said about a symbol at one instant, from the Gold values public by then:
          the price move, the latest reported results and the macro backdrop. Nothing published
          after the instant reaches the page's facts or the written digest (ADR 0020).
        </p>
        <div className="controls">
          <label className="inline">
            Symbol
            <select value={symbol} onChange={(e) => setSymbol(e.target.value)}>
              {(symbols.length ? symbols : [{ symbol, name: symbol } as DigestSymbol]).map((s) => (
                <option key={s.symbol} value={s.symbol}>
                  {s.symbol} · {s.name}
                </option>
              ))}
            </select>
          </label>
          <label className="inline">
            As of (UTC)
            <input
              type="date"
              value={day}
              min={chosen?.first_session ?? undefined}
              onChange={(e) => setDay(e.target.value)}
            />
            <input type="time" value={time} onChange={(e) => setTime(e.target.value || "00:00")} />
          </label>
          {chosen?.last_session && (
            <span className="muted">
              Gold holds {chosen.symbol} sessions {chosen.first_session} to {chosen.last_session}.
            </span>
          )}
        </div>
        {error && <p className="error">{error}</p>}
      </section>

      {digest && (
        <div className={loading ? "refreshing" : undefined}>
          <section>
            <div className="tiles">
              <div className="tile">
                <span className="muted">Last close known</span>
                <strong>
                  {digest.moves.last_close === null ? "—" : price(digest.moves.last_close)}
                </strong>
                <span className="muted">{digest.moves.last_session ?? "no session yet"}</span>
              </div>
              {digest.moves.changes.map((m) => (
                <div className="tile" key={m.sessions}>
                  <span className="muted">
                    {m.sessions} session{m.sessions > 1 ? "s" : ""}
                  </span>
                  <strong className={m.change >= 0 ? "success" : "error"}>
                    {m.change >= 0 ? "▲" : "▼"} {percent(m.change)}
                  </strong>
                </div>
              ))}
            </div>
          </section>

          <section>
            <header className="section-head">
              <h2>
                {digest.symbol} closes on {digest.venue}
              </h2>
              <label className="inline">
                <input
                  type="checkbox"
                  checked={reveal}
                  onChange={(e) => setReveal(e.target.checked)}
                  disabled={hidden === 0}
                />
                Reveal the {hidden} session{hidden === 1 ? "" : "s"} after the instant
              </label>
            </header>
            <p className="muted">
              {reveal
                ? "The shaded part became public after the instant: the digest never sees it."
                : `Only sessions closed by ${minute(digest.as_of)} UTC` +
                  (lastKnown ? `, the last on ${lastKnown.session_date}.` : ".")}
            </p>
            <LineChart
              series={[{ name: `${digest.symbol} close`, points }]}
              marker={{ x: asOfMs, label: "as of" }}
              dimAfter={reveal ? (lastKnown ? Date.parse(lastKnown.session_date) : 0) : undefined}
              formatY={price}
            />
          </section>

          <section>
            <header className="section-head">
              <h2>Digest</h2>
              <button onClick={write} disabled={writing || !digest.chat}>
                {writing ? "Writing…" : summary ? "Written" : "Write the digest"}
              </button>
            </header>
            {!digest.chat && (
              <p className="muted">The app has no chat endpoint, so it shows the facts only.</p>
            )}
            {summaryError && <p className="error">{summaryError}</p>}
            {summary && (
              <div className="digest">
                <p>{summary.text}</p>
                <details>
                  <summary className="muted">
                    Written by {summary.endpoint} from {summary.facts.length} facts, all public by{" "}
                    {minute(digest.as_of)} UTC. Informational only, not advice.
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

          <section>
            <h2>Known at the instant</h2>
            {known.length + priceRows.length === 0 ? (
              <p className="muted">Gold held nothing for {digest.symbol} by then.</p>
            ) : (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Value</th>
                      <th>For</th>
                      <th className="num">Shown</th>
                      <th>Public since (UTC)</th>
                      <th>Source</th>
                    </tr>
                  </thead>
                  <tbody>
                    {[...priceRows, ...known].map((r) => (
                      <tr key={r.feature}>
                        <td>{r.label}</td>
                        <td>{r.period}</td>
                        <td className="num">{r.shown}</td>
                        <td>{minute(r.knowable_at)}</td>
                        <td className="muted">{r.source ?? "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          {digest.filings.length > 0 && (
            <section>
              <h2>Filings</h2>
              <ul className="filings">
                {digest.filings.map((f) => (
                  <li key={f.accession_number} className={f.known ? undefined : "unknown"}>
                    <strong>{f.form}</strong> for the period ending {f.period_end}, accepted{" "}
                    {minute(f.knowable_at)} UTC <span className="muted">{f.accession_number}</span>
                    {!f.known && <em> · not yet public at the instant</em>}
                  </li>
                ))}
              </ul>
              {upcoming && (
                <p className="muted">
                  The digest cannot use the {upcoming.form} for {upcoming.period_end}: it was
                  accepted after the instant.
                </p>
              )}
            </section>
          )}
        </div>
      )}
    </>
  );
}
