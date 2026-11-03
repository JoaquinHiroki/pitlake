import { useEffect, useState } from "react";
import { api, Config } from "./api";
import { Ask } from "./pages/Ask";
import { Backtests, Coverage, Performance, Rejections, Venues } from "./pages/Data";
import { Digest } from "./pages/Digest";
import { Ingest } from "./pages/Ingest";

const PAGES = [
  ["ask", "Ask"],
  ["digest", "Digest"],
  ["coverage", "Coverage"],
  ["rejections", "Rejections"],
  ["venues", "Crypto venues"],
  ["backtests", "Backtests"],
  ["performance", "Performance"],
  ["ingest", "Ingest"],
] as const;

type Page = (typeof PAGES)[number][0];

const fromHash = (): Page => {
  const found = PAGES.find(([key]) => `#/${key}` === window.location.hash);
  return found ? found[0] : "ask";
};

export function App() {
  const [config, setConfig] = useState<Config | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState<Page>(fromHash);

  useEffect(() => {
    api.config().then(setConfig, (e: Error) => setError(e.message));
    const onHash = () => setPage(fromHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  useEffect(() => {
    if (config) document.title = `PITLake · ${config.catalog}`;
  }, [config]);

  if (error) return <p className="error page">The console could not load: {error}</p>;
  if (!config) return <p className="muted page">Loading…</p>;

  return (
    <div className="layout">
      <header className="top">
        <strong>PITLake</strong>
        <span className="muted">{config.catalog}</span>
        <nav>
          {PAGES.map(([key, label]) => (
            <a key={key} href={`#/${key}`} className={key === page ? "active" : undefined}>
              {label}
            </a>
          ))}
        </nav>
      </header>
      <main>
        {page === "ask" && <Ask spaces={config.spaces} />}
        {page === "digest" && <Digest />}
        {page === "coverage" && <Coverage />}
        {page === "rejections" && <Rejections />}
        {page === "venues" && <Venues />}
        {page === "backtests" && <Backtests />}
        {page === "performance" && <Performance catalog={config.catalog} />}
        {page === "ingest" && <Ingest />}
      </main>
    </div>
  );
}
