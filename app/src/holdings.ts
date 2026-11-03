import { Holding } from "./api";

const STORED = "pitlake.portfolio";

export const STARTER: Holding[] = [
  { symbol: "AAPL", quantity: 10 },
  { symbol: "NVDA", quantity: 20 },
  { symbol: "SPY", quantity: 5 },
  { symbol: "BTCUSDT", quantity: 0.05 },
];

export function stored(): Holding[] {
  try {
    const found = JSON.parse(localStorage.getItem(STORED) ?? "null");
    return Array.isArray(found) && found.length ? found : STARTER;
  } catch {
    return STARTER;
  }
}

export function remember(holdings: Holding[]) {
  try {
    localStorage.setItem(STORED, JSON.stringify(holdings));
  } catch {
    return;
  }
}
