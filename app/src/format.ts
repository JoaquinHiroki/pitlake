const DAY_MS = 24 * 60 * 60 * 1000;

export const price = (value: number) =>
  value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });

export const percent = (value: number) =>
  `${value >= 0 ? "+" : "−"}${Math.abs(value * 100).toFixed(2)}%`;

export const minute = (instant: string) => instant.slice(0, 16).replace("T", " ");

export const nextDay = (day: string) =>
  new Date(Date.parse(day) + DAY_MS).toISOString().slice(0, 10);
