export type Column = { name: string; type: string | null };

export type Table = {
  columns: Column[];
  rows: unknown[][];
  row_count: number;
  truncated: boolean;
};

export type Space = { key: string; title: string; caption: string; starters: string[] };

export type Config = {
  catalog: string;
  host: string;
  ingest_job_id: number;
  spaces: Space[];
};

export type Attachment =
  | { kind: "text"; id: string; content: string }
  | {
      kind: "query";
      id: string;
      title: string | null;
      description: string | null;
      sql: string;
      row_count: number | null;
    }
  | { kind: "suggestions"; questions: string[] };

export type Message = {
  conversation_id: string;
  message_id: string;
  question: string;
  status: string;
  done: boolean;
  error: string | null;
  feedback: string | null;
  created: number | null;
  attachments: Attachment[];
};

export type ConversationSummary = { conversation_id: string; title: string; created: number | null };

export type IngestRun = {
  run_id: number;
  started: string;
  state: string;
  result: string;
  parameters: Record<string, string>;
};

export type DigestSymbol = {
  symbol: string;
  name: string;
  venue: string;
  first_session: string | null;
  last_session: string | null;
};

export type KnownValue = {
  group: string;
  feature: string;
  label: string;
  period: string;
  shown: string;
  knowable_at: string;
  source: string | null;
};

export type Filing = {
  form: string;
  accession_number: string;
  period_end: string;
  knowable_at: string;
  known: boolean;
};

export type Digest = {
  symbol: string;
  name: string;
  venue: string;
  as_of: string;
  chat: boolean;
  bars: { session_date: string; close: number | null; known: boolean }[];
  moves: {
    last_session: string | null;
    last_close: number | null;
    changes: { sessions: number; change: number }[];
    high: number | null;
    low: number | null;
  };
  snapshot: KnownValue[];
  filings: Filing[];
};

export type DigestSummary = { text: string; endpoint: string; facts: string[] };

export type Holding = { symbol: string; quantity: number };

export type Position = {
  symbol: string;
  name: string;
  quantity: number;
  last_session: string | null;
  last_close: number | null;
  value: number | null;
  weight: number | null;
  changes: { sessions: number; change: number }[];
};

export type Portfolio = {
  as_of: string;
  chat: boolean;
  value: number | null;
  last_day: string | null;
  changes: { period: string; change: number }[];
  positions: Position[];
  series: { date: string; value: number; known: boolean }[];
};

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: { "Content-Type": "application/json", ...init?.headers },
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    const detail = typeof body.detail === "string" ? body.detail : response.statusText;
    throw new ApiError(detail, response.status);
  }
  return response.json();
}

const post = <T>(path: string, body: unknown) =>
  request<T>(path, { method: "POST", body: JSON.stringify(body) });

const conversationPath = (space: string, conversation: string) =>
  `/api/genie/${space}/conversations/${conversation}`;

export const api = {
  config: () => request<Config>("/api/config"),

  view: (name: string, params: Record<string, string | number> = {}, refresh = false) => {
    const query = new URLSearchParams(
      Object.entries({ ...params, ...(refresh ? { refresh: "true" } : {}) }).map(([k, v]) => [
        k,
        String(v),
      ]),
    );
    return request<Table>(`/api/views/${name}?${query}`);
  },

  ask: (space: string, question: string, conversationId?: string) =>
    post<{ conversation_id: string; message_id: string }>(`/api/genie/${space}/messages`, {
      question,
      conversation_id: conversationId ?? null,
    }),

  message: (space: string, conversation: string, message: string) =>
    request<Message>(`${conversationPath(space, conversation)}/messages/${message}`),

  result: (space: string, conversation: string, message: string, attachment: string) =>
    request<Table>(
      `${conversationPath(space, conversation)}/messages/${message}/attachments/${attachment}/result`,
    ),

  feedback: (space: string, conversation: string, message: string, rating: string) =>
    post(`${conversationPath(space, conversation)}/messages/${message}/feedback`, { rating }),

  conversations: (space: string) =>
    request<ConversationSummary[]>(`/api/genie/${space}/conversations`),

  messages: (space: string, conversation: string) =>
    request<Message[]>(conversationPath(space, conversation)),

  ingest: (body: {
    datasets: string[];
    start: string | null;
    end: string | null;
    reload: boolean;
    max_files: number;
  }) => post<{ run_id: number; url: string }>("/api/ingest", body),

  digestSymbols: () => request<DigestSymbol[]>("/api/digest/symbols"),

  digest: (symbol: string, asOf: string) =>
    request<Digest>(`/api/digest?${new URLSearchParams({ symbol, as_of: asOf })}`),

  digestSummary: (symbol: string, asOf: string) =>
    post<DigestSummary>("/api/digest/summary", { symbol, as_of: asOf }),

  portfolio: (holdings: Holding[], asOf: string) =>
    post<Portfolio>("/api/portfolio", { holdings, as_of: asOf }),

  portfolioSummary: (holdings: Holding[], asOf: string) =>
    post<DigestSummary>("/api/portfolio/summary", { holdings, as_of: asOf }),

  ingestRuns: () => request<IngestRun[]>("/api/ingest/runs"),
};
