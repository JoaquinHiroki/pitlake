import re
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from pitlake.documents import (
    CHUNK_CHARS,
    OVERLAP_CHARS,
    chunk_document,
    chunk_id,
    context_header,
    long_date,
)
from pitlake.questions import answer_pattern

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
EMBED_BATCH = 8
DOCUMENTS_PER_PARTITION = 2
CHUNK_FIELDS = (
    ("chunk_id", "STRING"),
    ("chunk_index", "INT"),
    ("section", "STRING"),
    ("context", "STRING"),
    ("text", "STRING"),
)
CHUNK_UDF_SCHEMA = "ARRAY<STRUCT<" + ", ".join(f"{n}: {t}" for n, t in CHUNK_FIELDS) + ">>"
INDEX_COLUMNS = (
    "chunk_id",
    "symbol",
    "accession_number",
    "form",
    "filing_date",
    "section",
    "knowable_at_us",
    "context",
    "text",
)
SCORE = "score"
EXCERPTS = 5
SYSTEM_PROMPT = (
    "You answer questions about companies from excerpts of their SEC filings. It is {now}: "
    "nothing published after that exists, so use only the numbered excerpts, never what you may "
    "remember. Answer in one or two sentences, quote figures exactly as the excerpts print them "
    "and say their unit (tables are usually in millions), and cite the excerpts you used as [1], "
    "[2]. If the excerpts do not answer the question, say that the filings available on {now} do "
    "not say."
)
_CITATION = re.compile(r"\[(\d{1,2})\]")


def micros(at: datetime) -> int:
    if at.tzinfo is None:
        raise TypeError(f"an instant needs a timezone: {at!r}")
    return (at - EPOCH) // timedelta(microseconds=1)


def instant(us: int) -> datetime:
    return EPOCH + timedelta(microseconds=int(us))


def document_chunks(
    symbol: str,
    accession_number: str,
    form: str,
    report_date: date | None,
    filing_date: date,
    html: str,
    size: int = CHUNK_CHARS,
    overlap: int = OVERLAP_CHARS,
) -> list[tuple]:
    return [
        (
            chunk_id(accession_number, c.index, c.text),
            c.index,
            c.section,
            context_header(symbol, form, report_date, filing_date, c.section),
            c.text,
        )
        for c in chunk_document(html, size, overlap)
    ]


def filing_chunks(documents, count: int):
    from pyspark.sql import functions as F

    @F.udf(CHUNK_UDF_SCHEMA)
    def split(symbol, accession_number, form, report_date, filing_date, html):
        return document_chunks(symbol, accession_number, form, report_date, filing_date, html)

    columns = ("symbol", "accession_number", "form", "filing_date", "report_date")
    return (
        documents.repartition(max(1, count // DOCUMENTS_PER_PARTITION), "accession_number")
        .select(
            *columns,
            "knowable_at",
            F.explode(
                split("symbol", "accession_number", "form", "report_date", "filing_date", "html")
            ).alias("c"),
        )
        .select(*columns, *(F.col(f"c.{n}").alias(n) for n, _ in CHUNK_FIELDS), "knowable_at")
    )


def embedding_text(context: str, text: str) -> str:
    return f"{context}\n{text}"


class Embedder:
    def __init__(self, client, endpoint: str, batch: int = EMBED_BATCH):
        self.client = client
        self.endpoint = endpoint
        self.batch = batch
        self.dimension: int | None = None

    def __call__(self, texts: Sequence[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for start in range(0, len(texts), self.batch):
            batch = list(texts[start : start + self.batch])
            response = self.client.serving_endpoints.query(name=self.endpoint, input=batch)
            data = sorted(response.data or [], key=lambda e: e.index)
            if len(data) != len(batch):
                raise RuntimeError(
                    f"{self.endpoint} returned {len(data)} embeddings for {len(batch)} texts"
                )
            for element in data:
                vector = [float(x) for x in element.embedding]
                if self.dimension is None:
                    self.dimension = len(vector)
                if len(vector) != self.dimension:
                    raise RuntimeError(f"{self.endpoint} changed dimension mid-run")
                vectors.append(vector)
        return vectors


def chat(client, endpoint: str, messages: Sequence[dict], max_tokens: int = 400) -> str:
    from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

    response = client.serving_endpoints.query(
        name=endpoint,
        messages=[
            ChatMessage(role=ChatMessageRole(m["role"]), content=m["content"]) for m in messages
        ],
        max_tokens=max_tokens,
        temperature=0.0,
    )
    if not response.choices or response.choices[0].message is None:
        raise RuntimeError(f"{endpoint} returned no answer")
    return response.choices[0].message.content or ""


@dataclass(frozen=True)
class Excerpt:
    chunk_id: str
    context: str
    text: str


def moment(at: datetime) -> str:
    return f"{long_date(at.astimezone(UTC).date())} {at.astimezone(UTC):%H:%M} UTC"


def prompt_messages(question: str, at: datetime, excerpts: Sequence[Excerpt]) -> list[dict]:
    now = moment(at)
    numbered = "\n\n".join(f"[{n}] {e.context}\n{e.text}" for n, e in enumerate(excerpts, start=1))
    return [
        {"role": "system", "content": SYSTEM_PROMPT.format(now=now)},
        {"role": "user", "content": f"Excerpts:\n\n{numbered or '(none)'}\n\nQuestion: {question}"},
    ]


def citations(answer: str, excerpts: int) -> list[int]:
    found = {int(n) for n in _CITATION.findall(answer)}
    return sorted(n for n in found if 1 <= n <= excerpts)


def answered(answer: str, answers: Sequence[str]) -> bool:
    return bool(answers) and bool(answer_pattern(answers).search(answer))


def index_filters(as_of_us: int, symbol: str | None) -> dict:
    filters: dict = {"knowable_at_us <=": int(as_of_us)}
    if symbol:
        filters["symbol"] = symbol
    return filters


def parse_query_response(response) -> list[dict]:
    columns = [c.name for c in (response.manifest.columns or [])]
    rows = (response.result.data_array if response.result else None) or []
    return [dict(zip(columns, row, strict=True)) for row in rows]


def query_index(client, index: str, vector, as_of_us: int, symbol: str | None, k: int):
    import json

    response = client.vector_search_indexes.query_index(
        index_name=index,
        columns=list(INDEX_COLUMNS),
        query_vector=[float(x) for x in vector],
        filters_json=json.dumps(index_filters(as_of_us, symbol)),
        num_results=k,
    )
    rows = parse_query_response(response)
    for row in rows:
        row["knowable_at_us"] = int(row["knowable_at_us"])
        row[SCORE] = float(row[SCORE])
        if row["knowable_at_us"] > as_of_us:
            raise RuntimeError(
                f"{index} returned {row['chunk_id']}, knowable at {row['knowable_at_us']}, for a "
                f"query as of {as_of_us}: the point-in-time filter did not hold"
            )
    return rows


def ensure_endpoint(client, name: str, timeout: timedelta = timedelta(minutes=30)) -> None:
    from databricks.sdk.errors import NotFound
    from databricks.sdk.service.vectorsearch import EndpointType

    try:
        client.vector_search_endpoints.get_endpoint(name)
    except NotFound:
        print(f"creating vector search endpoint {name}")
        client.vector_search_endpoints.create_endpoint(name, EndpointType.STANDARD).result(
            timeout=timeout
        )
        return
    deadline = time.monotonic() + timeout.total_seconds()
    while True:
        status = client.vector_search_endpoints.get_endpoint(name).endpoint_status
        state = status.state.value if status and status.state else "UNKNOWN"
        if state == "ONLINE":
            return
        if state == "OFFLINE" or time.monotonic() > deadline:
            raise RuntimeError(f"vector search endpoint {name} is {state}: {status}")
        time.sleep(30)


def ensure_index(client, endpoint: str, index: str, source_table: str, dimension: int) -> bool:
    from databricks.sdk.errors import NotFound
    from databricks.sdk.service.vectorsearch import (
        DeltaSyncVectorIndexSpecRequest,
        EmbeddingVectorColumn,
        PipelineType,
        VectorIndexType,
    )

    try:
        client.vector_search_indexes.get_index(index)
        return False
    except NotFound:
        pass
    print(f"creating vector index {index} on {source_table} ({dimension} dimensions)")
    client.vector_search_indexes.create_index(
        name=index,
        endpoint_name=endpoint,
        primary_key="chunk_id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=source_table,
            pipeline_type=PipelineType.TRIGGERED,
            embedding_vector_columns=[
                EmbeddingVectorColumn(name="embedding", embedding_dimension=dimension)
            ],
            columns_to_sync=[*INDEX_COLUMNS, "embedding"],
        ),
    )
    return True


def wait_for_index(
    client, index: str, expected_rows: int, timeout: timedelta = timedelta(minutes=45)
) -> int:
    deadline = time.monotonic() + timeout.total_seconds()
    while True:
        status = client.vector_search_indexes.get_index(index).status
        rows = (status.indexed_row_count if status else None) or 0
        if status and status.ready and rows == expected_rows:
            return rows
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"{index}: {rows} of {expected_rows} rows indexed, "
                f"ready={status and status.ready}: "
                f"{status and status.message}"
            )
        time.sleep(20)


def vector_index(catalog: str) -> str:
    return f"{catalog}.gold.chunk_embeddings_index"


def index_source(catalog: str) -> str:
    return f"{catalog}.gold.chunk_embeddings"


def latest_version(spark, table_fqn: str) -> int:
    return int(spark.sql(f"DESCRIBE HISTORY {table_fqn} LIMIT 1").collect()[0]["version"])


def build_row(run_id, at, step, table, version, total, changed, inputs, detail="") -> tuple:
    return (run_id, at, step, table, version, total, changed, dict(inputs), detail)
