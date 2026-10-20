import os
from datetime import UTC, date, datetime

import pytest

if os.environ.get("PITLAKE_REQUIRE_SPARK") != "1":
    pytest.importorskip("pyspark")

from test_cleaning import TS, bronze, judge, rule_counts  # noqa: E402

from pitlake.cleaning import FAILED_RULE  # noqa: E402
from pitlake.datasets import DATASETS, EDGAR_FILING_DOCUMENTS  # noqa: E402
from pitlake.rag import document_chunks, filing_chunks  # noqa: E402
from pitlake.readers import read_raw  # noqa: E402
from pitlake.silver import EDGAR_FILING_DOCUMENTS_SILVER  # noqa: E402
from pitlake.tables import CORRUPT_RECORD_COLUMN  # noqa: E402

Q3 = "0000320193-24-000081"
UNLISTED = "0000320193-24-000099"
SHORT = "0000320193-24-000100"
ACCEPTED = datetime(2024, 8, 2, 10, 1, 27, tzinfo=UTC)
PARAGRAPH = "<p>Net sales rose because of Services. " + "Detail. " * 40 + "</p>"
HTML = (
    "<?xml version='1.0'?><html><body><div style='display:none'><ix:header>hidden"
    "</ix:header></div><p>Item 2. Management's Discussion</p>" + PARAGRAPH * 40 + "</body></html>"
)


@pytest.fixture(scope="module")
def registry(spark):
    return spark.createDataFrame(
        [(d.source, d.name) for d in DATASETS.values()], "source STRING, dataset STRING"
    )


def test_a_document_is_read_whole_into_one_row(spark, tmp_path):
    (tmp_path / "doc.htm").write_text(HTML + "\n<!-- trailing -->\n")
    (tmp_path / "empty.htm").write_text("")
    [row] = read_raw(spark, EDGAR_FILING_DOCUMENTS, str(tmp_path / "doc.htm")).collect()
    assert row["document"].startswith("<?xml") and "trailing" in row["document"]
    assert row[CORRUPT_RECORD_COLUMN] is None
    empty = read_raw(spark, EDGAR_FILING_DOCUMENTS, str(tmp_path / "empty.htm")).collect()
    assert all(r["document"] is None and r[CORRUPT_RECORD_COLUMN] == "" for r in empty)


def document(spark, accession, html):
    return bronze(
        spark,
        EDGAR_FILING_DOCUMENTS,
        [{"document": html}],
        "AAPL",
        date(2024, 8, 2),
        f"edgar/filing_documents/AAPL/AAPL-document-{accession}.htm",
    )


def test_documents_are_knowable_at_acceptance_and_halt_without_it(spark, registry):
    rows = (
        document(spark, Q3, HTML)
        .unionByName(document(spark, UNLISTED, HTML))
        .unionByName(document(spark, SHORT, "<html>tiny</html>"))
    )
    filings = spark.createDataFrame(
        [
            ("AAPL", Q3, "10-Q", date(2024, 8, 2), date(2024, 6, 29), ACCEPTED),
            ("AAPL", SHORT, "10-Q", date(2024, 8, 2), date(2024, 6, 29), ACCEPTED),
        ],
        "symbol STRING, accession_number STRING, form STRING, filing_date DATE, "
        "report_date DATE, acceptance_time TIMESTAMP",
    )
    evaluated = judge(
        EDGAR_FILING_DOCUMENTS_SILVER, rows, registry, lookups={"edgar_filings": filings}
    )
    found = {
        r["accession_number"]: (r[FAILED_RULE], r["form"], r["k"])
        for r in evaluated.selectExpr(
            "accession_number", FAILED_RULE, "form", f"date_format(knowable_at, '{TS}') AS k"
        ).collect()
    }
    assert found == {
        Q3: (None, "10-Q", "2024-08-02 10:01:27"),
        UNLISTED: ("knowable_at_present", None, None),
        SHORT: ("document_has_text", "10-Q", "2024-08-02 10:01:27"),
    }
    assert rule_counts(evaluated) == {None: 1, "knowable_at_present": 1, "document_has_text": 1}


def test_the_chunking_udf_matches_the_python_chunker(spark):
    documents = spark.createDataFrame(
        [("AAPL", Q3, "10-Q", date(2024, 8, 2), date(2024, 6, 29), ACCEPTED, HTML)],
        "symbol STRING, accession_number STRING, form STRING, filing_date DATE, "
        "report_date DATE, knowable_at TIMESTAMP, html STRING",
    )
    rows = filing_chunks(documents, 1).orderBy("chunk_index").collect()
    expected = document_chunks("AAPL", Q3, "10-Q", date(2024, 6, 29), date(2024, 8, 2), HTML)
    assert len(rows) == len(expected) > 1
    assert [
        (r["chunk_id"], r["chunk_index"], r["section"], r["context"], r["text"]) for r in rows
    ] == expected
    assert {r["knowable_at"] for r in rows} == {ACCEPTED.replace(tzinfo=None)}
    assert all("hidden" not in r["text"] for r in rows)
