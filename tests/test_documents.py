from datetime import date

import pytest

from pitlake.documents import (
    Chunk,
    chunk_document,
    chunk_id,
    chunk_lines,
    context_header,
    html_to_lines,
    is_heading,
    long_date,
    split_long,
    table_row,
)

FILING = """<?xml version='1.0' encoding='ASCII'?>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:ix="http://www.xbrl.org/2013/inlineXBRL">
<head><title>aapl-20240629</title><style>p {margin:0}</style></head>
<body>
<div style="display:none"><ix:header><ix:hidden>
<ix:nonNumeric name="dei:AmendmentFlag">false</ix:nonNumeric>
</ix:hidden></ix:header></div>
<div><span>PART I &#8212; FINANCIAL INFORMATION</span></div>
<div><span>Item 1.&#160;&#160;&#160;&#160;Financial Statements</span></div>
<table>
<tr><td colspan="3"><span>Three Months Ended</span></td></tr>
<tr><td><span>&#160;</span></td><td><span>June 29,<br/>2024</span></td>
<td>June 24,<br/>2023</td></tr>
<tr><td><span>Total net sales</span></td><td>$</td><td>85,777</td><td>$</td><td>81,797</td></tr>
<tr><td>Other income/(expense), net</td><td>(</td><td>(142</td><td>)</td>
<td>(265</td><td>)</td></tr>
<tr><td>Gross margin percentage</td><td>46.3</td><td>%</td></tr>
</table>
<p>Net sales rose&#160;5% because of <b>Services</b>
growth.</p>
<script>var tracking = "never in the text";</script>
</body></html>"""


def test_html_to_lines_keeps_text_and_flattens_tables():
    lines = html_to_lines(FILING)
    assert lines == [
        "PART I — FINANCIAL INFORMATION",
        "Item 1. Financial Statements",
        "Three Months Ended",
        "June 29, 2024 | June 24, 2023",
        "Total net sales | 85,777 | 81,797",
        "Other income/(expense), net | ( | (142) | (265)",
        "Gross margin percentage | 46.3%",
        "Net sales rose 5% because of Services growth.",
    ]


def test_hidden_inline_xbrl_scripts_and_styles_are_not_text():
    text = "\n".join(html_to_lines(FILING))
    assert "AmendmentFlag" not in text and "false" not in text
    assert "tracking" not in text and "margin:0" not in text and "aapl-20240629" not in text


def test_table_row_drops_currency_cells_and_attaches_closing_cells():
    assert table_row(["Net income", " $ ", "(1,234", ")", "", "12", "%"]) == (
        "Net income | (1,234) | 12%"
    )
    assert table_row([" ", "$"]) == ""


@pytest.mark.parametrize(
    "line, expected",
    [
        ("Item 1A. Risk Factors", True),
        ("ITEM 7: Management's Discussion", True),
        ("PART II — OTHER INFORMATION", True),
        ("Item 1A. | Risk Factors | 5", False),
        ("Items sold rose 5%", False),
        ("Item 7. " + "x" * 200, False),
    ],
)
def test_headings(line, expected):
    assert is_heading(line) is expected


def test_split_long_prefers_sentence_ends():
    line = "First sentence here. " * 10 + "Tail"
    parts = split_long(line.strip(), 60)
    assert all(len(p) <= 60 for p in parts)
    assert all(p.endswith(".") for p in parts[:-1])
    assert " ".join(parts) == line.strip()
    assert split_long("x" * 130, 60) == ["x" * 60, "x" * 60, "x" * 10]


def test_chunks_respect_size_overlap_and_sections():
    lines = ["Item 1. Business"] + [f"line {i:02d} " + "y" * 30 for i in range(12)]
    lines += ["Item 1A. Risk Factors"] + [f"risk {i:02d} " + "z" * 30 for i in range(12)]
    chunks = chunk_lines(lines, size=200, overlap=80)
    assert all(len(c.text) <= 200 for c in chunks)
    assert [c.index for c in chunks] == list(range(len(chunks)))
    assert chunks[0].section == "Item 1. Business"
    assert chunks[-1].section == "Item 1A. Risk Factors"
    for a, b in zip(chunks, chunks[1:], strict=False):
        assert a.text.split("\n")[-1] in b.text.split("\n")
    covered = {line for c in chunks for line in c.text.split("\n")}
    assert covered == set(lines)


def test_chunks_without_overlap_partition_the_lines():
    lines = [f"{i:03d}" for i in range(50)]
    chunks = chunk_lines(lines, size=40, overlap=0)
    assert [x for c in chunks for x in c.text.split("\n")] == lines


def test_a_line_longer_than_a_chunk_still_makes_progress():
    chunks = chunk_lines(["a" * 500, "b"], size=100, overlap=50)
    assert "".join(c.text for c in chunks[:5]) == "a" * 500
    assert chunks[-1].text.endswith("b")


def test_overlap_must_be_smaller_than_the_chunk():
    with pytest.raises(ValueError):
        chunk_lines(["x"], size=100, overlap=100)


def test_chunk_document_end_to_end():
    [chunk] = chunk_document(FILING)
    assert chunk == Chunk(0, "PART I — FINANCIAL INFORMATION", "\n".join(html_to_lines(FILING)))


def test_chunk_ids_are_stable_and_distinct():
    a = chunk_id("0000320193-24-000081", 3, "text")
    assert a == chunk_id("0000320193-24-000081", 3, "text") and len(a) == 32
    assert a != chunk_id("0000320193-24-000081", 4, "text")
    assert a != chunk_id("0000320193-24-000081", 3, "text!")


def test_context_header_names_company_period_and_section():
    header = context_header(
        "AAPL", "10-Q", date(2024, 6, 29), date(2024, 8, 2), "Item 1. Financial Statements"
    )
    assert header == (
        "Apple Inc. (AAPL) Form 10-Q for the period ended June 29, 2024, filed August 2, 2024. "
        "Item 1. Financial Statements"
    )
    assert context_header("XYZ", "10-K", None, date(2024, 1, 5), "") == (
        "XYZ (XYZ) Form 10-K, filed January 5, 2024."
    )
    assert long_date(date(2024, 6, 9)) == "June 9, 2024"
