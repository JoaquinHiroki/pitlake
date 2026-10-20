import hashlib
import re
from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser

COMPANIES = {"AAPL": "Apple Inc.", "MSFT": "Microsoft Corporation", "NVDA": "NVIDIA Corporation"}
CHUNK_CHARS = 2000
OVERLAP_CHARS = 300
CELL_SEPARATOR = " | "

_SKIPPED = frozenset({"script", "style", "head", "ix:header"})
_BLOCKS = frozenset(
    {"p", "div", "br", "li", "ul", "ol", "table", "section", "center", "hr", "tr", "body"}
    | {f"h{i}" for i in range(1, 7)}
)
_CELLS = frozenset({"td", "th"})
_SPACE = re.compile(r"\s+")
_ATTACHED = frozenset({")", "%", ")%", "%)"})
_DROPPED = frozenset({"$", "€"})
_HEADING = re.compile(r"^(PART\s+[IV]+\b|Item\s+\d{1,2}[A-C]?\s*[.:])", re.IGNORECASE)
_HEADING_CHARS = 150
_SENTENCE_END = re.compile(r"(?<=[.;:])\s")


def normalize(text: str) -> str:
    return _SPACE.sub(" ", text.replace("​", "")).strip()


def table_row(cells: list[str]) -> str:
    kept: list[str] = []
    for cell in (normalize(c) for c in cells):
        if not cell or cell in _DROPPED:
            continue
        if cell in _ATTACHED and kept:
            kept[-1] += cell
            continue
        kept.append(cell)
    return CELL_SEPARATOR.join(kept)


class _TextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.lines: list[str] = []
        self.pieces: list[str] = []
        self.skipping = 0
        self.row: list[str] | None = None
        self.cell: list[str] | None = None

    def flush(self) -> None:
        line = normalize("".join(self.pieces))
        if line:
            self.lines.append(line)
        self.pieces = []

    def handle_starttag(self, tag, attrs) -> None:
        if tag in _SKIPPED:
            self.skipping += 1
        elif self.skipping:
            return
        elif tag in _CELLS and self.row is not None:
            self.cell = []
        elif tag == "tr":
            self.flush()
            self.row = []
        elif tag in _BLOCKS:
            if self.cell is not None:
                self.cell.append(" ")
            elif self.row is None:
                self.flush()

    def handle_startendtag(self, tag, attrs) -> None:
        if tag in _BLOCKS:
            self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag) -> None:
        if tag in _SKIPPED:
            self.skipping = max(0, self.skipping - 1)
        elif self.skipping:
            return
        elif tag in _CELLS and self.cell is not None:
            self.row.append("".join(self.cell))
            self.cell = None
        elif tag == "tr" and self.row is not None:
            if self.cell is not None:
                self.row.append("".join(self.cell))
                self.cell = None
            line = table_row(self.row)
            if line:
                self.lines.append(line)
            self.row = None
        elif tag in _BLOCKS and self.cell is None and self.row is None:
            self.flush()

    def handle_data(self, data: str) -> None:
        if self.skipping:
            return
        if self.cell is not None:
            self.cell.append(data)
        elif self.row is None:
            self.pieces.append(data)


def html_to_lines(html: str) -> list[str]:
    parser = _TextParser()
    parser.feed(html)
    parser.close()
    parser.flush()
    return parser.lines


def is_heading(line: str) -> bool:
    return len(line) <= _HEADING_CHARS and CELL_SEPARATOR not in line and bool(_HEADING.match(line))


def split_long(line: str, size: int) -> list[str]:
    if len(line) <= size:
        return [line]
    parts, rest = [], line
    while len(rest) > size:
        window = rest[:size]
        cuts = [m.end() for m in _SENTENCE_END.finditer(window)]
        cut = cuts[-1] if cuts and cuts[-1] > size // 2 else window.rfind(" ") + 1
        if cut <= 0:
            cut = size
        parts.append(rest[:cut].strip())
        rest = rest[cut:].strip()
    if rest:
        parts.append(rest)
    return parts


@dataclass(frozen=True)
class Chunk:
    index: int
    section: str
    text: str


def chunk_lines(
    lines: list[str], size: int = CHUNK_CHARS, overlap: int = OVERLAP_CHARS
) -> list[Chunk]:
    if not 0 <= overlap < size:
        raise ValueError("overlap must be at least 0 and smaller than the chunk size")
    pieces: list[tuple[str, str]] = []
    section = ""
    for line in lines:
        if is_heading(line):
            section = line
        pieces.extend((part, section) for part in split_long(line, size))

    chunks: list[Chunk] = []
    start = 0
    while start < len(pieces):
        end, used = start, 0
        while end < len(pieces) and (end == start or used + 1 + len(pieces[end][0]) <= size):
            used += len(pieces[end][0]) + (1 if end > start else 0)
            end += 1
        chunks.append(
            Chunk(len(chunks), pieces[start][1], "\n".join(p for p, _ in pieces[start:end]))
        )
        if end == len(pieces):
            break
        back, kept = end, 0
        while back - 1 > start and kept + len(pieces[back - 1][0]) <= overlap:
            back -= 1
            kept += len(pieces[back][0])
        start = back
    return chunks


def chunk_document(html: str, size: int = CHUNK_CHARS, overlap: int = OVERLAP_CHARS):
    return chunk_lines(html_to_lines(html), size, overlap)


def chunk_id(accession_number: str, index: int, text: str) -> str:
    return hashlib.sha256(f"{accession_number}\x1f{index}\x1f{text}".encode()).hexdigest()[:32]


def long_date(day: date) -> str:
    return f"{day:%B} {day.day}, {day.year}"


def company(symbol: str) -> str:
    return COMPANIES.get(symbol, symbol)


def context_header(
    symbol: str, form: str, report_date: date | None, filing_date: date, section: str
) -> str:
    period = f" for the period ended {long_date(report_date)}" if report_date else ""
    where = f" {section}" if section else ""
    return (
        f"{company(symbol)} ({symbol}) Form {form}{period}, filed {long_date(filing_date)}.{where}"
    )
