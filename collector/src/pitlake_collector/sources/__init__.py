from pitlake_collector.sources.base import Source
from pitlake_collector.sources.binance import BinanceSpotTrades, Http

_SOURCES: dict[tuple[str, str], type] = {
    (BinanceSpotTrades.name, BinanceSpotTrades.dataset): BinanceSpotTrades,
}


def build_source(source: str, dataset: str, http: Http) -> Source:
    try:
        cls = _SOURCES[(source, dataset)]
    except KeyError:
        known = sorted(f"{s}.{d}" for s, d in _SOURCES)
        raise ValueError(f"unknown feed {source}.{dataset}; known: {known}") from None
    return cls(http)
