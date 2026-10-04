from pitlake_collector.sources.base import Http, Source
from pitlake_collector.sources.binance import BinanceSpotTrades
from pitlake_collector.sources.coinbase import CoinbaseCandles

_SOURCES: dict[tuple[str, str], type] = {
    (cls.name, cls.dataset): cls for cls in (BinanceSpotTrades, CoinbaseCandles)
}


def build_source(source: str, dataset: str, http: Http) -> Source:
    try:
        cls = _SOURCES[(source, dataset)]
    except KeyError:
        known = sorted(f"{s}.{d}" for s, d in _SOURCES)
        raise ValueError(f"unknown feed {source}.{dataset}; known: {known}") from None
    return cls(http)
