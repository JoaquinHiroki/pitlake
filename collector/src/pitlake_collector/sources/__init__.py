from pitlake_collector.sources.alpaca import AlpacaDailyBars, AlpacaTrades
from pitlake_collector.sources.base import Http, Secrets, Source
from pitlake_collector.sources.binance import BinanceSpotTrades
from pitlake_collector.sources.coinbase import CoinbaseCandles
from pitlake_collector.sources.edgar import EdgarCompanyFacts, EdgarFilings
from pitlake_collector.sources.fred import FredVintages

_SOURCES: dict[tuple[str, str], type] = {
    (cls.name, cls.dataset): cls
    for cls in (
        BinanceSpotTrades,
        CoinbaseCandles,
        FredVintages,
        AlpacaTrades,
        AlpacaDailyBars,
        EdgarCompanyFacts,
        EdgarFilings,
    )
}


def _no_secrets(name: str) -> str:
    raise RuntimeError(f"secret {name!r} requested, but no secret reader was configured")


def build_source(source: str, dataset: str, http: Http, secrets: Secrets | None = None) -> Source:
    try:
        cls = _SOURCES[(source, dataset)]
    except KeyError:
        known = sorted(f"{s}.{d}" for s, d in _SOURCES)
        raise ValueError(f"unknown feed {source}.{dataset}; known: {known}") from None
    return cls.create(http, secrets or _no_secrets)
