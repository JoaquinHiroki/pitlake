# PITLake

Full specification: [docs/PITLake-project-specification.pdf](docs/PITLake-project-specification.pdf)

Changes since specification revision 3 are recorded as architecture decisions in [docs/adr/](docs/adr/),
notably [ADR 0003](docs/adr/0003-collector-on-databricks.md): the collector runs inside Databricks, and
[ADR 0004](docs/adr/0004-api-sources.md): how sources served by an API are landed (Coinbase lands
one-minute candles rather than trades).
Setup and operation: [docs/runbooks/stage1-setup.md](docs/runbooks/stage1-setup.md).
