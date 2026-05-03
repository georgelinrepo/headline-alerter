# Headline Alerter

Streaming pipeline that ingests news and social-media events, scores rates-market
impact via Claude Haiku 4.5, and alerts via Twilio SMS/WhatsApp.

See [`docs/superpowers/specs/2026-05-03-headline-alerter-design.md`](docs/superpowers/specs/2026-05-03-headline-alerter-design.md) for design.

## Setup

1. Install Docker + Docker Compose v2.
2. Copy `.env.example` to `.env` and fill in API keys.
3. `docker compose up -d`.
4. `python tools/smoke_test.py` to verify Phase 0 plumbing.

## Development

```bash
pip install -e ".[dev]"
pytest                              # unit + integration
pytest tests/unit                   # unit only (no Docker required)
```
