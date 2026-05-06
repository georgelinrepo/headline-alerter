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

## WhatsApp Sandbox setup (one-time, required for Phase 1b alerter)

The alerter uses Twilio's WhatsApp Sandbox. Free number, ~$0.005/message, no
need to exit Twilio trial mode.

1. Twilio Console → Develop → Messaging → Try it out → **Send a WhatsApp
   message**. Note the sandbox number (`+1 415 523 8886`) and the join code
   (a phrase like `join sky-glow`).
2. From your phone's WhatsApp, send `join <your-code>` to `+1 415 523 8886`.
   You should receive `Joined <your-code>. Reply ...`.
3. Copy your Account SID and Auth Token from the Twilio Console
   (Account → API keys & tokens) into `.env`:
   ```
   TWILIO_ACCOUNT_SID=AC...
   TWILIO_AUTH_TOKEN=...
   TWILIO_FROM=whatsapp:+14155238886
   ALERT_RECIPIENT=whatsapp:+44...    # your phone, with country code
   ```
4. `docker compose up -d alerter` to start the alerter.
5. `python tools/alerter_smoke.py` to verify end-to-end (sends one
   hardcoded test message; you should receive it within 5s).

### The 24-hour window caveat

WhatsApp Sandbox sessions expire 24h after your last inbound message. If
the alerter goes silent for >24h (no events meeting threshold over a quiet
weekend), the next outbound message will fail with Twilio code 63016. The
alerter routes that to `events.dlq` with `stage='alerter_whatsapp_template'`
and keeps consuming. Reply anything to the sandbox to reopen the window.
