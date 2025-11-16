# Rehash Dialer V1

This is a minimal Twilio-powered dialer backend + frontend for the Rehash / RevCore stack.

## What's included

- Node.js + Express backend (`server.js`)
- Twilio Voice + Status webhooks
- Local presence caller ID logic
- In-memory lead queue and per-agent metrics
- Disposition endpoint (fires optional Zapier + Slack webhooks)
- Dubai-theme dialer UI (`public/dialer.html`)

## Quick start

1. Install dependencies:

   ```bash
   npm install
   ```

2. Copy `.env.example` to `.env` and fill in:

   - `TWILIO_ACCOUNT_SID`
   - `TWILIO_AUTH_TOKEN`
   - `BASE_URL` (ngrok or deployed URL)
   - optional `ZAPIER_HOOK_URL`
   - optional `SLACK_WEBHOOK_URL`

3. Run the server:

   ```bash
   npm start
   ```

4. Open the dialer UI:

   - Local: http://localhost:3000/dialer?agentId=1

5. Point a Twilio number's Voice webhooks at:

   - `POST {BASE_URL}/twilio/voice`
   - `POST {BASE_URL}/twilio/status`

Replace the placeholder phone numbers in `server.js`:

- `agents[agentId].dialTarget` with your agents' real phones
- `localPresenceMap` and `defaultCallerId` with your Twilio numbers
- `leadsQueue` with real test leads
# revcorehq-dialer
# revcorehq-dialer
