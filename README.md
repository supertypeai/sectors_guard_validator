# Sectors Guard Validator (FastAPI)

Backend service for validating IDX financial datasets with anomaly detection and email notifications. Built with FastAPI, Gunicorn/Uvicorn, and Supabase.

## Features
- Validate multiple IDX tables (annual/quarterly financials, daily prices, dividends, filings, stock splits)
- Run single-table or all-table validations via API
- Email alerting on anomalies with HTML/text templates
- Dashboard endpoints for results and trends
- Health endpoint for platform checks

## Architecture
- Framework: FastAPI (`app.main:app`)
- ASGI Server: Gunicorn + Uvicorn worker
- Data: Supabase (Postgres + REST)
- Email: SMTP

## Environment variables
Set via `.env` locally or platform env in production.

Required for data access:
- SUPABASE_URL
- SUPABASE_KEY
- DB_PASSWORD (used to compose Postgres URL for SQLAlchemy)

Email (optional but recommended):
- SMTP_SERVER (default: smtp.gmail.com)
- SMTP_PORT (default: 587)
- SMTP_USERNAME
- SMTP_PASSWORD
- FROM_EMAIL (defaults to SMTP_USERNAME)
- DEFAULT_EMAIL_RECIPIENTS (comma-separated)
- DAILY_SUMMARY_RECIPIENTS (comma-separated)

General:
- PORT (default local 8000; on Fly set to 8080 via `fly.toml`)
- DEBUG (true/false)

## API endpoints
Base path: `/`

Health
- GET `/health` → { status: "healthy" }

Validation API (prefix `/api/validation`)
- GET `/tables` → list available tables and last validated time
- POST `/run/{table_name}` → run validation, optional query params `start_date`, `end_date`
- POST `/run-all` → run all validations, optional `start_date`, `end_date`

Dashboard API (prefix `/api/dashboard`)
- GET `/results` → recent validation results (DB, fallback to local)
- GET `/stats` → high-level stats
- GET `/charts/validation-trends` → series for charts

## Local development
1) Create and populate `.env` with the variables above.
2) Install deps:
	- Windows (cmd):
	  ```cmd
	  pip install -r requirements.txt
	  ```
3) Run dev server with reload:
	```cmd
	uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
	```
4) Open http://localhost:8000 and docs at http://localhost:8000/docs

## Production run (Gunicorn/Uvicorn)
Locally you can mimic production:
```cmd
gunicorn -k uvicorn.workers.UvicornWorker -w 2 app.main:app --bind 0.0.0.0:8080
```

## Docker
Already configured via `Dockerfile`.

Build and run:
```cmd
docker build -t sectors-guard-validator .
docker run -p 8080:8080 --env-file .env sectors-guard-validator
```

## Deploy on Fly.io
Configured via `fly.toml` and `Procfile`.

- Procfile: `web: gunicorn -k uvicorn.workers.UvicornWorker -w 2 app.main:app --bind 0.0.0.0:$PORT --timeout 60`
- Port: 8080 (Dockerfile, Procfile, fly.toml aligned)
- Health checks: TCP + HTTP `/health`

Deploy steps (once logged in and app created):
```cmd
fly deploy
```

`fly.toml` highlights:
- `env.PORT=8080`
- `services.internal_port=8080`
- `[[services.http_checks]] path="/health"`

## Troubleshooting
- 404 on `/health`: Ensure FastAPI app exposes `/health` (it does in `app.main`).
- 502 on Fly: Confirm ports align (8080 everywhere) and health check passes.
- Supabase errors: Verify `SUPABASE_URL`, `SUPABASE_KEY`, and `DB_PASSWORD`.
- Emails not sent: Check SMTP creds and `DEFAULT_EMAIL_RECIPIENTS`.

## License
MIT