This directory contains Render cron job definitions and helper scripts for
scheduled tasks related to the Protein Platform ingestion system.

Place Render cron files (or scripts invoked by Render) here. Example jobs:
- `seed_daily.sh` — run a periodic seed/update of lookup data
- `cleanup_old_records.sh` — archive or purge old test data

Keep scripts small and idempotent. These are intended to be run in the same
environment as the web service so they can access the database via
`DATABASE_URL`.
