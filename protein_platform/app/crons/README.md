Protein Platform — cron jobs and helper scripts

Purpose
 - Hold small, idempotent scripts and Render cron definitions that perform
   scheduled maintenance or data-seeding tasks for the Protein Platform.

Recommended scripts & examples
 - seed_daily.sh — run periodic seeding/upserts. Two safe approaches:
   - Call the running service admin endpoint (recommended in Render):
     curl -s -X POST "https://$HOST/admin/seed" -H "X-Admin-Token: $ADMIN_TOKEN"
   - Run the module directly (for environments where the code and DB are available):
     export DATABASE_URL="postgres://..." && python -m app.seed

 - cleanup_old_records.sh — archive or purge test/dev records; ensure it
   uses deterministic criteria so it can be re-run safely.

Idempotency guarantees
 - The project exposes `run_seed(engine)` which the CLI entrypoint (`python -m app.seed`)
   calls. `run_seed` is implemented to be idempotent: it upserts plants and products
   by keys, upserts mappings by (source_system, source_item_id, plant_code), and
   inserts production/pricing rows only when unique constraints are not violated.
 - Because of these guarantees, scheduled runs can safely execute repeatedly.

Environment variables
 - `DATABASE_URL`: connection string used when running `python -m app.seed`.
 - `ADMIN_TOKEN`: required when calling the `/admin/seed` endpoint.
 - `HOST` / service URL: used by cron scripts that call the HTTP endpoint.

Render cron example
 - Render expects a schedule that runs a command. Example cron entry (conceptual):
   "0 3 * * *" -> run a script that executes the `curl` POST to `/admin/seed` with `X-Admin-Token`.

Guidelines
 - Make scripts executable (`chmod +x`) and include `#!/usr/bin/env bash`.
 - Keep scripts short and rely on the service's HTTP admin endpoint where
   possible so the environment (DB, credentials) is consistent in Render.
 - Log progress to stdout/stderr for observability in Render job logs.

Adding files
 - Add files here using the repository structure `protein_platform/app/crons/`.
 - If you'd like, add a sample `seed_daily.sh` and I can commit it for you.
