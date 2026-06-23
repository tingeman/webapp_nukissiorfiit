# Cross-Repo Development Guide

This file is the operational guide for cross-repo local development and the place to document ongoing intended usage and workflow changes.

## Scope

Repositories in shared workspace:
- Backend: `dtu_sus_monitoring_db`
- Frontend: `webapp_nukissiorfiit`

Goals:
- Keep existing production/remote-db settings intact.
- Use a local override path for development against local backend DB.

## Local Development Path (Current)

### Backend (local DB + Django develop stack)
From backend repo root:

```powershell
docker compose --project-name mdb --env-file .\secrets\mdb-postgres.dev.env -f .\docker-compose.develop.yml up -d --build
```

### Frontend (base compose + local override)
From frontend repo root:

```powershell
docker compose --project-name nuki -f .\compose.develop.yml -f .\compose.develop.local.yml up -d --build webapp_nuki
```

## Stop / Down Workflow

### Stop both stacks (keep containers)

```powershell
# backend
docker compose --project-name mdb --env-file .\secrets\mdb-postgres.dev.env -f .\docker-compose.develop.yml stop

# frontend
docker compose --project-name nuki -f .\compose.develop.yml -f .\compose.develop.local.yml stop
```

### Remove both stacks (keep named volumes)

```powershell
# backend
docker compose --project-name mdb --env-file .\secrets\mdb-postgres.dev.env -f .\docker-compose.develop.yml down

# frontend
docker compose --project-name nuki -f .\compose.develop.yml -f .\compose.develop.local.yml down
```

### Full reset (destructive: removes volumes)

```powershell
# backend only, includes DB volume removal
docker compose --project-name mdb --env-file .\secrets\mdb-postgres.dev.env -f .\docker-compose.develop.yml down -v
```

Use full reset only when you intentionally want to wipe local DB state.

## Why Override Instead of Editing Base

`compose.develop.yml` is kept as-is to preserve the existing working remote-db behavior.

`compose.develop.local.yml` only overrides the mounted env files for `webapp_nuki`:
- DB env: `secrets/webapp_nuki_mdb-postgres.dev.env`
- App env: `secrets/webapp_nuki.dev.env`

This avoids accidental changes to production-like defaults.

## Local-Only Env Files

Frontend local env files:
- `secrets/webapp_nuki_mdb-postgres.dev.env`
- `secrets/webapp_nuki.dev.env`

These are local development files and should stay local-only.

## Git Workflow in Shared Workspace

Treat each repository independently:

1. Create/switch branch in each repo separately.
2. Commit in each repo separately.
3. Keep commit messages aligned when implementing cross-repo changes.

Suggested branch pattern:
- `develop/<topic>`

## Branch / Commit Checklist

Use this sequence before and after each cross-repo change.

1. Confirm active branch in both repos is the same topic branch (for example `develop/cross-repo-dev-path`).
2. Run status in each repo and review changed files before staging.
3. Stage and commit backend and frontend separately.
4. Keep commit messages aligned across repos for the same feature.
5. Push both branches before opening PRs.

Example commit style:
- Backend: `dev-path: prepare backend local stack compatibility`
- Frontend: `dev-path: add local compose override for db env`

## Ongoing Change Log (append entries)

- 2026-06-23: Added local override path using `compose.develop.local.yml` to preserve base compose behavior.
- 2026-06-23: Added frontend local env files with `.dev` naming for local DB and app settings.
- 2026-06-23: Added backend Bash dump script at `scripts/db/dump_postgres.sh` for full DB backups from env settings.

## Production DB Dump Script (Bash)

Backend repo provides a Bash backup script that reads DB settings from env files:

```bash
./scripts/db/dump_postgres.sh
```

Useful options:

```bash
# include roles/tablespaces globals
./scripts/db/dump_postgres.sh --dump-globals

# use a specific env file
./scripts/db/dump_postgres.sh --env-file secrets/mdb-postgres.env

# force dockerized pg_dump (portable on hosts without local pg tools)
./scripts/db/dump_postgres.sh --docker
```

## Next Planned Work

- Introduce backend API layer incrementally.
- Migrate frontend data access from direct Django ORM integration to API calls.
- Remove tight coupling between frontend and backend Django model internals.
