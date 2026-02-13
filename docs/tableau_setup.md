# Tableau Refresh Setup (Self-hosted Airflow)

This document explains how to call Tableau refresh APIs from the Airflow task `tableau_refresh`.

## Required values
- `TABLEAU_SERVER_URL` example: `https://prod-apn-a.online.tableau.com`
- `TABLEAU_SITE_CONTENT_URL` example: `koin`
- `TABLEAU_PAT_NAME`
- `TABLEAU_PAT_SECRET`
- `TABLEAU_REFRESH_TARGET`: `workbook`, `datasource`, or `flow`
- `TABLEAU_WORKBOOK_ID` when target is `workbook`
- `TABLEAU_DATASOURCE_ID` when target is `datasource`
- `TABLEAU_FLOW_ID` when target is `flow`

## Optional values
- `TABLEAU_API_VERSION` default: `3.27`
- `TABLEAU_WAIT_FOR_JOB` default: `false`
- `TABLEAU_JOB_POLL_SECONDS` default: `20`
- `TABLEAU_JOB_TIMEOUT_SECONDS` default: `3600`
- `TABLEAU_DEBUG_AUTH_ONLY` default: `false`

## Run check
1. Open Airflow UI and run `dataform_tableau_pipeline` manually.
2. Check `tableau_refresh` logs.
3. Confirm `refresh job id` appears.
4. If `TABLEAU_WAIT_FOR_JOB=true`, confirm job reaches completion in logs.
