# Pandora Geekolytics

Pandora analytics dashboard powered by the Pandora indexer.

## What this repo does

- Serves the full analytics UI from `analytics/dune/mega`.
- Pulls fresh data from `https://pandoraindexer.up.railway.app/`.
- Generates a static snapshot (`data/analytics.json`) for GitHub Pages.
- Auto-refreshes and deploys via GitHub Actions every 6 hours.

## Local run

```bash
npm run analytics:mega
```

Open `http://localhost:8787`.

## Local static export

```bash
npm run analytics:mega:export-static
```

Output is written to `analytics/dune/mega-ipfs`.

## Hosting

Workflow: `.github/workflows/pandora-analytics-pages.yml`

- Trigger: on push to `main/master`, manual dispatch, and scheduled every 6 hours.
- Publish target: GitHub Pages.
