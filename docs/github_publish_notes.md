# GitHub Publish Notes

## What belongs in GitHub

The clean GitHub version of this project should include:

- source code
- configs
- prompts
- SQL schema and helper queries
- architecture and results documentation
- run scripts

## What does not need to be committed

These are generated locally and are intentionally gitignored:

- `workspace_*`
- `published_runs/`
- SQLite database files
- logs
- other machine-local runtime artifacts
- private handoff notes

## Current publish posture

This folder is ready for GitHub publishing from a content perspective. The main code, docs, configs, prompts, SQL, ERD, and public case-study deck are in place. Generated run artifacts remain local so the public repo stays smaller and avoids publishing company-like synthetic supplier records.

## Clean snapshot option

Run:

```powershell
.\scripts\export_github_snapshot.ps1
```

This creates a clean sibling folder that contains only publishable content.
By default, the snapshot folder name is derived from the current repo folder name and ends with `_github_ready`. You can also pass `-TargetFolderName` if you want a specific export folder name.

## Important note

Git is not currently available on the shell `PATH` in this environment, so repository initialization, staging, and commits were not performed here. If you use GitHub Desktop, VS Code source control, or a shell with Git installed, this folder is ready to use.
