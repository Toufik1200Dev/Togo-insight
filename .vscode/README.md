# Workspace VS Code settings

- **`settings.json`** is listed in the repo root **`.gitignore`** so it is not committed. It may contain Azure subscription IDs, resource groups, and App Service resource paths — **do not** paste real values into any tracked file.
- Copy **`settings.json.example`** to **`settings.json`** and replace the placeholders with your deployment targets.

```bash
# PowerShell (repo root)
Copy-Item .vscode/settings.json.example .vscode/settings.json
```

Then edit `.vscode/settings.json` with your Azure App Service details.
