# AI agent debugging workflow (DotNetSolution)

This **DotNetSolution** folder includes a **machine-run diagnostic harness** for the .NET application and related tooling. Future AI agents should use it instead of asking the human to copy/paste individual shell commands from chat.

## What to do (agents)

1. **Do not** ask the user to manually run one-off diagnostic commands from chat.
2. **Add** the diagnostic commands you need to:
   - `DotNetSolution/.ai-debug/debug_commands.sh` (Unix / macOS / Linux), and/or  
   - `DotNetSolution/.ai-debug/debug_commands.ps1` (Windows PowerShell)  
   Use **one command per line**. Blank lines and lines starting with `#` are ignored.
3. Use **only safe, non-destructive, read-only** diagnostic commands unless the user explicitly approves otherwise.
4. **Commit** and **push** the updated command file(s) on your branch.
5. Tell the user to **`git pull`** (or equivalent) so they have the latest command list.
6. Tell the user to run **from the `DotNetSolution` directory** (not necessarily the monorepo root):
   - Unix / macOS / Linux: `./debug.sh`
   - Windows: `.\debug.ps1`  
   **Requires Python 3** on Unix/macOS/Linux (`debug.sh` uses it to emit safe JSON). PowerShell uses built-in JSON serialization.
7. Tell the user to paste the contents of **`DotNetSolution/debug_results.json`** back into the chat (or attach the file).
8. **Never** intentionally capture secrets, tokens, credentials, private keys, or sensitive environment values in debug output. Do not add commands that print full `env`, read `.env`, `~/.ssh`, cloud credential files, etc.
9. Prefer **narrowly scoped** commands (e.g. `docker compose ps`, `dotnet --info`, `dotnet build -v:m` with no secrets).
10. **Remove** obsolete diagnostic commands when they are no longer needed so the files stay small and clear.
11. Keep commands **reproducible** and easy to understand; add `#` comments where helpful. Prefer paths **relative to `DotNetSolution`** when you list commands (the scripts set the working directory to that folder).
12. For cross-platform debugging, add **equivalent** commands to **both** `debug_commands.sh` and `debug_commands.ps1` when practical.

## What the user does (humans)

1. Pull the branch the agent updated.
2. From **`DotNetSolution`**, run `./debug.sh` or `.\debug.ps1`.
3. Open `debug_results.json` in that same folder and paste its contents (or attach it) into the chat.

`debug_results.json` is **gitignored**; it is generated locally and may contain machine-specific paths.

## Docker / Compose (agents)

- **Bake build failures** (`failed to execute bake: exit status 1`): `deploy/lib-nightmare-compose.sh` and `deploy/run-local.ps1` default **`COMPOSE_BAKE=false`**. For manual compose: `COMPOSE_BAKE=false docker compose -f deploy/docker-compose.yml build`.
- **`docker run … dotnet --info` starts the web app**: the image **ENTRYPOINT** is `dotnet NightmareV2.CommandCenter.dll`; extra words become **app arguments**, not a new process. Use: `docker run --rm --entrypoint dotnet <image> --info`.
- **Postgres `127.0.0.1` inside a standalone container**: published `appsettings.json` uses `localhost` for Postgres. Compose sets `ConnectionStrings__Postgres` to `Host=postgres`. Run through compose, or pass `-e ConnectionStrings__Postgres=…` pointing at a reachable host.
- **Smoke test without DB** (not for production): `NIGHTMARE_SKIP_STARTUP_DATABASE=1` or config `Nightmare:SkipStartupDatabase` skips startup `EnsureCreated`; APIs that touch the DB will still fail until Postgres is configured.

## Reading `debug_results.json`

- **`dotnet_skip: not on PATH`**: many Linux deploy hosts only have Docker; the .NET SDK runs inside `Dockerfile.web` / `Dockerfile.worker` builds, not on the host. That line is informational, not a stack failure.
- **Non-zero `exit_code` on a Docker line**: usually Docker not running, missing `docker compose` plugin, or permission denied on the socket (try the same command with `sudo` only if your ops policy allows it).

## Safety rules (agents)

- No destructive commands (no `rm -rf`, `docker system prune`, database drops, etc.).
- No git history rewrite (`git reset --hard`, force pushes from scripts, etc.).
- No global installs or system configuration changes from these command lists.
- If a command might expose sensitive data, ask the user first or design the command to redact output.
- Do not instruct the user to paste secrets into chat.

## Files in this folder

| File | Purpose |
|------|---------|
| `AI_DEBUG_WORKFLOW.md` | This document |
| `debug_commands.sh` | Command list for `debug.sh` |
| `debug_commands.ps1` | Command list for `debug.ps1` |

The **`DotNetSolution`** directory also contains **`debug.sh`** and **`debug.ps1`** runners.
