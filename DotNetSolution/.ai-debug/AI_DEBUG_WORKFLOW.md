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
