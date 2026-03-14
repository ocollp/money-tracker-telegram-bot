---
name: commit
description: Helps create git commits by analyzing staged or unstaged changes and suggesting a clear commit message. Use when the user asks to commit, wants a commit message, or invokes /commit.
---

# Commit

## When to use

Apply this skill when the user:
- Asks to "commit", "make a commit", or "do a commit"
- Invokes `/commit` or similar
- Asks for a commit message or help writing one

## Workflow

1. **Inspect changes**
   - Run `git status` to see modified/untracked files.
   - Run `git diff` (and if needed `git diff --staged`) to see what actually changed.

2. **Propose a message**
   - Write a short subject line (about 50 chars): start with a verb in imperative (Add, Fix, Update, Refactor, …).
   - Optionally add a body line or two explaining why, not what.
   - Prefer the project’s language (e.g. Catalan/Spanish for this repo) if the user has used it; otherwise English is fine.

3. **Commit**
   - If the user asked to perform the commit: suggest or run `git add` for the intended files, then `git commit -m "..."` with the proposed message.
   - If the user only asked for a message: output the message so they can copy it.

## Format

- **Subject**: one line, imperative, no period at the end.
- **Body** (optional): blank line, then 1–2 lines of context.

Examples:
- `Add Telegram bot to append rows to Google Sheets`
- `Fix date parsing for DD/MM when month > 12`
- `Traduir missatges del bot al català`

## Notes

- Do not commit secrets, `.env`, or `node_modules`. If such files appear in `git status`, warn and exclude them.
- If nothing is staged and the user said "commit", ask which files to include or suggest `git add` for the relevant paths before committing.
