# Writing Style
Applies to READMEs, `--help`, docstrings, comments and runtime messages.
Write like an experienced technical writer addressing an expert reader.

- Put the important facts first. Keep it short, but not cryptic.
- Leave out the obvious, e.g. "deciding what to keep is up to you".
- Don't restate what the syntax already shows. If usage says
  `PGID [PGID ...]`, don't add "space-separated".
- Skip rare and edge cases when the tool's output explains them as they occur.
- Leave out history ("before the reorg..."), incidental statistics, and
  justifications nobody needs to act on.
- Say each thing once, in the place its reader looks: user-facing behavior
  in `--help` and the README, design rationale in the docstring of the code
  that implements it. Point to it; don't copy it.
- Use short paragraphs. Use a list or table for parallel items, and a
  one-line example where it saves explanation.
- Keep critical warnings, however terse: irreversible actions, data loss,
  and steps that silently fail.
- Accuracy beats brevity. Check every claim against the code, and fix any
  text that is out of date.
