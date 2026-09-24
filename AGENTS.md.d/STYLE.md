# Writing
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

# CLI tools: for users
- `--help` answers what the tool does, when to use it, how to act on its
  output, and what it assumes. Keep option help to one line where possible.
- Make the usage line carry constraints (required, mutually exclusive,
  dependent options). Hand-write it when argparse can't express one.
- Default to read-only. A tool that changes state says so up front; prefer
  printing proposals or commands over applying them.
- stdout is for results and must stay parseable (e.g. offer a JSON mode).
  Notes, warnings and summaries go to stderr.
- Each message is one wrapped paragraph that says what happened and what to
  do next. Say a thing once per run.
- Fail early with a clear `ERROR:` and a non-zero exit. Never guess
  silently when the result would look plausible but be wrong.
- Name bad input (unmatched ids, unknown hosts) so typos surface.
- Mark estimates or fallback values in the output (e.g. `~`) and explain the
  mark once, in a footnote.
- Output is deterministic: stable sort order and tie-breaks.
- Defaults track the environment's own settings (e.g. the cluster's
  ratios), not hard-coded numbers.
- Offer an offline mode (capture state, replay it) so users can try the tool
  and report bugs without touching production.

# Code: for developers
- Separate deciding from printing: `plan()` returns a typed result and
  `render()` prints it. Logic tests assert on results, format tests on
  rendering.
- Share code rather than copy it, including option definitions and message
  text.
- Docstrings state the contract: what the function returns, its units, and
  when it returns None or exits. Add why only if it isn't obvious.
- Comments explain what the code can't say itself. Delete comments that
  restate the code.
- Name constants that encode a decision, and give their meaning in a line.
- Test corner cases, and check invariants against real captured data.
  Tests must not need a live system.
- Make tests robust to wording and line wrapping (e.g. collapse whitespace
  before substring checks) unless the exact format is what's under test.
