# CLI Tool User Experience
- `--help` answers what the tool does, when to use it, how to act on its
  output, and what it assumes. Keep option help to one line where possible.
- Default to read-only. A tool that changes state says so up front; prefer
  printing proposals or commands over applying them.
- stdout is for results and must stay parseable (e.g. offer a JSON mode).
  Notes, warnings and summaries go to stderr.
- Each message is one wrapped paragraph that says what happened and what to
  do next. Say a thing once per run.
- Fail early with a clear `ERROR:` and a non-zero exit. Never guess
  silently when the result would look plausible but be wrong.
- Name bad input so typos surface.
- Mark estimates or fallback values in the output (e.g. `~`) and explain the
  mark once, in a footnote.
- Output is deterministic: stable sort order and tie-breaks.
- Offer an offline mode (capture state, replay it) so users can try the tool
  and report bugs without touching production.

# CLI Tool Code
- Separate deciding from printing: e.g. `plan()` returns a typed result and
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
