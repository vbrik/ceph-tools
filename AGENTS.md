# Agent
- Ask clarifying questions
- Flag design contradictions

# Project
- Maintain README.md file to stay in sync with changes. Keep it nice. Not too verbose, not too terse.
- Keep the project's github page's About and Topics in sync with the code
- Follow project-specific instructions in files in `AGENTS.md.d` directory, if it exists.

## Discoverability
- Keep project's README.md, github page's About and Topics search-engine friendly (make it easier for people to find the project), but make sure not to lie or mislead.

# Code
- Ensure comments and code don't get out-of-sync
- Structure code for human readability
- Good comments: concise
- Write doc strings except for trivial functions
- Use latest available python features and syntax when it makes code better
- Assume the reader is a python expert
- Run `ruff` on python files (mark both "format" and "check" subcommands happy)
- Use best practices of the programming language you are using

# Testing
- Add unit tests for new or changed functionality
- If makes sense, add unit tests when the way of how components interact changes
- Run unit tests to check your work
- Test corner conditions

# Git commit messages
- In *addition* to normal stuff that goes into commit message bodies
  - Include info about the reasoning that led to the changes, so that later we could flag contradictory changes:
    - In commit messages include design choices that resulted in the change
    - In commit messages include the intended effect of the changes
