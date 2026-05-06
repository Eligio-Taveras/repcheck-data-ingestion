---
name: diagnose-loop
description: Pause and produce a structured diagnosis when stuck in a build-fix loop. Invoke after 2-3 same-class failures (compile, test, build, deploy) before any further fix attempts.
---

# Diagnose Loop

You are stuck in a build-fix loop. STOP. Before any further fix attempts, fill in this template completely, then surface to the user and wait for confirmation.

## Template

**Command that fails**: <exact command, copy-paste from the most recent attempt>

**Error class**: <one sentence — what kind of failure is this? Be specific. Examples: "H2 doesn't support this DDL feature", "scalafix keeps stripping the import on every run", "CRLF mismatch causing scalafmt false positive", "test database state leaking between specs">

**Attempts so far**:
1. <what you tried> -> <what failed, exact error class>
2. <what you tried> -> <what failed>
3. <what you tried> -> <what failed>

**Working hypothesis for root cause**: <what is the actual underlying cause? Be specific. Avoid vague answers like "configuration issue" or "syntax problem".>

**Evidence for the hypothesis**: <what observation supports this? what would invalidate it?>

**Next move**: <what would you try next, AND WHY do you expect it to work given the hypothesis?>

**Alternative if next move fails**: <if the next move fails, what would you do instead? Switching tools, backends, or abstractions counts. Another variant of the same fix does NOT — that's a continuation of the loop.>

**Time elapsed on this loop**: <rough estimate>

## After surfacing the summary

Wait for the user to:
- Confirm the next move
- Suggest a different approach
- Take over the diagnosis themselves

Do not continue without explicit user confirmation. If the user is not present (autonomous run), pause and surface the summary; do not pick the next move on your own.

## When to invoke this skill

- After 2-3 consecutive failures of the same kind
- When the same test, build phase, or deploy step has failed more than twice with the same class of error
- When you notice yourself making syntactic variations of the same fix
- When elapsed time on the current task exceeds expectations and progress feels uncertain
- Whenever the user types "diagnose" or invokes this skill manually
