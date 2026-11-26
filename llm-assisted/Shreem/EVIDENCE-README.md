# Report evidence — Shreem Arjyal

Supporting evidence for the CS 789 Final Report. The report cites files here by name rather
than embedding every screenshot, following the Intermediate Report feedback to summarise
repetitive results and link to supporting evidence instead of reproducing it.

## screenshots/

| prefix | what it covers |
|---|---|
| `Cat01#12539-` | Category 1. Design targets present, clean cherry-pick, tests before and after. |
| `Cat02#17095-` | Category 2. JaCoCo coverage before and after, 8 tests passing. |
| `Cat02#12432-` | Category 2. JaCoCo coverage before and after at class and method level, 5 tests passing. |
| `Cat03#12288-` | Category 3. Cherry-pick failing with an empty HEAD side, RePatch database queries. |
| `Cat04#12752-` | Category 4. "deleted by us" cherry-pick, RePatch database, tests after adaptation. |
| `Cat04#14088-` | Category 4. Seven conflicts in two kinds, RePatch database. |
| `Cat04#12244-` | Category 4. Content-only conflicts, the RePatch log showing a rename inverted and replayed, tests after adaptation. |
| `Fig1`–`Fig4` | The four design-gap diagrams and the Category 4 taxonomy. |
| `Metrics-hotspot-analysis.png` | Hotspot analysis of `3.0-li`, change frequency against complexity for all 2,432 production source files, with my seven PRs' integration points marked. |
| `PartA#14088-` | LLM Part A. Model and hardware, baseline failure, the three attempts and their verification. |
| `PartB#12244-` | LLM Part B. Test baseline, coverage before and after, generated tests failing to compile, the repaired test, the can-it-fail check. |
| `Test#12752-`, `Test#12244-`, `Test#14088-` | The two regression tests added after the Intermediate, and the RePatch conflict-block queries from 16 November. |

## logs/

Raw text captured at the time rather than retyped: cherry-pick conflicts and status output for
every PR that failed, RePatch verdict tables for all four runs, and two analyses written on
16 November, `Cat04#12244-repatch-conflict-blocks.txt`, which breaks the 109 conflicting lines
into four blocks, and `Cat04-repatch-file-counting-finding.txt`, which records that RePatch's
file count omits Scala files and modify/delete conflicts.

Two files from a fifth run on 25 November 2026. `Cat04#14088-repatch-rerun-25nov.log` is the
console output of `repatch lab apache/kafka linkedin/kafka 14088`, with the IntelliJ indexer
warnings stripped and everything else intact; it carries the `Conflicting files (7)` line and
both `Scoped detection` lines. `Cat04-repatch-log-vs-database-finding.txt` records what that run
settled: RePatch names seven conflicting files in its log and stores one in `conflicting_file`,
and none of the 454 refactorings it detected mentions a Scala file, so the `Log.scala` to
`UnifiedLog.scala` rename was never detected rather than detected and discarded. That corrects
what I wrote in the Intermediate Report.

The LLM prompts, raw model responses, generated patches and verification logs are not here.
They live under `llm-assisted/Shreem/part-a/` and `part-b/` on this same branch.

## hotspot_analysis.py

The script that produces `screenshots/Metrics-hotspot-analysis.png`. It reads the `3.0-li`
history with git, counts how often each production source file changed, measures size and
decision points, ranks every file by change frequency times complexity, and marks where my
seven PRs' integration points fall. Run it with `python3 hotspot_analysis.py` from a checkout
of this fork; it needs matplotlib for the chart and prints the rankings either way. The
`REPO` path at the top points at my local clone and is the one line to change.
