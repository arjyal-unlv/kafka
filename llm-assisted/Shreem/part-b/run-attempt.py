#!/usr/bin/env python3
"""
Run one Part A attempt against a local Ollama model and save the raw response.

Usage:  python3 llm-assisted/Shreem/part-b/run-attempt.py 01

Sends ONLY the text after the "## Prompt as sent" marker in the prompt file, so the
rationale notes above that line never reach the model. Settings are fixed here so the
run is reproducible; they are also written into the output header.
"""
import json, sys, time, urllib.request, pathlib

MODEL   = "qwen2.5-coder:7b"
OPTIONS = {"temperature": 0.2, "top_p": 0.9, "seed": 42, "num_ctx": 16384,
           "repeat_penalty": 1.15, "num_predict": 4096}
MARKER  = "## Prompt as sent"
BASE    = pathlib.Path(__file__).parent

n     = sys.argv[1]
# --no-penalty reproduces the original failing configuration (no repeat_penalty),
# kept so the repetition-loop failure mode can be demonstrated rather than asserted.
noP   = "--no-penalty" in sys.argv
if noP:
    OPTIONS.pop("repeat_penalty", None)
suffix = "-loop" if noP else ""
pfile = BASE / "prompts" / f"attempt-{n}.md"
ofile = BASE / "outputs" / f"attempt-{n}{suffix}-response-raw.md"

text = pfile.read_text()
if MARKER not in text:
    sys.exit(f"marker {MARKER!r} not found in {pfile}")
prompt = text.split(MARKER, 1)[1].lstrip("\n")

print(f"model   : {MODEL}")
print(f"options : {OPTIONS}")
print(f"prompt  : {pfile.name}  ({len(prompt)} chars)")
print("streaming response (ctrl-c to abort):\n" + "-"*60, flush=True)

req = urllib.request.Request(
    "http://localhost:11434/api/generate",
    data=json.dumps({"model": MODEL, "prompt": prompt,
                     "stream": True, "options": OPTIONS}).encode(),
    headers={"Content-Type": "application/json"})

parts, final, t0, note = [], {}, time.time(), ""
try:
    with urllib.request.urlopen(req, timeout=600) as r:
        for line in r:
            if not line.strip():
                continue
            d = json.loads(line)
            chunk = d.get("response", "")
            parts.append(chunk)
            sys.stdout.write(chunk); sys.stdout.flush()
            if d.get("done"):
                final = d
except KeyboardInterrupt:
    note = "INTERRUPTED BY USER (ctrl-c)"
    print("\n[interrupted — saving what was generated]")
except Exception as e:
    note = f"ERROR: {type(e).__name__}: {e}"
    print(f"\n[error — saving what was generated: {e}]")
secs = time.time() - t0
body = "".join(parts)

hdr = (f"<!--\nmodel: {MODEL}\noptions: {OPTIONS}\n"
       f"prompt_file: {pfile.name}\n"
       f"eval_count: {final.get('eval_count')} tokens\n"
       f"prompt_eval_count: {final.get('prompt_eval_count')} tokens\n"
       f"total_duration_s: {secs:.1f}\n"
       f"run_at: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
       f"done_reason: {final.get('done_reason')}\n"
       + (f"note: {note}\n" if note else "") + "-->\n\n")
ofile.write_text(hdr + body)

print("\n" + "-"*60)
print(f"done in {secs:.1f}s, {final.get('eval_count')} tokens generated")
print(f"saved   : {ofile}")
