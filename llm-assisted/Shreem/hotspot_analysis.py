#!/usr/bin/env python3
"""
Hotspot analysis of linkedin/kafka 3.0-li.

Technique from the Metrics and Visualization lab (hotspot analysis = change
frequency x complexity, the OORP pattern "Study the Exceptional Entities"),
computed from git history instead of CodeScene's hosted analysis.

Question: are the files my seven assigned PRs integrate into already
hotspots in the target variant?

Usage:  python3 hotspot_analysis.py
"""
import subprocess, re, sys, os
from collections import Counter

REPO   = os.path.expanduser("~/Desktop/SoftwareReengineering/kafka")
BRANCH = "3.0-li"
OUT    = os.path.dirname(os.path.abspath(__file__))

# the integration points of my seven PRs that exist in 3.0-li
TARGETS = {
 "common/network/PlaintextChannelBuilder.java":          "#12539",
 "common/network/SaslChannelBuilder.java":               "#12539",
 "common/network/SslChannelBuilder.java":                "#12539",
 "trogdor/workload/GaussianThroughputGenerator.java":    "#17095",
 "admin/internals/AlterConsumerGroupOffsetsHandler.java":"#12432",
 "connect/mirror/MirrorCheckpointTask.java":             "#12432",
 "streams/processor/internals/ClientUtils.java":         "#12288",
 "clients/producer/KafkaProducer.java":                  "#12752",
 "kafka/server/ReplicaManager.scala":                    "#14088",
 "consumer/internals/ConsumerCoordinator.java":          "#12244",
}

def tag_of(path):
    for suffix, pr in TARGETS.items():
        if path.endswith(suffix):
            return pr
    return None

def git(*args):
    return subprocess.run(["git","-C",REPO,*args], capture_output=True,
                          text=True, errors="ignore").stdout

print("1/4  counting how often each file changed ...")
log = git("log", BRANCH, "--name-only", "--format=%n")
churn = Counter(l.strip() for l in log.splitlines()
                if l.strip() and re.search(r"/src/main/.*\.(java|scala)$", l))
print(f"     {len(churn)} production source files touched across the history")

print("2/4  measuring size and complexity of each file ...")
files = [f for f in git("ls-tree","-r","--name-only",BRANCH).splitlines()
         if re.search(r"/src/main/.*\.(java|scala)$", f)]
spec  = "\n".join(f"{BRANCH}:{f}" for f in files)
batch = subprocess.run(["git","-C",REPO,"cat-file","--batch"], input=spec,
                       capture_output=True, text=True, errors="ignore").stdout

DECISION = re.compile(r"\b(if|for|while|case|catch)\b|&&|\|\||\?")
rows, pos = [], 0
for f in files:
    nl = batch.find("\n", pos)
    if nl == -1: break
    header = batch[pos:nl].split()
    if len(header) < 3: pos = nl + 1; continue
    size = int(header[2])
    body = batch[nl+1:nl+1+size]
    pos  = nl + 1 + size + 1
    loc  = body.count("\n") + 1
    cplx = len(DECISION.findall(body))
    rows.append((f, churn.get(f, 0), loc, cplx))

print(f"     measured {len(rows)} files")

print("3/4  ranking hotspots ...")
# hotspot score = change frequency x complexity, the standard formulation
scored = sorted(((f,c,loc,cx,c*cx) for f,c,loc,cx in rows),
                key=lambda r: r[4], reverse=True)
rank = {r[0]: i+1 for i,r in enumerate(scored)}
total = len(scored)

def show(title, items):
    print(f"\n{title}")
    print(f"{'rank':>5}  {'changes':>7} {'LOC':>6} {'cplx':>5}  file")
    print("-"*96)
    for f,c,loc,cx,s in items:
        tag = f"   <-- {tag_of(f)}" if tag_of(f) else ""
        print(f"{rank[f]:>5}  {c:>7} {loc:>6} {cx:>5}  {f.split('/src/main/')[-1][:58]}{tag}")

show("TOP 20 HOTSPOTS IN 3.0-li", scored[:20])

mine = [r for r in scored if tag_of(r[0])]
show("MY SEVEN PRs' INTEGRATION POINTS", mine)

print(f"\n{'':5}  (ranked out of {total} production source files)")

print("\n4/4  drawing the chart ...")
try:
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(11,7.5))
    bx = [r[1] for r in scored]; by = [r[3] for r in scored]
    ax.scatter(bx, by, s=12, c="#c9d4e0", edgecolors="none", label="all production files", zorder=1)
    mx = [r[1] for r in mine];  my_ = [r[3] for r in mine]
    ax.scatter(mx, my_, s=90, c="#c0392b", edgecolors="white", linewidths=1.2,
               label="my seven PRs' integration points", zorder=3)
    # stagger labels so the three top-20 hotspots do not collide
    OFFS = {"#14088":(8,12),"#12752":(8,-24),"#12244":(-104,-20),"#12539":(8,6),
            "#12288":(8,-16),"#12432":(8,8),"#17095":(8,-18)}
    seen = {}
    for f,c,loc,cx,s in mine:
        pr = tag_of(f); n = seen.get(pr,0); seen[pr] = n+1
        dx,dy = OFFS.get(pr,(8,6)); dy = dy - 13*n
        ax.annotate(f"{os.path.basename(f)}\n{pr}", (c,cx), fontsize=7.5,
                    xytext=(dx,dy), textcoords="offset points", zorder=4)
    ax.set_xscale("symlog"); ax.set_yscale("symlog")
    ax.set_xlabel("change frequency  (commits touching the file, 3.0-li history)")
    ax.set_ylabel("complexity  (decision points)")
    ax.set_title("Hotspots in linkedin/kafka 3.0-li: change frequency vs complexity\n"
                 "Study the Exceptional Entities (OORP p. 75), computed from git history",
                 fontsize=11)
    ax.grid(alpha=.25, zorder=0); ax.legend(loc="upper left", fontsize=9)
    fig.tight_layout()
    out = os.path.join(OUT, "Metrics-hotspot-analysis.png")
    fig.savefig(out, dpi=170)
    print(f"     saved: {out}")
except ImportError:
    print("     matplotlib not installed - the tables above are still usable")
    print("     install with: python3 -m pip install matplotlib")
