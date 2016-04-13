import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
plt.style.use("acm-2col.mplstyle")
import numpy as np
import json, sys

with open("ACCURACY.json") as f:
    result = json.load(f)

# NOTE: Assume the first result is always n = 1
ks = np.array(result["ks"])
results = result["results"]

fig, axes = plt.subplots(1, 3, figsize=(8, 2.2), sharex=True, sharey=True)
plt.ylim(0, 1)

fscore = lambda p, r : 2 * p * r / (p + r)

# Precision
ax = axes[0]
ps = np.array([np.mean(x["precisions"]) for x in results])
ax.plot(ks, ps)
ax.set_ylabel("Precision")
ax.set_xlabel("K")

# Recall
ax = axes[1]
rs = np.array([np.mean(x["recalls"]) for x in results])
ax.plot(ks, rs)
ax.set_ylabel("Recall")
ax.set_xlabel("K")

# F score
ax = axes[2]
fs = fscore(ps, rs)
ax.plot(ks, fs)
ax.set_ylabel("F score")
ax.set_xlabel("K")

plt.savefig("lsh_accuracy.pdf", bbox_inches='tight')
plt.close()


print("K", ks)
print("Precision", ps)
print("Recall", rs)
print("F-score", fs)
