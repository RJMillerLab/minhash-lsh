import json, sys, collections
import numpy as np
import scipy
from scipy.stats import gamma
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
plt.style.use("acm-2col.mplstyle")

with open("JACCARD_DIST_DIST.json") as f:
    [gamma_x, gamma_xk, max_x] = json.load(f)

ks = [10, 20, 50]

fig, axes = plt.subplots(1, 2, figsize=(4, 2.2), sharex=True)

# Plot all pair distance distribution
x = np.linspace(0.0, max_x, num=100)
pdf = gamma.pdf(x, gamma_x[0], gamma_x[1], gamma_x[2]) 
pdf = pdf / np.sum(pdf)
axes[0].plot(x, pdf)
axes[0].set_ylabel("Probability")
axes[0].set_xlabel("Sqaured Jaccard Distance") 

# Plot kth nearest neighbour distance distribution
for i, k in enumerate(ks):
    shape, loc, scale = gamma_xk[k-1]
    pdf = gamma.pdf(x, shape, loc, scale) 
    pdf = pdf / np.sum(pdf)
    axes[1].plot(x, pdf, label="%d-NN" % k)
axes[1].legend(loc="lower right")
axes[1].set_xlabel("Sqaured Jaccard Distance") 

plt.savefig("dist_dist.pdf", bbox_inches='tight')
plt.close()
