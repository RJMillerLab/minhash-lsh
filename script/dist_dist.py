'''
Plot the histogram of top-k distances
'''

import json, sys, collections
import numpy as np
import scipy
from scipy.stats import gamma
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

with open("ALL_PAIR_JACCARD.json") as f:
    all_pairs = json.load(f)

print("Get all pairs distribution")
all_pair_dists = np.square(np.array(all_pairs).flatten())
gamma_x = gamma.fit(all_pair_dists)

print("Get top k distance distribution")
k = 50
topk_dists = np.square(np.sort(all_pairs, axis=1))
gamma_xk = []
for i in range(k):
    dists = topk_dists[:,i]
    gamma_xk.append(gamma.fit(dists))
max_x = np.max(all_pair_dists)
with open("JACCARD_DIST_DIST.json", "w") as f:
    d = [gamma_x, gamma_xk, max_x] 
    json.dump(d, f)
print("Output file")

