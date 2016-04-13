import sys
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
plt.style.use("acm-2col.mplstyle")

size1 = [1,2,3,4,5]
size2 = [1,2,3,4]
lsh = np.array([254901,141464,231116,242489,331351]) / 1000000.0
ii = np.array([66089,70500,290608,1499199]) / 1000000.0
plt.figure(figsize=(4, 2.2))
plt.plot(size1,lsh, "-x", label="LSH")
plt.plot(size2,ii, "-+", label="Inverted Index")
plt.legend(loc=2)
plt.xlabel("Number of Tables (log base 10)")
plt.ylabel("Time (ms)")

plt.savefig("lsh_time.pdf", bbox_inches='tight')
