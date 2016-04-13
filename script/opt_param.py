import json, sys, collections
import numpy as np
import scipy
from scipy.stats import gamma, norm
from scipy.integrate import quad
from scipy.constants import pi

def _hash_probability(m, l, r):
    p = 1.0 - (1.0 - (1.0 - float(r))**float(m))**float(l)
    if p < 0.0:
        print(m, l, r)
        raise ValueError()
    return p

def _recall(m, l, gamma_params, max_x):
    k = len(gamma_params)
    s = 0.0
    for i in range(1, k):
        shape, loc, scale = gamma_params[i]
        join_prob_func = lambda x : _hash_probability(m, l, np.sqrt(x)) * gamma.pdf(x, shape, loc, scale)
        prob, _ = quad(join_prob_func, 0.0, max_x) 
        s += prob
    return s / float(k - 1)

def _selectivity(m, l, gamma_param, max_x):
    shape, loc, scale = gamma_param
    join_prob_func = lambda x : _hash_probability(m, l, np.sqrt(x)) * gamma.pdf(x, shape, loc, scale)
    prob, _ = quad(join_prob_func, 0.0, max_x) 
    return prob

def optimization(h, max_x, gamma_x, gamma_xk, required_recall):
    best_m = 0
    best_l = 0
    best_selectivity = float('inf')
    for m in range(1, 5):
        for l in range(64, h+1, 2):
            if m*l > h:
                continue
            recall = _recall(m, l, gamma_xk, max_x) 
            print(m, l, recall)
            if recall < required_recall:
                continue
            selectivity = _selectivity(m, l, gamma_x, max_x)
            print(m, l, selectivity, recall)
            if selectivity < best_selectivity:
                print("Current best", selectivity, m, l)
                best_selectivity = selectivity
                best_m = m
                best_l = l
    print("Best overall m = %d, l = %d" % (best_m, best_l))
    return best_m, best_l

h = 512
required_recall = 0.6
with open("JACCARD_DIST_DIST.json") as f:
    [gamma_x, gamma_xk, max_x] = json.load(f)
optimization(h, max_x, gamma_x, gamma_xk, required_recall)
