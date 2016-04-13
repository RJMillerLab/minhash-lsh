package experiment

// CmpMetric is a function takes two attributes q and a,
// and computes the measure relevance of a to q.
// It returns two values: the first is an integer that
// depends on q, the second is a real number independent of
// q.
type CmpMetric func(q, a *Attribute) (int, float64)

func ExactResemblance(q, a *Attribute) (r int, n float64) {
	qSize := len(q.Domain)
	aSize := len(a.Domain)
	if qSize == 0 || aSize == 0 {
		return 0, 0.0
	}
	var smaller, bigger *(map[string]bool)
	if qSize > aSize {
		smaller = &(a.Domain)
		bigger = &(q.Domain)
	} else {
		smaller = &(q.Domain)
		bigger = &(a.Domain)
	}
	intersection := 0
	for v := range *smaller {
		if _, exist := (*bigger)[v]; exist {
			intersection++
		}
	}
	r = intersection
	n = float64(intersection) / float64(qSize+aSize-intersection)
	return
}

func EstimatedResemblance(q, a *Attribute) (r int, n float64) {
	r = q.Signature.Intersection(a.Signature)
	n = float64(r) / float64(len(q.Signature))
	return
}
