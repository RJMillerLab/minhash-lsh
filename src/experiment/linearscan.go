package experiment

type Linearscan struct {
	attributes []Attribute
}

func NewLinearscan(attributes []Attribute) *Linearscan {
	return &Linearscan{attributes}
}

func (l *Linearscan) TopK(query *Attribute,
	k int, threshold float64, metric CmpMetric) []*Attribute {
	topkQueue := NewTopKQueue(k)
	var normalizedDegree float64
	var degree int
	for i := range l.attributes {
		attr := &(l.attributes[i])
		degree, normalizedDegree = metric(query, attr)
		if normalizedDegree <= threshold {
			continue
		}
		topkQueue.Push(attr, degree)
	}
	out := make([]*Attribute, topkQueue.Size())
	for i := len(out) - 1; i >= 0; i-- {
		v, _ := topkQueue.Pop()
		out[i] = v.(*Attribute)
	}
	return out
}
