package mr

import "container/heap"

type mergeItem struct {
	kv          KeyValue
	fileIndex   int
	recordIndex int
}

type mergeHeap []mergeItem

func newMergeHeap(kvas [][]KeyValue) mergeHeap {
	candidates := make(mergeHeap, 0, len(kvas))
	for idx, kvs := range kvas {
		if len(kvs) > 0 {
			candidates = append(candidates, mergeItem{
				kvs[0], idx, 0,
			})
		}
	}
	heap.Init(&candidates)
	return candidates
}

func collectKeyValues(candidates *mergeHeap, kvas [][]KeyValue, key string) []string {
	values := make([]string, 0)
	// Pop candidates while the smallest key matches the target key.
	for candidates.Len() > 0 && (*candidates)[0].kv.Key == key {
		item := heap.Pop(candidates).(mergeItem)
		fileIndex := item.fileIndex
		recordIndex := item.recordIndex
		fileKVs := kvas[fileIndex]
		// Collect consecutive values for key from this file.
		for recordIndex < len(fileKVs) {
			kv := fileKVs[recordIndex]
			if kv.Key != key {
				// Push the first non-matching KV back onto the heap.
				heap.Push(candidates, mergeItem{
					kv, fileIndex, recordIndex,
				})
				break
			}
			values = append(values, kv.Value)
			recordIndex++
		}
	}
	return values
}

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return h[i].kv.Key < h[j].kv.Key }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(mergeItem))
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = mergeItem{}
	*h = old[:n-1]
	return item
}
