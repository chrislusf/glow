// priority_queue_test.go
package util

import (
	"math/rand"
	"reflect"
	"testing"
)

func TestSortedData(t *testing.T) {
	data := make([]int32, 102400)
	for i := 0; i < 100; i++ {
		data[i] = rand.Int31n(15)
	}
	pq := NewPriorityQueue(func(a, b reflect.Value) bool {
		if a.Int() < b.Int() {
			return true
		}
		return false
	})
	for i := 0; i < 10; i++ {
		pq.Enqueue(reflect.ValueOf(rand.Int31n(5606)), i)
	}
	for pq.Len() > 0 {
		t, i := pq.Dequeue()
		println(t.Int(), i)
	}
}
