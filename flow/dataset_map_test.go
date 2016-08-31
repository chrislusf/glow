package flow

import (
	"reflect"
	"testing"
)

func TestMapSingleParameter(t *testing.T) {
	dataset := New().Slice(
		[]int{1, 2, 3},
	).Map(func(t int) (int, int) {
		return t, t
	}).Map(func(x, y int) int {
		return x + y
	})
	got := collectOutput(dataset)

	if want := []int{2, 4, 6}; !reflect.DeepEqual(got, want) {
		t.Errorf("Got: %v want: %v", got, want)
	}
}

func TestGroupByKeyMap(t *testing.T) {
	dataset := New().Slice(
		[]int{1, 1, 2, 2, 3, 3},
	).Map(func(t int) (int, int) {
		return t, t * 2
	}).GroupByKey().Map(func(key int, values []int) []int {
		return append([]int{key}, values...)
	})

	got := collectOutput(dataset)

	if want := [][]int{{1, 2, 2}, {2, 4, 4}, {3, 6, 6}}; !reflect.DeepEqual(got, want) {
		t.Errorf("Got %v want %v", got, want)
	}
}

func TestCoGroupMap(t *testing.T) {
	f := New()
	left := f.Slice(
		[]int{1, 1, 2},
	).Map(func(t int) (int, int) {
		return t, t * 2
	})

	right := f.Slice(
		[]int{1, 2, 2},
	).Map(func(t int) (int, int) {
		return t, t * 5
	})

	type result struct {
		key   int
		left  []int
		right []int
	}
	cogroupResult := left.CoGroup(right).Map(func(key int, lefts, rights []int) result {
		return result{
			key:   key,
			left:  lefts,
			right: rights,
		}
	})

	got := collectOutput(cogroupResult)
	want := []result{
		{
			key:   1,
			left:  []int{2, 2},
			right: []int{5},
		}, {
			key:   2,
			left:  []int{4},
			right: []int{10, 10},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Got %v want %v", got, want)
	}
}
