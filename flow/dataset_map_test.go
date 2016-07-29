package flow

import (
	"fmt"
	"reflect"
	"testing"
)

func TestMapSingleParameter(t *testing.T) {

	New().Slice(
		[]int{1, 2, 3, 4, 5},
	).Map(func(t int) (int, int) {
		return t, t * 7
	}).Map(func(x, y int) {
		fmt.Println("x=", x, "7*x=", y)
	}).Run()

	t.Logf("single parameter mapping runs well")

}

func TestGroupByKeyMap(t *testing.T) {

	fmt.Println("group by result mapping testing start...")
	New().Slice(
		[]int{1, 1, 1, 1, 1, 2, 2, 2, 2,
			3, 3, 3, 4, 4, 5,
			100, 234, 43, 100, 43, 43, 43},
	).Map(func(t int) (int, int) {
		return t, t * 2
	}).Map(func(key, value int) (int, int) {
		return key, value * 3
	}).GroupByKey().Map(func(key int, values []int) {
		fmt.Printf("key: %d values: %v\n", key, values)
	}).Run()

	fmt.Println("group by result mapping runs well")

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
	cogroup_result := left.CoGroup(right).Map(func(key int, lefts, rights []int) result {
		return result{
			key:   key,
			left:  lefts,
			right: rights,
		}
	})

	outChan := make(chan result, 0)
	cogroup_result.AddOutput(outChan)
	go cogroup_result.Run()

	got := make([]result, 0, 2)
	for item := range outChan {
		got = append(got, item)
	}

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
