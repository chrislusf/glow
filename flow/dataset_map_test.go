package flow

import (
	"fmt"
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

func TestCoGroupMap(t *testing.T) {

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

	t.Logf("group by result mapping runs well")

}
