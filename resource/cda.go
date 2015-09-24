package resource

//
// Continuous Double Auction Protocol
// (CDA) is to allocate the best possible resource to an arriving
// task and to prioritise tasks according to their price
// bid. When a Task Query object arrives at the market the
// protocol searches all available resource offers and returns
// the first occurrence of the ’best’ match, i.e. the
// cheapest or the fastest resource which satisfies the task’s
// constraints. Whenever a resource becomes available and
// there are several tasks waiting, the one with the highest
// price bid is processed first.
