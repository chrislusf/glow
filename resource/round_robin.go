package resource

// In the Round-Robin Protocol no pricing is used. The
// incoming task queries are matched with the ’next’ available
// resource offer which meets the task’s constraints —
// but which is usually not the ’best’. For this purpose an
// iterator is used which cycles through the list of Server
// resource offers.
// On arrival of a Task Query object, the list of resource
// offers is searched until a resource is found which satis-
// fies the task’s constraints { size, price, deadline }. The
// search starts at the current position of the iterator. In
// case of success, the resource offer is taken. The result is
// returned and the iterator is incremented. Otherwise, the
// iterator is also incremented and the next resource offer
// is considered. This step is repeated until all resource offers
// have been checked or a match has been found. If the
// query is successful, the result is sent to the task’s Client.
// Otherwise, the Task Query object remains at the EMP
// until a suitable resource becomes available. Whenever
// a resource becomes available, the Task Query objects at
// the EMP are processed (in the order of their arrival) until
// the resource offer is taken or all elements have been
// checked and no match was found.
