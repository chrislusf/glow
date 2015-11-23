./word_count -glow -test 1 -glow.flow.plot > x.dot ; dot -Tpng -otestBasicMapReduce.png x.dot
./word_count -glow -test 2 -glow.flow.plot > x.dot ; dot -Tpng -otestPartitionAndSort.png x.dot
./word_count -glow -test 3 -glow.flow.plot > x.dot ; dot -Tpng -otestSelfJoin.png x.dot
./word_count -glow -test 4 -glow.flow.plot > x.dot ; dot -Tpng -otestJoin.png x.dot
./word_count -glow -test 5 -glow.flow.plot > x.dot ; dot -Tpng -otestInputOutputChannels.png x.dot
./word_count -glow -test 6 -glow.flow.plot > x.dot ; dot -Tpng -otestUnrolledStaticLoop.png x.dot
