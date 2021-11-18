# change input and output dir path based on your setup. 
hadoop fs -rm -r /a2/paralleldijkstra/output
hadoop jar paralleldijkstra.jar ParallelDijkstra /a2/paralleldijkstra/large /a2/paralleldijkstra/output 12 2 "non-weighted"
