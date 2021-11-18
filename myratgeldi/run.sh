hadoop fs -rm -r /a2/paralleldijkstra/output /output*
hadoop jar my.jar ParallelDijkstra /a2/paralleldijkstra/small /a2/paralleldijkstra/output 1 0 "non-weighted"