hadoop fs -rm -r /a2/pagerank/output
hadoop fs -rm -r /a2temp
hadoop jar pagerank.jar PageRank 0.4 1 /a2/pagerank/small /a2/pagerank/output