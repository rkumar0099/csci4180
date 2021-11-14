hadoop fs -rm -r /a2/output || echo ""
javac -classpath `yarn classpath` PDPreProcess.java AdjacencyList.java Tuple.java
jar -cvf pdpreprocess.jar *.class
hadoop jar pdpreprocess.jar PDPreProcess /a2/input /a2/output
#hadoop fs -cat /a2/output/part-r-00000