rm *.jar || echo ""
rm -r ./src || echo ""

test -d "/src" || mkdir ./src

javac -classpath `yarn classpath` *.java -d src
jar -cvf paralleldijkstra.jar ParallelDijkstra -C src .

