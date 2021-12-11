javac MyDedup.java 
# upload procedure. java MyDedup upload min_chunk avg_chunk, max_chunk <d> file storage
# simulate program with different parameters to check correctness
#java MyDedup upload 1024 2048 4096 257 localStorage
#java MyDedup upload 512 1024 2048 257 ./f.txt localStorage
# run MyDedup on data we got in assign2 to generate anchor points and test the program on corner cases
java MyDedup upload 4096 8192 16384 257 ./f2.txt localStorage
