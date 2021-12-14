rm *.class downloaed_file
javac MyDedup.java 
# upload procedure. java MyDedup upload min_chunk avg_chunk, max_chunk <d> file storage
# simulate program with different parameters to check correctness
#java MyDedup upload 1024 2048 4096 257 localStorage
#java MyDedup upload 512 1024 2048 257 ./f.txt localStorage
java MyDedup upload 4096 8192 16384 257 ./large.txt "local"
