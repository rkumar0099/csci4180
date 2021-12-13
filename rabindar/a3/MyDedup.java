import java.io.*;
import java.util.*;
import java.nio.file.Files;
import java.lang.Math;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;




public class MyDedup {
    private static final int CONTAINER_SIZE = 1024*1024;
    private static PrintStream out = new PrintStream(System.out);
    private static final String INDEX_FILE = "index.txt";
    private static final String FILE_RECEIPTS = "recipe.txt";
    private static byte[] container = new byte[CONTAINER_SIZE];
    private static int containerCount, containerBytesStored;
    private static byte[] chunk;
    private static HashMap<String, Integer[]> fingerPrintIndex = new HashMap<String, Integer[]>();
    private static HashMap<String, HashMap<Integer, String>> fileReceipt = new HashMap<String, HashMap<Integer, String>>(); 
    private static int minSize, avgSize, maxSize;
    private static boolean finishChunking = false;
    private static int numChunks = 0;
    private static int activeThreads = 0; // when threads active are zero, exit main
    private static MessageDigest md;
    private static String dedupStorage;
    private static int totalBytesContainer = 0;
    private static int numDedupChunks = 0;
    private static int base;

    public static void initFingerPrintIndex() {
        BufferedReader bf = null;
        try {
            File file = new File(INDEX_FILE);
            if (file.createNewFile()) {
                out.println("MyDedup.index file created");
                return;
            } 
            bf = new BufferedReader(new FileReader(file));
            containerCount = Integer.parseInt(bf.readLine());
            String line;
            String hash;
            int containerNo, containerOffset, chunkSize, refCounter;
            while ((line = bf.readLine()) != null) {
                String[] tokens = line.split(",");
                if (tokens.length == 5) {
                    hash = tokens[0];
                    containerNo = Integer.parseInt(tokens[1]);
                    containerOffset = Integer.parseInt(tokens[2]);
                    chunkSize = Integer.parseInt(tokens[3]);
                    refCounter = Integer.parseInt(tokens[4]);
                    Integer[] payload = {containerNo, containerOffset, chunkSize, refCounter};
                    fingerPrintIndex.put(hash, payload);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (bf != null) {
                    bf.close();
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void storeFingerPrintIndex() {
        
            File file = new File(INDEX_FILE);
            BufferedWriter bf = null;
            try {
                bf = new BufferedWriter(new FileWriter(file));
    
                bf.write(Integer.toString(containerCount));
                
                for (Map.Entry<String, Integer[]> entry : fingerPrintIndex.entrySet()) {
                    bf.newLine();
                    bf.write(entry.getKey());
                    for (int i = 0; i < entry.getValue().length; i++) {
                        bf.write("," + entry.getValue()[i]);
                    }
                    
                }
    
                bf.flush();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                try {
                    bf.close();
                }
                catch (Exception e) {
                }
            }
    }

    public static void initFileReceipt() {

        BufferedReader bf = null;
        try {
            File file = new File(FILE_RECEIPTS);
            if (file.createNewFile()) {
                out.println("File receipt created");
                return;
            }
            bf = new BufferedReader(new FileReader(file));

            String line;
            String pathname;
            while ((line = bf.readLine()) != null) {
                String[] tokens = line.split(",");
                if (tokens.length != 0) {
                    pathname = tokens[0];
                    HashMap<Integer, String> fileChunks = new HashMap<Integer, String>();
                    for (int i = 1; i < tokens.length; i++) {
                        // tokens[i] is Id, tokens[i + 1] is chunkHash
                        fileChunks.put(Integer.parseInt(tokens[i]), tokens[i+1]);
                        i++;
                    }
                    fileReceipt.put(pathname, fileChunks);
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                bf.close();
            }
            catch (Exception e) {
            }
        }
    
    }

    public static void storeFileReceipt() {
        BufferedWriter bf = null;
        try {
            File file = new File(FILE_RECEIPTS);
            bf = new BufferedWriter(new FileWriter(file));

            
            for (Map.Entry<String, HashMap<Integer, String>> entry : fileReceipt.entrySet()) {
                bf.write(entry.getKey());
                HashMap<Integer, String> fileChunks = entry.getValue();
                for(Integer Id: fileChunks.keySet()) {      
                    bf.write("," + Id + "," + fileChunks.get(Id));
                }
        
                bf.newLine();
            }

            bf.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                bf.close();
            }
            catch (Exception e) {
            }
        }
    }


    public static int getMask(int avgChunkSize) {
        return avgChunkSize - 1;
    }

    public static int fastExp(int b, int e, int m ) {
        int a = 1;
        String s = Integer.toBinaryString(e);
        for(int i = 0; i < s.length(); i++) {
            a = (a*a) % m; 
            if (s.charAt(i) - '0' == 1) {
                a = (a*b) % m;
            } 
        }
        return a;
    }

    public static int modulo(int a, int b) {
        int val = (int)Math.floor((double)a / b) * b;
        return a - val;
    }

    public static boolean checkPowerTwo(int n)
    {
        return n != 0 && ((n & (n - 1)) == 0);
    }


    public static void print(byte[] chunk) {
        for (byte val: chunk) {
            out.print("" + val + " ");
        }
        out.println();
    }

    public static byte[] makeWindow(int s, int e, byte[] data) {
        byte[] window = new byte[e-s+1];
        int ind = 0;
        for(int i = s; i < e; i++) {
            window[ind] = data[i];
            ind += 1;
        }
        return window;
    }

    public static void deduplicate(byte[] chunk, int chunkSize, HashMap<Integer, String> fileChunks) {
        try {
            md.update(chunk, 0, chunkSize);
            byte[] checkSum = md.digest();
            String chunkHash = Base64.getEncoder().encodeToString(checkSum);
            md.reset();

            if (fingerPrintIndex.containsKey(chunkHash)) {
                Integer[] metaData = fingerPrintIndex.get(chunkHash);
                metaData[3] += 1;
                numDedupChunks += 1;
                out.println("Chunk deduplicated");
            } else {
                // if container has only 10kb left, size of chunk is 12kb
                // test on binary file
                int spaceLeft = CONTAINER_SIZE - containerBytesStored;
                if (spaceLeft < chunkSize) {
                    // create new task to store the container in local or azure
                    ProcessContainer task = new ProcessContainer(container, containerBytesStored, containerCount, dedupStorage);
                    new Thread(task).start();
                    container = new byte[CONTAINER_SIZE];
                    totalBytesContainer += containerBytesStored;
                    containerCount += 1;
                    containerBytesStored = 0;
                    deduplicate(chunk, chunkSize, fileChunks);
                } else {
                    int startIndex = containerBytesStored;
                    for(int i = 0; i < chunkSize; i++) {
                        container[containerBytesStored] = chunk[i];
                        containerBytesStored += 1;
                    }
                    Integer[] metaData = {containerCount, startIndex, chunkSize, 1};
                    fingerPrintIndex.put(chunkHash, metaData);
                    out.println("Chunk stored in container");
                }
            }
            if (!fileChunks.containsKey(numChunks)) {
                fileChunks.put(numChunks, chunkHash);
            }

        } catch(Exception e) {
            e.printStackTrace();
        }

    }

    public static int calcRes(int ps, int d, int m, int ts, int q) {
        int ds = modulo((fastExp(d, m-1, q) * (modulo(ts, q))), q);
        int res = modulo(((modulo(ps, q)) - ds), q);
        return res;
    }

    public static int rfp(int m, int d, int q, byte[] chunk, FileInputStream fs) {
        int sum = 0;
        int mask = getMask(q);
        boolean chunkFound = false;
        int prevRFP = -1;

        // compute p0
        for (int i = 1; i <= m; i++) {
            int r1 = chunk[i - 1] % q;
            int r2 = fastExp(d, m - i, q);
            sum += (r1*r2) % q;
        }
        prevRFP = sum % q;

        if ((prevRFP & mask) == 0) {
            //out.println("Chunk found at " + m);
            chunkFound = true;
            return m;
        }

        // compute pi until rfp = 0
        // also do it with s = 0, see eof
        int s = 0;
        int bytesRead = m;
        try {

            while(s+m < maxSize) {
                int res = calcRes(prevRFP, d, m, chunk[s], q);
                int a1 = modulo((modulo(d, q) * res), q);
                int val = fs.read();
                if (val == -1) {
                    finishChunking = true;
                    break;
                }
                bytesRead += 1;
                chunk[s+m] = (byte)val;
                int ps = modulo((a1 + (val % q)), q);
          
                if ((ps & mask) == 0) {
                    chunkFound = true;
                    break;
                }
                prevRFP = ps;
                s += 1;
            }
        } catch(Exception e) {
            e.printStackTrace();
        }   
        //if(chunkFound) {
            //out.println("Chunk found");
        //}
        return bytesRead;
    }

    public static void uploadFile(String pathname) {
        if (fileReceipt.containsKey(pathname)) {
            out.println("File with entered pathname is already uploaded. Enter different file pathname");
            return;
        }
        HashMap<Integer, String> fileChunks = new HashMap<Integer, String>();
        fileReceipt.put(pathname, fileChunks);

        long start = System.currentTimeMillis();
        try {
            int read = 0;
            int totalBytesRead = 0;

            File file = new File(pathname);
            FileInputStream fs = new FileInputStream(file);

            out.println(totalBytesRead);
            while(!finishChunking) {
                chunk = new byte[maxSize];
                read = fs.read(chunk, 0, minSize);

                if (read == -1) {
                    finishChunking = true;
                    break;
                }

                if (read < minSize && read > 0) {
                    totalBytesRead += read;
                    out.println(totalBytesRead);
                    numChunks += 1;
                    deduplicate(chunk, read, fileChunks);
                    finishChunking = true;
                    // process the chunk in separate thread
                    break;
                }

                if (read == minSize) { 
                    int bytesRead = rfp(minSize, base, avgSize, chunk, fs);
                    totalBytesRead += bytesRead;
                    numChunks += 1;
                    deduplicate(chunk, bytesRead, fileChunks);
                    out.println(totalBytesRead);
                    // process the chunk in separate thread
                }

                // to test first 20 points with ta program
                
                //if (numChunks == 1000) {
                    //out.println("Time took to generate " + numChunks + ": " + (System.currentTimeMillis() - start)/1000.0 + " seconds");
                    //return;
                //}
                
            }
            if (containerBytesStored > 0) {
                ProcessContainer task = new ProcessContainer(container, containerBytesStored, containerCount, dedupStorage);
                containerCount += 1;
                totalBytesContainer += containerBytesStored;
                new Thread(task).start();
            }
            while(activeThreads > 0) {
                Thread.sleep(100);
            }
            storeFingerPrintIndex();
            storeFileReceipt();

        } catch(Exception e) {
            out.println("[Error] Can't open the upload file. ");
            e.printStackTrace();
        }
        out.println("Total bytes stored in containers are: " + totalBytesContainer);
        int uniqueChunks = fingerPrintIndex.size();
        out.println("Unique chunks in storage: " + (numChunks - numDedupChunks) + "\nDeduplicate chunks for this upload: " + numDedupChunks);
        out.println("Time took to generate " + numChunks + ": " + (System.currentTimeMillis() - start)/1000.0 + " seconds");
    }

    public static void normalDownloadFile(String pathname, String localFileName) {
        try {
            if (!fileReceipt.containsKey(pathname)) {
                out.println("[Error] File with entered pathname is not uploaded in the system.");
                return;
            }
            HashMap<Integer, String> fileChunks = fileReceipt.get(pathname);
            int totalNumberChunks = fileChunks.size();
            int spaceLeft = CONTAINER_SIZE;
            int offset = 0;
            String currDir = System.getProperty("user.dir");
            FileOutputStream output = new FileOutputStream(currDir + "/" + localFileName);

            for (int i = 1; i <= totalNumberChunks; i++) {
                String chunkHash = fileChunks.get(i);
                Integer[] metaData = fingerPrintIndex.get(chunkHash);
                File file = new File(currDir + "/data/container" + metaData[0]);
                FileInputStream fs = new FileInputStream(file);
                fs.skip(metaData[1]);
                if(spaceLeft < metaData[2]) {
                    output.write(container, 0, offset);
                    container = new byte[CONTAINER_SIZE];
                    spaceLeft = CONTAINER_SIZE;
                    offset = 0;
                }
                int read = fs.read(container, offset, metaData[2]);
                if (read != metaData[2]) {
                    out.println("[Error] Can't read correct number of bytes from container while downloading");
                    return;
                }
                spaceLeft -= read;
                offset += read;
                fs.close();
             
            }

            if (offset > 0) {
                output.write(container, 0, offset);
            }

            output.close();
            out.println("File downloaded successfully");

        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void multithreadDownloadFile(String pathname, String localFileName) {
        try {
            if (!fileReceipt.containsKey(pathname)) {
                out.println("[Error] File with entered pathname is not uploaded in the system.");
                return;
            }
            
            
            String currDir = System.getProperty("user.dir");
            File threadFile = new File(currDir + "/temp");
            if (!threadFile.exists()) {
                threadFile.mkdir();
            }
            int numTasks = 0;
            int totalNumberChunks = fileReceipt.get(pathname).size();
            int ratio = totalNumberChunks / 500;
            int start = 1;
            for(int i = 0; i < ratio; i++) {
                numTasks += 1;
                FetchChunks task = new FetchChunks(pathname, numTasks, start, start+500-1);
                new Thread(task).start();
                start += 500;
            }
            if (start <= totalNumberChunks) {
                numTasks += 1;
                FetchChunks task = new FetchChunks(pathname, numTasks, start, totalNumberChunks);
                new Thread(task).start();
            }
            while(activeThreads > 0) {
                Thread.sleep(100);
            }
            

            File file = new File(currDir + "/" + localFileName);
            FileOutputStream output = new FileOutputStream(file, true);

            for(int i = 1; i <= numTasks; i++) {
                File tempFile = new File(currDir + "/temp/file" + i);
                FileInputStream fs = new FileInputStream(tempFile);
                while(true) {
                    int read = fs.read(container, 0, CONTAINER_SIZE);
                    if (read == -1) {
                        break;
                    }
                    output.write(container, 0, read);
                }
                fs.close();

            }
            output.close();
            File[] tempFiles = threadFile.listFiles();
            for(File f: tempFiles) {
                f.delete();
            }
            threadFile.delete();
            out.println("File downloaded successfully");


        } catch(Exception e) {

        }
    }

    public static void deleteFile(String pathname) {

    }
    

    public static void main(String[] args) {
        try {

            initFingerPrintIndex();
            initFileReceipt();
            md = MessageDigest.getInstance("SHA-256");
            String option = args[0];
            String pathname;

            switch(option) {
                case "upload":
                    minSize = Integer.parseInt(args[1]);
                    avgSize = Integer.parseInt(args[2]);
                    maxSize = Integer.parseInt(args[3]);
                    if (!checkPowerTwo(minSize)) {
                        out.println("[Error] Minimum size of chunk must be a power of 2");
                        return;
                    } 
                    if (!checkPowerTwo(avgSize)) {
                        out.println("[Error] Average size of chunk must be a power of 2");
                        return;
                    }
                    if (!checkPowerTwo(maxSize)) {
                        out.println("[Error] Maximum size of chunk must be a power of 2");
                        return;
                    }
                    dedupStorage = args[6];
                    base = Integer.parseInt(args[4]);
                    uploadFile(args[5]);
                    break;
            
                case "download":
                    pathname = args[1];
                    String downloadFile = args[2];
                    dedupStorage = args[3];
                    long start = System.currentTimeMillis();
                    //multithreadDownloadFile(pathname, downloadFile);
                    normalDownloadFile(pathname, downloadFile);
                    out.println("File downloaded in " + (System.currentTimeMillis() - start)/1000 + " seconds");
                    break;

                case "delete":
                    pathname = args[1];
                    deleteFile(pathname);
                    break;

                default:
                    out.println("[Error] Choose upload, download, or delete.");
                    return;

            }

        } catch(Exception e) {
            e.printStackTrace();
        }            
            
            /*
            String s = "2144364329";
            byte[] window = new byte[s.length()];

            for(int i = 0; i < s.length(); i++) {
                window[count] = (byte)(s.charAt(i) - '0');
                //out.println(window[count]);
                count += 1;
                
            }
            */

}


    public static class ProcessContainer implements Runnable {
        byte[] container;
        int containerSize;
        int containerID;
        String storage;

        public ProcessContainer(byte[] container, int containerSize, int containerID, String storage) {
            this.container = container;
            this.containerSize = containerSize;
            this.containerID = containerID;
            this.storage = storage;
        }

        @Override
        public void run() {
            try {
                incrementThread();
                if (dedupStorage.equals("local")) {

                    String currDir = System.getProperty("user.dir"); 
                    File file = new File(currDir + "/data");
                    if (!file.exists()) {
                        file.mkdir();
                    }
                    FileOutputStream writer = new FileOutputStream(currDir + "/data/container" + containerID);
                    writer.write(container);
                    writer.close();
                } else if (dedupStorage.equals("azure")) {
                    // store the container in cloud
                }
                decrementThread();

            } catch(Exception e) {
                out.println("[Error] Error while storing container");
                e.printStackTrace();
            }
        }

        public synchronized void incrementThread() {
            activeThreads += 1;
            out.println(activeThreads);
        }

        public synchronized void decrementThread() {
            activeThreads -= 1;
            out.println("Thread decrement");
        }
    }

    public static class FetchChunks implements Runnable {
        String filename;
        int startID, endID, threadID;

        public FetchChunks(String filename, int threadId, int start, int end) {
            this.filename = filename;
            this.startID = start;
            this.endID = end;
            this.threadID = threadId;
        }

        @Override 
        public void run() {
            try {
                incrementThread();
                HashMap<Integer, String> fileChunks = fileReceipt.get(filename);
                String currDir = System.getProperty("user.dir");
                String path = currDir + "/temp/file" + threadID;
                FileOutputStream output = new FileOutputStream(path, true);
                for(int i = startID; i <= endID; i++) {
                    String chunkHash = fileChunks.get(i);
                    Integer[] metaData = fingerPrintIndex.get(chunkHash);
                    File file = new File(currDir + "/data/container" + metaData[0]);
                    FileInputStream fs = new FileInputStream(file);
                    fs.skip(metaData[1]);
                    byte[] data = new byte[metaData[2]];
                    int read = fs.read(data, 0, metaData[2]);
                    if (read == metaData[2]) {
                        output.write(data);
                    } 
                }
                decrementThread();

            } catch(Exception e) {
                e.printStackTrace();
            }
        }
        public synchronized void incrementThread() {
            activeThreads += 1;
            out.println("Thread incremented");
        }

        public synchronized void decrementThread() {
            activeThreads -= 1;
            out.println("Thread decrement");
        }
    }

}