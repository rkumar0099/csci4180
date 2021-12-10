import java.util.*;
import java.lang.Math;
import java.lang.System;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MyDedup {
    private static int minChunkSize, avgChunkSize, maxChunkSize, d;
    private static String storage;
    private static HashMap<Integer,Integer> usedContainers = new HashMap<Integer,Integer>();
    private static HashMap<String, int[]> fingerprintIndex = new HashMap<String, int[]>();
    private static HashMap<String, ArrayList<String>> fileRecipes = new HashMap<String, ArrayList<String>>();
    private static final String INDEX_FILE = "index.txt";
    private static final String FILE_RECIPES = "recipe.txt";
    private static int containerOffset = 0;
    private static int chunkOffset = 0;
    private static byte[] container = new byte[1048576];
    private static byte[] chunk;
    private static MessageDigest md;
    private static int containerCount = 0;
    private static int totalNumBytesRead = 0;
    private static ArrayList<String> chunkHashes = new ArrayList<String>();
    //public static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=csci4180tut9;AccountKey=11FcKJd1dXb740YeQ1gwKAx7FFWuuP52vkNF1PDoxAII1kQvAGV3/vjAvvVOEiTJ6XtomOp6VUlEq8V8z8eqhw==;EndpointSuffix=core.windows.net"
    //private static CloudStorageAccount storageAccount;
    //private static CloudBlobClient blobClient
    //CloudBlobContainer container;

    public static boolean checkPowerTwo(int n)
    {
        return n != 0 && ((n & (n - 1)) == 0);
    }


    public static void readFingerprintIndex() {
        File file = new File(INDEX_FILE);
        BufferedReader bf = null;
        try {
            bf = new BufferedReader(new FileReader(file));
            containerCount = Integer.parseInt(bf.readLine());
            String line;
            String hash;
            int containNo, containOff, chunkSize, pointers;
            while ((line = bf.readLine()) != null) {
                String[] tokens = line.split(",");
                if (tokens.length == 5) {
                    hash = tokens[0];
                    containNo = Integer.parseInt(tokens[1]);
                    if (!usedContainers.containsKey(containNo))
                    {
                        usedContainers.put(containNo,1);
                    }
                    else
                    {
                        usedContainers.replace(containNo, usedContainers.get(containNo)+1);
                    }
                    containOff = Integer.parseInt(tokens[2]);
                    chunkSize = Integer.parseInt(tokens[3]);
                    pointers = Integer.parseInt(tokens[4]);
                    int[] payload = {containNo, containOff, chunkSize, pointers};
                    fingerprintIndex.put(hash, payload);
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

    private static void readFileRecipes() {
        File file = new File(FILE_RECIPES);
        BufferedReader bf = null;
        try {
            bf = new BufferedReader(new FileReader(file));

            String line;
            String pathname;
            while ((line = bf.readLine()) != null) {
                String[] tokens = line.split(",");
                if (tokens.length != 0) {
                    pathname = tokens[0];
                    ArrayList<String> hashes = new ArrayList<String>();
                    for (int i = 1; i < tokens.length; i++) {
                        //System.out.println(tokens[1]);
                        hashes.add(tokens[i]);
                    }
                    fileRecipes.put(pathname, hashes);
                   // System.out.println("readFileRecipes");
                 //   System.out.println(fileRecipes);
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

    public static void writeFileRecipes() {
        File file = new File(FILE_RECIPES);
        BufferedWriter bf = null;
        try {
            bf = new BufferedWriter(new FileWriter(file));

            
            for (Map.Entry<String, ArrayList<String>> entry : fileRecipes.entrySet()) {
                bf.write(entry.getKey());
                for (int i = 0; i < entry.getValue().size(); i++) {
                    bf.write("," + entry.getValue().get(i));
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

    public static void writeFingerprintIndex() {
        
        File file = new File(INDEX_FILE);
        BufferedWriter bf = null;
        try {
            bf = new BufferedWriter(new FileWriter(file));

            bf.write(Integer.toString(containerCount));
            
            for (Map.Entry<String, int[]> entry : fingerprintIndex.entrySet()) {
                bf.newLine();
                bf.write(entry.getKey());
                for (int i = 0; i < entry.getValue().length; i++) {
                    bf.write("," + entry.getValue()[i]);
                }
                
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

    public static int power(int num, int pow) {
        int result = 1;
        for (int i = 0; i < pow; i++) {
            result = (result * (num % avgChunkSize)) % avgChunkSize;
        }
        return result;
    }

    public static void processChunk() {
        // compute checksum
        md.update(chunk, 0, chunkOffset);
        byte[] checkSumBytes = md.digest();
        String hash = Base64.getEncoder().encodeToString(checkSumBytes);
        md.reset();
        // put chunk hash into file recipe
        chunkHashes.add(hash);
        // check if chunk already exists in fingerprint index
        if (!fingerprintIndex.containsKey(hash)) {
            // check for container overflow
            if (containerOffset + chunkOffset > container.length) {
                // upload current container
                if (storage.equals("local")) {
                    // check if data directory exists, create if not
                    String currentDirectory = System.getProperty("user.dir");
                    File file = new File(currentDirectory + "/data");
                    if (!file.exists()) {
                        file.mkdir();
                    }
                    try (FileOutputStream fos = new FileOutputStream(currentDirectory + "/data/container" + containerCount)) {
                        fos.write(container);
                    }
                    catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                else if (storage.equals("azure")) {

                }
                container = new byte[1048576];
                containerOffset = 0;
                containerCount++;
            }
            // move chunk into container
            System.arraycopy(chunk, 0, container, containerOffset, chunkOffset);
            // update fingerprint index

            int[] chunkInformation = {containerCount, containerOffset, chunkOffset, 1};
            containerOffset += chunkOffset;
            fingerprintIndex.put(hash, chunkInformation);
        }
        else{
            int[] tmp = fingerprintIndex.get(hash);
            tmp[3]++;
            fingerprintIndex.put(hash,tmp);
        }
        chunk = new byte[maxChunkSize];
        chunkOffset = 0;
    }
    private static void getFileLocal(String downloadFile,String localFile)
    {
        ArrayList<String> chunks = fileRecipes.get(downloadFile);
        //System.out.println(fileRecipes);
            String currentDirectory = System.getProperty("user.dir");
            try{
                FileOutputStream fos = new FileOutputStream(localFile,true);
                for (String chunk : chunks){
                    //System.out.println(chunk);
                    int[] x = fingerprintIndex.get(chunk);
                    int containerNum = x[0], _containerOffset=x[1], chunkSize=x[2];
                    //System.out.println(_containerOffset);
                    InputStream fis = new FileInputStream(currentDirectory + "/data/container" + containerNum);
                    fis.skip(_containerOffset);
                    byte[] buff = new byte [chunkSize];
                    fis.read(buff,0,chunkSize);
                    fos.write(buff);
                    fos.flush();
                    fis.close();
                }
                fos.close();
            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        return;
    }
    private static void deleteFileLocal(String fileName){
        ArrayList<String> chunks = fileRecipes.get(fileName);
        fileRecipes.remove(fileName);
        for (String chunk : chunks)
        {
            int[] x = fingerprintIndex.get(chunk);
            int containerNum = x[0], cnt=x[3];
            cnt--;
            if (cnt == 0){
                System.out.println("cnt= "+cnt);
                usedContainers.put(containerNum, usedContainers.get(containerNum)-1);
                fingerprintIndex.remove(chunk);
            }
            else{
                x[3]--;
                fingerprintIndex.put(chunk, x);
            }
            if (usedContainers.get(containerNum)==0){
                System.out.println("usedContainers.get(containerNum)= "+usedContainers.get(containerNum));
                String path = System.getProperty("user.dir") + "/data/container" + containerNum;
                try {
                    File containerFile = new File(path); 
                    containerFile.delete();
                }
                catch(Exception e){
                    System.out.println("Failed to delete the file.");   
                }
            }
        }
    }
    public static void main(String args[]) throws NoSuchAlgorithmException {
        //storageAccount = CloudStorageAccount.parse(storageConnectionString);
        //blobClient = storageAccount.createCloudBlobClient();
        //container = blobClient.getContainerReference("csci4180-asg3");
        //container.createIfNotExist();
        // int minChunkSize, avgChunkSize, maxChunkSize, d;
        String uploadFile, downloadFile, deleteFile, localFile;
        String command = args[0];
        readFingerprintIndex();
        readFileRecipes();
        md = MessageDigest.getInstance("SHA-256");
        if (command.equals("upload")) {
            minChunkSize = Integer.parseInt(args[1]);
            avgChunkSize = Integer.parseInt(args[2]);
            maxChunkSize = Integer.parseInt(args[3]);
            assert checkPowerTwo(minChunkSize) : "min chunk size is not power of two";
            assert checkPowerTwo(avgChunkSize) : "avg chunk size is not power of two";
            assert checkPowerTwo(maxChunkSize) : "max chunk size is not power of two";
            d = Integer.parseInt(args[4]);
            uploadFile = args[5];
            storage = args[6];
            try {
                InputStream input = new FileInputStream(uploadFile);
                int mask = avgChunkSize-1;
                chunk = new byte[maxChunkSize];
                boolean finishedChunking = false;
                boolean newChunk = true;
                int curRFP = 0;
                while (!finishedChunking) {
                    if (newChunk) {
                        int numBytesRead = input.read(chunk, chunkOffset, minChunkSize);
                        if (numBytesRead != -1) {chunkOffset += numBytesRead; totalNumBytesRead += numBytesRead;}
                        if (numBytesRead == -1 || numBytesRead < minChunkSize) {
                            // eof reached
                            finishedChunking = true;
                            processChunk();
                            System.out.println("new Chunk -> " + chunkOffset);
                            //System.out.println(totalNumBytesRead);
                            // check if data directory exists, create if not
                            String currentDirectory = System.getProperty("user.dir");
                            File file = new File(currentDirectory + "/data");
                            if (!file.exists()) {
                                file.mkdir();
                            }
                            if (containerOffset!=0)
                            {
                                try (FileOutputStream fos = new FileOutputStream(currentDirectory + "/data/container" + containerCount)) {
                                    fos.write(container);
                                    containerCount++;
                                }
                                catch (FileNotFoundException e) {
                                    e.printStackTrace();
                                }
                                catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                            fileRecipes.put(uploadFile, chunkHashes);
                        }
                        else {
                            // first time computing RFP for this chunk
                            curRFP = 0;
                            for (int i = 0; i < minChunkSize; i++) {
                                curRFP = (curRFP + (chunk[i] * power(d, minChunkSize - 1 - i)) % avgChunkSize) % avgChunkSize;
                            }
                            
                            if ((curRFP & mask) == 0) {
                                processChunk();
                                System.out.println(totalNumBytesRead);
                            }
                            else {
                                newChunk = false;
                            }
                        }
                        
                    }
                    else {
                        if (chunkOffset + 1 > maxChunkSize) {
                            processChunk();
                            System.out.println(totalNumBytesRead);
                            newChunk = true;
                        }
                        else {
                            int cur = input.read();
                            if (cur == -1) {
                                // eof reached
                                finishedChunking = true;
                                processChunk();
                                System.out.println(totalNumBytesRead);
                                // check if data directory exists, create if not
                                String currentDirectory = System.getProperty("user.dir");
                                File file = new File(currentDirectory + "/data");
                                if (!file.exists()) {
                                    file.mkdir();
                                }
                                if (containerOffset!=0)
                                {
                                    try (FileOutputStream fos = new FileOutputStream(currentDirectory + "/data/container" + containerCount)) {
                                        fos.write(container);
                                        containerCount++;
                                    }
                                    catch (FileNotFoundException e) {
                                        e.printStackTrace();
                                    }
                                    catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                                fileRecipes.put(uploadFile, chunkHashes);
                            }
                            else {
                                chunk[chunkOffset] = (byte)cur;
                                curRFP = ((d * (curRFP - (power(d, minChunkSize - 1) * chunk[chunkOffset - minChunkSize]) % avgChunkSize) % avgChunkSize) % avgChunkSize + chunk[chunkOffset]) % avgChunkSize;
                                chunkOffset++;
                                totalNumBytesRead++;
                                
                                if ((curRFP & mask) == 0) {
                                    processChunk();
                                    System.out.println(totalNumBytesRead);
                                    newChunk = true;   
                                }
                            }
                        }
                    }
                }
            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
        }
        else if (command.equals("download")) {
            System.out.print("download\n");
            downloadFile = args[1];
            localFile = args[2];
            storage = args[3];
            if (storage.equals("local")){
                getFileLocal(downloadFile, localFile);
            }
        }
        else if (command.equals("delete")) {
            deleteFile = args[1];
            storage = args[2];
            if (storage.equals("local")){
                deleteFileLocal(deleteFile);
            }
        }
        writeFingerprintIndex();
        writeFileRecipes();
    }
}
