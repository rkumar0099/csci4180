import java.util.*;
import java.lang.Math;
import java.lang.System;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MyDedup {
    private static int minChunkSize, avgChunkSize, maxChunkSize, d;
    private static String storage;
    private static HashMap<String, int[]> fingerprintIndex = new HashMap<String, int[]>();
    private static final String FILENAME = "mydedup.index";
    private static int containerOffset = 0;
    private static int chunkOffset = 0;
    private static byte[] container = new byte[1048576];
    private static byte[] chunk;
    private static MessageDigest md;
    private static int containerCount = 0;
    private static int totalNumBytesRead = 0;


    public static boolean checkPowerTwo(int n)
    {
        return n != 0 && ((n & (n - 1)) == 0);
    }
    
    public static int getAnchorMask(int avgChunkSize) {
        int count = 0;

        while(avgChunkSize != 0)
        {
            avgChunkSize >>= 1;
            count += 1;
        }
        // since avgChunkSize is power of two
        count -= 1;
        return (1 << count) - 1;
    }


    public static void loadFingerprintIndex() {
        // don't forget to load container count
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
        // check if chunk already exists in fingerprint index
        if (!fingerprintIndex.containsKey(hash)) {
            // check for container overflow
            if (containerOffset + chunkOffset > container.length) {
                // upload current container
                container = new byte[1048576];
                containerOffset = 0;
                containerCount++;
            }
            // move chunk into container
            // System.out.println("container offset: " + containerOffset);
            // System.out.println("chunk offset: " + chunkOffset);
            System.arraycopy(chunk, 0, container, containerOffset, chunkOffset);
            containerOffset += chunkOffset;
            // update fingerprint index
            int[] chunkInformation = {containerCount, containerOffset, chunkOffset};
            fingerprintIndex.put(hash, chunkInformation);
        }
        chunk = new byte[maxChunkSize];
        chunkOffset = 0;
    }

    public static void main(String args[]) throws NoSuchAlgorithmException {
        // int minChunkSize, avgChunkSize, maxChunkSize, d;
        String uploadFile, downloadFile, deleteFile, localFile;
        String command = args[0];
        loadFingerprintIndex();
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
                int mask = getAnchorMask(avgChunkSize);
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
                            System.out.println(totalNumBytesRead);
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
            downloadFile = args[1];
            localFile = args[2];
            storage = args[3];
        }
        else if (command.equals("delete")) {
            deleteFile = args[1];
            storage = args[2];
        }
    }
}