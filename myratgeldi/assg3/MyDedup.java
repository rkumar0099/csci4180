import java.util.*;
import java.lang.Math;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class MyDedup {

    
    static boolean checkPowerTwo(int n)
    {
        return n != 0 && ((n & (n - 1)) == 0);
    }
    
    static int getK(int avgChunkSize) {
        int count = 0;
 
        while(avgChunkSize != 0)
        {
            avgChunkSize >>= 1;
            count += 1;
        }
        // since avgChunkSize is power of two
        return count - 1;
    }


    public static void main(String args[]) {
        int a = Integer.parseInt(args[0]);
        int k = getK(a);
        System.out.println("my pow: " + k);
        // System.out.println("pow of two: " + checkPowerTwo(a));
        System.out.println((1 << k) - 1);
    }

    public static void main2(String args[]) {
        int minChunkSize, avgChunkSize, maxChunkSize, d;
        String uploadFile, downloadFile, deleteFile, localFile;
        String storage;

        String command = args[0];
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
            byte[] container = new byte[1024];
            try {
                InputStream input = new FileInputStream(uploadFile);
                int k = getK(avgChunkSize);
                // chunking
                int index = 0;
                int off = 0;
                byte[] chunk = new byte[maxChunkSize];
                // first read from input
                int fread = input.read(chunk, off, minChunkSize);
                int mask = (1 << k) - 1;
                if (fread == -1) {
                    // empty file
                }
                else {
                    if (fread < minChunkSize) {
                        // file size smaller than minChunkSize
                        off = off + fread;
                    }
                    else {
                        off = off + minChunkSize;
                        int curRFP = 0;
                        for (int i = 0; i < minChunkSize; i++) {
                            curRFP = (curRFP + (chunk[i] % avgChunkSize * (int) Math.pow(d, minChunkSize - i - 1) % avgChunkSize) % avgChunkSize) % avgChunkSize;
                        }
                        if (curRFP & mask == 0) {
                            
                        }
                        else {

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