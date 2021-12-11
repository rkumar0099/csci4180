import java.io.*;
import java.util.*;
import java.nio.file.Files;
import java.lang.Math;


public class MyDedup {
    private static PrintStream out = new PrintStream(System.out);
    private static byte[] container = new byte[1024*1024];
    private static byte[] chunk;
    private static int minSize, avgSize, maxSize;
    private static boolean finishChunking = false;
    private static int numChunks = 0;


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
            numChunks += 1;
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
                    numChunks += 1;
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
                    finishChunking = true;
                    // process the chunk in separate thread
                    break;
                }

                if (read == minSize) { 
                    int bytesRead = rfp(minSize, 257, avgSize, chunk, fs);
                    totalBytesRead += bytesRead;
                    out.println(totalBytesRead);
                    // process the chunk in separate thread
                }

                // to test first 20 points with ta program
                
                if (numChunks == 20) {
                    return;
                }
                
            }
        } catch(Exception e) {
            out.println("[Error] Can't open the upload file. ");
            e.printStackTrace();
        }
    }

    public static void downloadFile(String pathname) {

    }

    public static void deleteFile(String pathname) {

    }
    

    public static void main(String[] args) {
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

            switch(args[0]) {
                case "upload":
                    uploadFile(args[5]);
                    break;

                case "download":
                    downloadFile(args[5]);
                    break;

                case "delete":
                    deleteFile(args[5]);
                    break;
                
                default:
                    out.println("[Error] Choose upload, download, or delete.");
                    return;
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

}