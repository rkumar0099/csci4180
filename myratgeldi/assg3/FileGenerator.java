import java.io.File;
import java.util.*;
import java.io.*;
public class FileGenerator {
    public static void main(String[] args) {
        byte[] myArr = {2, 1, 4, 4, 3, 6, 4, 3, 2, 9};
        String currentDirectory = System.getProperty("user.dir");

        try (FileOutputStream fos = new FileOutputStream(currentDirectory + "/myFile.txt")) {
            fos.write(myArr);
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
