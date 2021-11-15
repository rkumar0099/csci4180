import java.io.*;
import java.util.*;

public class Main {

    public static void main(String[] args) {
        try {
        File f1 = new File("./f1.txt");
        File f2 = new File("./f2.txt");
        Scanner readerF1 = new Scanner(f1);
        Scanner readerF2 = new Scanner(f2);
        while(readerF1.hasNextLine() || readerF2.hasNextLine()) {
            if ((readerF1.hasNextLine() && !readerF2.hasNextLine()) || (!readerF1.hasNextLine() && readerF2.hasNextLine())) {
                System.out.println("Outputs not match");
                System.exit(0);
            } else {
                String l1 = readerF1.nextLine();
                String l2 = readerF2.nextLine();
                //System.out.println(l1);
                //System.out.println(l2);
                if (l1.equals(l2)) {
                    continue;
                }
                System.out.println("Outputs not match");
                System.exit(0);
            }
        }
        System.out.println("Outputs are same");
    } catch(Exception e) {
        e.printStackTrace();
    }
}

}