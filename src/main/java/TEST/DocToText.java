package TEST;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class DocToText {
   public static void main(String[] args) throws IOException, TikaException {
      Tika tika = new Tika();
      String content = tika.parseToString(new File("D:/test.doc"));
      FileWriter fileWriter = new FileWriter("D:/test.txt");
      fileWriter.write(content);
      fileWriter.flush();
      fileWriter.close();
      System.out.println();
   }
}
