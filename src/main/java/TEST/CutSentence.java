package TEST;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.CustomDictionary;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CutSentence {
   @Test
   public void test(){
      Random random = new Random();
      for(int i =0;i<10;i++) {
         System.out.println(random.nextInt(256));
      }
   }
   public static void main(String[] args) throws IOException, TikaException {
      Tika tika = new Tika();
      String read = tika.parseToString(new File("input/tt.docx"));
      Segment segment = HanLP.newSegment();
      FileWriter fileWriter = new FileWriter("input/b.txt");
      List<String> words = new ArrayList<>();
      for(Term term:segment.seg(read)){

         String s[] = term.toString().split("/");
         String key = s[0];
         String type = s[1];
         if(type.equals("n")) {
            words.add(key);
            fileWriter.write(key+"\n");
            fileWriter.flush();
         }
      }
      fileWriter.close();
   }
}
