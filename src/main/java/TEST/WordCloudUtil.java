package TEST;

import java.awt.Color;
import java.awt.Dimension;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.WordFrequency;
import com.kennycason.kumo.bg.RectangleBackground;
import com.kennycason.kumo.font.FontWeight;
import com.kennycason.kumo.font.KumoFont;
import com.kennycason.kumo.font.scale.LinearFontScalar;
import com.kennycason.kumo.image.AngleGenerator;
import com.kennycason.kumo.nlp.FrequencyAnalyzer;
import com.kennycason.kumo.palette.ColorPalette;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

import java.awt.*;
import java.io.OutputStream;

public class WordCloudUtil{
      public static void buildWordCouldByWordFrequencies(int width_img, int height_img, int angle_word, int maxFont,
                                                         int minFont, List<WordFrequency> wordFrequencies, Color backgroudColor, OutputStream outputStream,
                                                         Color... wordColors) {
         writeToStreamAsPNG(buildWordCouldByWordFrequencies(width_img, height_img, angle_word, maxFont, minFont,
               wordFrequencies, backgroudColor, wordColors), outputStream);
      }

      public static void buildWordCouldByWordFrequencies(int width_img, int height_img, int angle_word, int maxFont,
                                                         int minFont, List<WordFrequency> wordFrequencies, Color backgroudColor, String filePath,
                                                         Color... wordColors) {
         writeToFile(buildWordCouldByWordFrequencies(width_img, height_img, angle_word, maxFont, minFont,
               wordFrequencies, backgroudColor, wordColors), filePath);
      }

      public static void buildWordCouldByWords(int width_img, int height_img, int angle_word, int maxFont, int minFont,
                                               List<String> words, Color backgroudColor, OutputStream outputStream, Color... wordColors) {
         writeToStreamAsPNG(buildWordCouldByWordFrequencies(width_img, height_img, angle_word, maxFont, minFont,
               buildWordFrequencies(words), backgroudColor, wordColors), outputStream);
      }

      public static void buildWordCouldByWords(int width_img, int height_img, int angle_word, int maxFont, int minFont,
                                               List<String> words, Color backgroudColor, String filePath, Color... wordColors) {
         writeToFile(buildWordCouldByWordFrequencies(width_img, height_img, angle_word, maxFont, minFont,
               buildWordFrequencies(words), backgroudColor, wordColors), filePath);
      }

      private static WordCloud buildWordCouldByWordFrequencies(int width_img, int height_img, int angle_word, int maxFont,
                                                               int minFont, List<WordFrequency> wordFrequencies, Color backgroudColor, Color... wordColors) {
         Dimension dimension = new Dimension(width_img, height_img);
         WordCloud wordCloud = new WordCloud(dimension, CollisionMode.RECTANGLE);
         wordCloud.setPadding(0);
         wordCloud.setBackground(new RectangleBackground(dimension));
         wordCloud.setKumoFont(new KumoFont("", FontWeight.PLAIN));
         wordCloud.setColorPalette(new ColorPalette(wordColors));
         wordCloud.setFontScalar(new LinearFontScalar(minFont, maxFont));
         wordCloud.setBackgroundColor(backgroudColor);
         wordCloud.setAngleGenerator(new AngleGenerator(angle_word));
         wordCloud.build(wordFrequencies);
         return wordCloud;
      }

      private static List<WordFrequency> buildWordFrequencies(List<String> words) {
         FrequencyAnalyzer frequencyAnalyzer = new FrequencyAnalyzer();
         List<WordFrequency> wordFrequencies = frequencyAnalyzer.load(words);
         return wordFrequencies;
      }

      private static void writeToFile(WordCloud wordCloud, String filePath) {
         wordCloud.writeToFile(filePath);
      }

      private static void writeToStreamAsPNG(WordCloud wordCloud, OutputStream outputStream) {
         wordCloud.writeToStreamAsPNG(outputStream);
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
         }
      }
      Color color = new Color(255,255,255);
      Color[] colors = new Color[9];
      colors[0] = new Color(255,0,0);
      colors[1] = new Color(0,0,255);
      colors[2] = new Color(24,51,74);
      colors[3] = new Color(0,255,0);
      colors[4] = new Color(0,25,0);
      colors[5] = new Color(245,45,94);
      colors[6] = new Color(245,45,94);
      colors[7] = new Color(98,245,94);
      colors[8] = new Color(20,45,244);
      buildWordCouldByWords(400,400,10,32,12,words,color,"input/mm.png",colors);
   }
}
