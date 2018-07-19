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
import com.kennycason.kumo.bg.CircleBackground;
import com.kennycason.kumo.bg.RectangleBackground;
import com.kennycason.kumo.font.FontWeight;
import com.kennycason.kumo.font.KumoFont;
import com.kennycason.kumo.font.scale.LinearFontScalar;
import com.kennycason.kumo.font.scale.SqrtFontScalar;
import com.kennycason.kumo.image.AngleGenerator;
import com.kennycason.kumo.nlp.FrequencyAnalyzer;
import com.kennycason.kumo.nlp.tokenizers.ChineseWordTokenizer;
import com.kennycason.kumo.palette.ColorPalette;
import com.kennycason.kumo.palette.LinearGradientColorPalette;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

import java.awt.*;
import java.io.OutputStream;
import java.util.Random;

public class wordcloud{
   public static void main(String[] args) throws IOException, TikaException {
      //建立词频分析器，设置词频，以及词语最短长度，此处的参数配置视情况而定即可
      FrequencyAnalyzer frequencyAnalyzer = new FrequencyAnalyzer();
      frequencyAnalyzer.setWordFrequenciesToReturn(600);
      frequencyAnalyzer.setMinWordLength(2);
      //引入中文解析器
      frequencyAnalyzer.setWordTokenizer(new ChineseWordTokenizer());
      //指定文本文件路径，生成词频集合
      final List<WordFrequency> wordFrequencyList = frequencyAnalyzer.load("input/b.txt");
      //设置图片分辨率
      Dimension dimension = new Dimension(600,600);
      //此处的设置采用内置常量即可，生成词云对象
      WordCloud wordCloud = new WordCloud(dimension,CollisionMode.PIXEL_PERFECT);
      //设置边界及字体
      wordCloud.setPadding(2);
      java.awt.Font font = new java.awt.Font("STSong-Light", 2, 20);
      //设置词云显示的三种颜色，越靠前设置表示词频越高的词语的颜色
      Random random = new Random();
      int r = random.nextInt(256);
      int b = random.nextInt(256);
      int g = random.nextInt(256);
      int r1 = random.nextInt(256);
      int b1 = random.nextInt(256);
      int g1 = random.nextInt(256);
      int r2 = random.nextInt(256);
      int b2 = random.nextInt(256);
      int g2 = random.nextInt(256);
      wordCloud.setColorPalette(new LinearGradientColorPalette(Color.RED,new Color(r,b,g),new Color(r1,b1,g1), 10, 40));
      wordCloud.setKumoFont(new KumoFont(font));
      //设置背景色
      wordCloud.setBackgroundColor(new Color(0,0,0));
      //设置背景图片
      //wordCloud.setBackground(new PixelBoundryBackground("E:\\爬虫/google.jpg"));
      //设置背景图层为圆形
      wordCloud.setBackground(new CircleBackground(255));
      wordCloud.setFontScalar(new SqrtFontScalar(12, 45));
      //生成词云
      wordCloud.build(wordFrequencyList);
      wordCloud.writeToFile("input/wy.png");
   }
}
