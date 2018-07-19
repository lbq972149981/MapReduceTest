package BloomFilterDriver;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class BloomFilterDriver {
	public static void main(String[] args) throws Exception {
		Path inputFile = new Path(args[0]);
		int numMembers = Integer.parseInt(args[1]);
		float falsePosRate = Float.parseFloat(args[2]);
		Path bfFile = new Path(args[3]);
		int vectorSize = MRDPUtils.getOptimalBloomFilterSize(numMembers, falsePosRate);
		int nbHash = MRDPUtils.getOptimalK(numMembers, vectorSize);
		BloomFilter filter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);
		String line = null;
		int numElements = 0;
		FileSystem fs = FileSystem.get(new Configuration());
		for (FileStatus Status : fs.listStatus(inputFile)) {
			BufferedReader rdr = new BufferedReader(
					new InputStreamReader(new GZIPInputStream(fs.open(Status.getPath()))));
			System.out.println("reading" + Status.getPath());
			while ((line = rdr.readLine()) != null) {
				filter.add(new Key(line.getBytes()));
				++numElements;
			}
			rdr.close();
		}
		FSDataOutputStream strm = fs.create(bfFile);
		filter.write(strm);
		strm.flush();
		strm.close();
		System.exit(0);

	}

	public static class BloomFilteringMapper extends Mapper<Object, Text, Text, NullWritable> {
		private BloomFilter filter = new BloomFilter();

		@Override
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
			DataInputStream strm = new DataInputStream(new FileInputStream(files[0].getPath()));
			filter.readFields(strm);
			strm.close();
		}

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] e = value.toString().split("\\s+", 8);
			if (e == null) {
				return;
			}
			if (filter.membershipTest(new Key(e[4].getBytes()))) {	
				boolean flag = true;
				if (e.length > 4) {
					for (char c : e[4].toCharArray()) {
						if (!Character.isDigit(c)) {
							flag = false;
							break;
						}
					}
					if (flag) {
						context.write(value, NullWritable.get());
					}
				}
			}
		}
	}
}
