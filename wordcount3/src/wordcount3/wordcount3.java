package wordcount3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class wordcount3 {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

		static enum CountersEnum { INPUT_WORDS }

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private Set<String> patterns = new HashSet<String>();

		private Configuration conf;
		private BufferedReader fis;

		@Override
		public void setup(Context context) throws IOException,
		InterruptedException {
			conf = context.getConfiguration();
			conf.getBoolean("wordcount.case.sensitive", true);
			if (conf.getBoolean("wordcount.patterns", true)) {
				URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();

				for (URI patternsURI : patternsURIs) {
					Path patternsPath = new Path(patternsURI.getPath());
					String patternsFileName = patternsPath.getName().toString();
					parsePatternFile(patternsFileName);
				}
			}
		}

		private void parsePatternFile(String fileName) {
			try {
				fis = new BufferedReader(new FileReader(fileName));
				String patternLine = null;
				while ((patternLine = fis.readLine()) != null) {
					StringTokenizer itr = new StringTokenizer(patternLine.toString());
					while (itr.hasMoreTokens()) {
						patterns.add(itr.nextToken());
					}
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while parsing the cached file '"
						+ StringUtils.stringifyException(ioe));
			}
		}

		@Override
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String text = value.toString();
			for (String pattern : patterns)
			{
				StringTokenizer itr = new StringTokenizer(text);
				while (itr.hasMoreTokens())
				{
					word.set(itr.nextToken());
					if (word.toString().equals(pattern))
						context.write(word, one);
				}
			}
		}
	}

	public static class IntSumReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
			System.err.println("Usage: wordcount <in> <out> [-p PatternFile]");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(wordcount3.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		List<String> otherArgs = new ArrayList<String>();
		for (int i=0; i < remainingArgs.length; ++i) {
			if ("-p".equals(remainingArgs[i])) {
				job.addCacheFile(new Path(remainingArgs[++i]).toUri());
				job.getConfiguration().setBoolean("wordcount.patterns", true);
			} else {
				otherArgs.add(remainingArgs[i]);
			}
		}
		FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}