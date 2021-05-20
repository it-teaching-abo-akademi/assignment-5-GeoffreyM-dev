import java.io.IOException;
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

public class CDNBilling {

	public static class TokenizerMapper extends Mapper<Object, Text, CompositeKey, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private CompositeKey newKey = new CompositeKey();
		
		private final int IP_INDEX = 0;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			String[] s = line.split(" ");
			
			String[] domainName = s[IP_INDEX].split("\\.");
			boolean isDomainName = false;
			for(String part : domainName) {
				try {
					Integer.parseInt(part);
				}catch(NumberFormatException e) {
					isDomainName = true;
				}
			}
			
			if(isDomainName) {
				newKey.setIp(s[IP_INDEX]);
	        	newKey.setCount(1);
	        	context.write(newKey, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<CompositeKey, IntWritable, Text, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(CompositeKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(new Text(key.getIp()), new Text(result.toString()));
		}
	}
	
	public static class SortingMapper extends Mapper<Object, Text, CompositeKey, IntWritable> {
		private CompositeKey newKey = new CompositeKey();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
	        StringTokenizer stringTokenizer = new StringTokenizer(line);
	        {
	            int number = 0; 
	            String ip = "null";

	            if(stringTokenizer.hasMoreTokens())
	            {
	                String str0= stringTokenizer.nextToken();
	                ip = str0.trim();
	            }

	            if(stringTokenizer.hasMoreElements())
	            {
	                String str1 = stringTokenizer.nextToken();
	                number = Integer.parseInt(str1.trim());
	            }
	            newKey.setCount(number);
	            newKey.setIp(ip);
	            context.write(newKey, new IntWritable(number));
	        }
		}
	}

	public static class SortingReducer extends Reducer<CompositeKey, IntWritable, Text, Text> {
		private IntWritable result = new IntWritable();
		private int nRecords = 0;
		
		public void reduce(CompositeKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			if(nRecords < 5) {
				int sum = 0;
				for (IntWritable val : values) {
					sum += val.get();
				}
				result.set(sum);
				context.write(new Text(key.getIp()), new Text(Integer.toString(key.getCount())));
			}
			nRecords++;
		}
	}

	public static void main(String[] args) throws Exception {
		
		/*JOB 1*/
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(CDNBilling.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(IntWritable.class);		
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		
		/* JOB 2*/
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "sorting");
		job2.setJarByClass(CDNBilling.class);
		job2.setMapperClass(SortingMapper.class);
		job2.setMapOutputKeyClass(CompositeKey.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setReducerClass(SortingReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}