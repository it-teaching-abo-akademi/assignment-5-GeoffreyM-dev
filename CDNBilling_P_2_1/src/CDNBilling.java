import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CDNBilling {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, LogEntry> {
		private LogEntry logEntry = new LogEntry();
		private Text newKey = new Text();
		
		private final int IP_INDEX = 0;
		private final int STATUS_INDEX = 8;
		private final int LENGTH_INDEX = 9;
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			String[] s = line.split(" ");
			
	        //IP Address
        	newKey = new Text(s[IP_INDEX]);
        	
        	//Status
        	try {
        		logEntry.setStatus(Integer.parseInt(s[STATUS_INDEX]));
        	} catch (NumberFormatException | IndexOutOfBoundsException e) {
        		logEntry.setStatus(0);
        	}
        	
        	//Length       	
        	try {
        		logEntry.setLength(Long.parseLong(s[LENGTH_INDEX]));
        	}catch(NumberFormatException | IndexOutOfBoundsException e) {
        		logEntry.setLength(0);
        	}
        	
	        context.write(newKey, logEntry);

		}
	}
	
	public static class CDNReducer extends Reducer<Text, LogEntry, Text, Text> { 
		
		public void reduce(Text key, Iterable<LogEntry> values, Context context) throws IOException, InterruptedException {
			
			long totalLength = 0;
			int servedRequest = 0;
			double totalGB = 0;
			double cost = 0;
			
			for (LogEntry val : values) {
				if(val.getStatus() == 200) {
					totalLength += val.getLength();
					servedRequest++;
				}
			}
			
			totalGB = ((( (double)totalLength/1024)/1024)/1024);
			
			cost = servedRequest*0.001;
			cost += totalGB*0.08;
			
			context.write(key, new Text("    total number of requests : " + servedRequest + "    total volume of transferred data : " + totalGB + " GB" + "    CDN costs : " + cost));
		}
	}
	
	public static void main(String[] args) throws Exception { 
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CDN Billing");
		job.setJarByClass(CDNBilling.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LogEntry.class);		
		job.setReducerClass(CDNReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
