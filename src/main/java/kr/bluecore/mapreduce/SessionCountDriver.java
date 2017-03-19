package kr.bluecore.mapreduce;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SessionCountDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		if(args.length !=2){
			System.err.printf("Usage: %s [generic options] <input> <output>\n.",SessionCountDriver.class.getSimpleName());
			System.exit(1);
		}
		
	 // Create a new Job
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Session Count");
	 
	 // Specify various job-specific parameters     

	    job.setJarByClass(SessionCountDriver.class);
	    job.setMapperClass(SessionCountMapper.class);
	    job.setCombinerClass(SessionCountReducer.class);
	    job.setReducerClass(SessionCountReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
