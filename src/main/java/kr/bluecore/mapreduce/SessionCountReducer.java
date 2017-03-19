package kr.bluecore.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SessionCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	 private IntWritable result = new IntWritable();
	 
	//public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		/*int sessioncnt=0;
	  for (IntWritable val : values) {
		  		sessioncnt += val.get();
	        }*/
		//context.write(key, new IntWritable(sessioncnt));
		int sessioncnt=0;
		/*while(values.hasNext()){
			sessioncnt+=values.next().get();
		}*/
		for(IntWritable val:values){
			sessioncnt+=val.get();
		}
		result.set(sessioncnt);
		context.write(key,result);
	}
}