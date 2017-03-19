package kr.bluecore.mapreduce;

import static org.mockito.Mockito.*;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.*;

public class SessionCountReducerTest {
	
	@Test
	public void returnsSessionCountValue() throws IOException, InterruptedException{

		SessionCountReducer reducer = new SessionCountReducer();
		//"1133850|SESSION_INT||||||||2017-03-15 06:50:55|1|170849"	
		Text key = new Text("1133850");
		//Iterator<Text> values = Arrays.asList(new Text(), new Text()).iterator();
		MapWritable values = new MapWritable();
		values.put(new Text("1133850"), new IntWritable(100));
		@SuppressWarnings("unchecked")
		Reducer<Text, MapWritable, Text, IntWritable>.Context context = mock(Reducer.Context.class); 
		
		reducer.reduce(key, values, context);
		
		verify(context).write(key, (IntWritable)values.get(key));
	}

}
