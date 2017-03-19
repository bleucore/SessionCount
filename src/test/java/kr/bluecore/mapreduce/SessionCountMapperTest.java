package kr.bluecore.mapreduce;

import static org.mockito.Mockito.*;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.*;

public class SessionCountMapperTest {

		// TODO Auto-generated method stub
	@Test
	public void processesValidRecord() throws IOException, InterruptedException{
		SessionCountMapper mapper = new SessionCountMapper();
		Text value = 	new Text("1133850|SESSION_INT||||||||2017-03-15 06:50:55|1|170849");
		MapWritable m_writable = new MapWritable();
		m_writable.put(new Text("1133850"), new IntWritable(100));

		@SuppressWarnings("unchecked")
		Mapper<LongWritable, Text, Text, MapWritable>.Context context = mock(Mapper.Context.class);

		mapper.map(null, value, context);
		
		verify(context).write(new Text("1133850"), mapper.getM_writable());
	}
	
}
