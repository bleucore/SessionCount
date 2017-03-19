package kr.bluecore.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SessionCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text userkey = new Text();
	private final static IntWritable one = new IntWritable(1);

	private MapWritable m_writable = new MapWritable();
	public MapWritable getM_writable() {
		return m_writable;
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String line = value.toString();//"1133850|SESSION_INT||||||||2017-03-15 06:50:55|1|170849"		
		String delims = "[|]";
		String[] tokens = line.split(delims);
	
		
		if(tokens.length>9){

			String uid = tokens[2].toString();
			String type = tokens[8].toString();
			userkey.set(uid);
			
			//for(String str:tokens){
			System.out.print(":::::tokens[2] = "+tokens[2].toString()+":::::\n");
			System.out.print(":::::tokens[8] = "+tokens[8].toString()+":::::\n");
		//}
			//for(String str:tokens){
			System.out.print(":::::uid="+uid+":::::\n");
			System.out.print(":::::type="+type+":::::\n");
		//}
			
			if(type.equals("Login succeeded.")){
					context.write(userkey,one);
					//if(m_writable.get(uid) == null){ 
					//	m_writable.put(new Text(uid), one);
					System.out.print(":::::"+tokens[8].toString()+":::::\n");
					System.out.print(":::::type is Login succeeded.:::::\n");
				}
				else{
					//one.set(0);
					//context.write(userkey,one);
					//System.out.print(":::::type is not LOGIN_ATTEMPT:::::\n");
				}
			}
			/*for(int i=0;i<m_writable.size();i++){
					context.write(new Text(uid), (IntWritable)m_writable.get(uid));
				} */
			//}

/*		
		if(tokens.length>2){
			String uid = tokens[0].toString();
			String type = tokens[8].toString();

			if(type.equals("SESSION_INT")){
				if(m_writable.get(uid) == null){ 
					m_writable.put(new Text(uid), new IntWritable(1));
				}
				else{
					IntWritable cnt_iw = (IntWritable)m_writable.get(uid);
					cnt_iw.set(cnt_iw.get()+1);
					m_writable.put(new Text(uid), cnt_iw);
				}
			}
			context.write(new Text(uid), m_writable);
		}		*/	
	}
}
