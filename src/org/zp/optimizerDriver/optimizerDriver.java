package org.zp.optimizerDriver;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
/*
 * Map:将URl，doc颠倒顺序
 * Reduce:取第一个URL 作为key输出
 */
public class optimizerDriver {
	
	public static class Map extends Mapper<Text, Text, Text, Text>{
		
		public void map(Text key,Text value,Context context) throws IOException, InterruptedException{
			context.write(value, key);
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			//取第一个URL
			String urlString = values.iterator().next().toString();
			context.write(new Text(urlString), key);
		}
	}
}
