package org.zp.docHBase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
//将存在HDFS中的页面数据存到HBase中，方面读取数据
public class docHBase {
	
	public static class Map extends Mapper<Text, Text,NullWritable,NullWritable>{
		
		public void map(Text key,Text value,Context context) throws IOException, InterruptedException{
			HBaseUtil.addURL(key.toString(), value.toString());
			context.write(NullWritable.get(),NullWritable.get());
		}
	}
}
