package org.zp.crawlerDriver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*
 * 此mapreduce用于爬取网页
 * Map：输入为<offline,url>,输出<url,offline>只是简单的颠倒顺序
 * Reduce:爬取URL，以<url,document>的形式保存
 */
public class crawlerDriver extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable,Text,Text,LongWritable>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			context.write(value,key);
		}
	}
	
	public static class Reduce extends Reducer<Text,LongWritable, Text,Text>{
		
		public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException{
			String text = Downloader.DownloadURL(key.toString());
			if(text!=null){			//判断爬取过程是否有异常发生
				//为了将网页内容保存成一行，去掉网页中的换行符
			    String string = text.replaceAll("[\\s]+", " ");
			    context.write(key, new Text(string));
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();
		Job job = new Job(configuration, "crawlerDriver");
		job.setJarByClass(crawlerDriver.class);
		
		Path inPath = new Path(args[0]);
		Path outPath = new Path(args[1]);
		FileSystem fileSystem = outPath.getFileSystem(configuration);
		if(fileSystem.exists(outPath))
			fileSystem.delete(outPath, true);
		
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(),new crawlerDriver(), args);
		System.exit(res);
	}
}
