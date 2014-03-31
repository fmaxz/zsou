package org.zp.ParserDriver;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 此程序从URl对应的网页内容中提取链接列表，并以<url,list>的形式保存
 * 所以只需要一个Map
 */
public class parserDriver extends Configured implements Tool{

	
	public static class Map extends Mapper<Text,Text,Text, Text>{
		//定义过滤器，提取以http://开头的链接
		LinkFilter filter = null;
		
		protected void setup(Context context){
			//定义过滤器，只提取oschina和csdn的网页开头的链接
			filter = new LinkFilter(){

				@Override
				public boolean accecpt(String url) {
					// TODO Auto-generated method stub
					if(url.contains("csdn.net")||url.contains("oschina.net"))
						return true;
					else
						return false;
				}
			};
		}
		
		public void map(Text key,Text value,Context context) {
			Set<String> urlsSet = HtmlParser.extracLinks(value.toString(), filter);
			String urlsString= "";
			if(urlsSet!=null&&urlsSet.size()!=0){
				for(String str:urlsSet){
					//去掉URL中的空格
					str = str.replaceAll("[\\s]+", "");
					
					//去掉url中的#后内容
					int index=0;
					if(str.contains("#")){
						index = str.indexOf("#");
						str = str.substring(0, index);
					}
					
					//去掉URl最后的'/',注意URL不能为空
					if(str.length()!=0&&str.charAt(str.length()-1)=='/')
						str = str.substring(0, str.length()-1);
					//url队列不为空
					if(urlsString.compareTo("")!=0)
						urlsString = urlsString + "," + str;
					else {
						urlsString += str;
					}
				}
			}
			try {
				context.write(key, new Text(urlsString));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	@Override
	public int run(String[] args){
		try {
			Configuration configuration = getConf();
			Job job = new Job(configuration, "parserDriver");
			job.setJarByClass(parserDriver.class);
			
			Path inPath = new Path(args[0]);
			Path outPath = new Path(args[1]);
			FileSystem fileSystem = outPath.getFileSystem(configuration);
			if(fileSystem.exists(outPath))
				fileSystem.delete(outPath, true);
			
			job.setMapperClass(Map.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, inPath);
			FileOutputFormat.setOutputPath(job, outPath);
			
			job.waitForCompletion(true);
			return 0;
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(),new parserDriver(), args);
		System.exit(res);
	}
}
