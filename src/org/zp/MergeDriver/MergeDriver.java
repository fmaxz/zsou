package org.zp.MergeDriver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * Map：从<url,listUrl>中分割listURl，去掉重复URl
 * Reduce:判断URl是否遍历过，将没遍历的写入url/urlX文件中
 */

public class MergeDriver extends Configured implements Tool{

	public static class Map extends Mapper<Text,Text,Text, Text>{
		
		public void map(Text key,Text value,Context context) {
			if(value.toString()!=null&&value.toString().compareTo("")!=0){
				String split[] = value.toString().split(",");
				for(String str:split){
					try {
						context.write(new Text(str),new Text(""));
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<Text,Text, Text, NullWritable>{
		
		private static SimpleBoolmFilter filter;
		//在reduce之前运行
		protected void setup(Context context) throws IOException{
			Configuration configuration = context.getConfiguration();
			filter = new SimpleBoolmFilter();
			
			//获得所有已遍历过的URL，并添加到布隆过滤器中
			Path path = new Path("hdfs://localhost:9000/user/zp/crawler/links/");
			FileSystem hdfs = path.getFileSystem(configuration);
			FileStatus status[] = hdfs.listStatus(path);
			for(FileStatus fs:status){
				if(fs.isDir()){
					Path dirPath = new Path(fs.getPath().toString());
					FileStatus status2[] = hdfs.listStatus(dirPath);
					for(FileStatus fs2:status2){
						String fileName = fs2.getPath().toString();
						if(fileName.contains("part")){
							FSDataInputStream inputStream = hdfs.open(fs2.getPath());
							BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
							String lineString = null;
							while((lineString=reader.readLine())!=null){
								String split[] = lineString.split("[\\s]+");
								filter.add(split[0]);
							}
						}
					}
				}
			}
		}
		
		public void reduce(Text key,Iterable<Text> values,Context context){
			String url = key.toString();
			if(!filter.contains(url)&&url.indexOf("http")==0){
				try {
					context.write(key, NullWritable.get());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	@Override
	public int run(String[] args) {
		try {
			Configuration configuration = getConf();
			Job job = new Job(configuration, "MergeDriver");
			job.setJarByClass(MergeDriver.class);
			
			Path inPath = new Path(args[0]);
			Path outPath = new Path(args[1]);
			FileSystem fileSystem = outPath.getFileSystem(configuration);
			if(fileSystem.exists(outPath))
				fileSystem.delete(outPath, true);
			
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, inPath);
			FileOutputFormat.setOutputPath(job, outPath);
			
			job.waitForCompletion(true);
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
		int res = ToolRunner.run(new Configuration(),new MergeDriver(), args);
		System.exit(res);
	}
	
}
