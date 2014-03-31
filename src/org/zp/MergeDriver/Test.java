package org.zp.MergeDriver;

//测试boolemfilter
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Test {

	private static SimpleBoolmFilter filter;
	public static void main(String[] args) throws IOException{
		Configuration configuration = new Configuration();
		
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
		
		if(filter.contains("http://www.oschina.nt")){
			System.out.println("include");
		}
		else {
			System.out.println("no_include");

		}
	}
}
