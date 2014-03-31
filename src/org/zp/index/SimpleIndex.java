package org.zp.index;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.zp.index.test.testTika;
import org.zp.index.test.testTika.Map;

import com.chenlb.mmseg4j.analysis.ComplexAnalyzer;

/*
 * 建立索引并保存到HDFS上，只需一个Map
 */
public class SimpleIndex {
	
	public static class Map extends Mapper<Text, Text, Text, Text>{
		
		public void map(Text key,Text value,Context context){
			String url = key.toString();
			String doc = value.toString();
			String content = null;
			String title = null;
			
			//将页面转化为inputStream,供Tika解析
			InputStream inputStream = new ByteArrayInputStream(doc.getBytes());
			Tika tika = new Tika();
			
			Metadata metadata = new Metadata();
			try {
				content = tika.parseToString(inputStream, metadata);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TikaException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			title = metadata.get("title");
			
			if(title==null) title="";
			
			//将索引存入HDFS
			try {
				Path path = new Path("hdfs://localhost:9000/user/zp/crawler/index/");
				Directory directory = new FsDirectory(path.getFileSystem(context.getConfiguration()), path, false, context.getConfiguration());
				IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_35, new ComplexAnalyzer()));
				Document document = new Document();
				
				Field urlField = new Field("url",url,Field.Store.YES,Field.Index.NOT_ANALYZED_NO_NORMS);
				Field titleField = new Field("title",title,Field.Store.YES,Field.Index.ANALYZED);
				Field contentField = new Field("content",content,Field.Store.NO,Field.Index.ANALYZED);
				/*
				 * 1、加大对www.oschina.net和www.csdn.net的权值
				 * 2、对URL中含'/'个书设权值
				 */
				if(url.split("/",-1).length==3){
					document.setBoost(500.0F);
					titleField.setBoost(500.0F);
				}
				if(url.split("/",-1).length==4){
					document.setBoost(100.0F);
					titleField.setBoost(100.0F);
				}
				if(url.split("/",-1).length==5){
					document.setBoost(5.0F);
					titleField.setBoost(5.0F);
				}
				
				if(url.compareTo("http://www.oschina.net")==0||url.compareTo("http://www.csdn.net")==0){
					document.setBoost(1000.0F);
					titleField.setBoost(1000.0F);
				}
				
				document.add(urlField);
				document.add(titleField);
				document.add(contentField);
				
				writer.addDocument(document);
				writer.close();
				
			} catch (CorruptIndexException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (LockObtainFailedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	public static int run(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = new Job(configuration,"SimpleIndex");
		
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
				
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/zp/crawler/doc_final/"));

		//建立临时目录
		Path tempPath = new Path("hdfs://localhost:9000/user/zp/crawler/temp/");
		FileOutputFormat.setOutputPath(job, tempPath);
		
		job.waitForCompletion(true);
		//删除临时目录
		FileSystem fileSystem = tempPath.getFileSystem(configuration);
		fileSystem.delete(tempPath, true);
		return 0;
	}

	public static void main(String[] args) throws Exception{
		run(args);
	}
}
