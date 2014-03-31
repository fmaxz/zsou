package org.zp.index.test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.zp.index.FsDirectory;

import com.chenlb.mmseg4j.analysis.ComplexAnalyzer;

public class testTika extends Configured implements Tool {

	public static class Map extends Mapper<Text, Text, Text, Text>{
		
		public void map(Text key,Text value,Context context) throws CorruptIndexException, LockObtainFailedException, IOException{
			String valueString = value.toString();
			InputStream stream  = new ByteArrayInputStream(valueString.getBytes());
			
			Metadata metadata = new Metadata();
			Tika tika = new Tika();
			try {
				valueString = tika.parseToString(stream, metadata);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			String title = metadata.get("title");
			if(title==null)
				title="";
			
			Path path = new Path("hdfs://localhost:9000/user/zp/crawler/indexTest/");
			Directory directory = new FsDirectory(path.getFileSystem(context.getConfiguration()), path, true, context.getConfiguration());
			IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(Version.LUCENE_35, new ComplexAnalyzer()));
			Document document = new Document();
			Field urlField = new Field("url",key.toString(),Field.Store.YES,Field.Index.NOT_ANALYZED_NO_NORMS);
			Field titleField = new Field("title",title,Field.Store.YES,Field.Index.ANALYZED);
			/*
			 * 1、加大对www.oschina.net和www.csdn.net的权值
			 * 2、减少url中含有&#等字符号的权值
			 */
			String url = key.toString();
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

			document.add(new Field("content",valueString,Field.Store.NO,Field.Index.ANALYZED));
			writer.addDocument(document);
			writer.close();
			
//			context.write(key, new Text(metadata.get("title")));
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();
		Job job = new Job(configuration,"testTika");
		
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		Path outpPath = new Path("hdfs://localhost:9000/user/zp/testTika");
		FileSystem fileSystem = outpPath.getFileSystem(configuration);
		if(fileSystem.exists(outpPath)){
			fileSystem.delete(outpPath,true);
		}
		
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/zp/crawler/doc/doc1"));
//		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/zp/crawler/doc_final"));
//		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/zp/crawler/doc/doc3"));

		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/zp/testTika"));
		
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args){
		int res=0;
		try {
			res = ToolRunner.run(new Configuration(), new testTika(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(res);
	}
}
