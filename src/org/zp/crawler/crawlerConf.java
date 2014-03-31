package org.zp.crawler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.zp.MergeDriver.MergeDriver;
import org.zp.ParserDriver.parserDriver;
import org.zp.crawlerDriver.crawlerDriver;
import org.zp.docHBase.docHBase;
import org.zp.optimizerDriver.optimizerDriver;


public class crawlerConf {
	
	public static void confCrawler(String same,int count) throws IOException, ClassNotFoundException, InterruptedException{
		/*
		//深度爬取count层
		for(int i=0;i<count;i++){
			//CrawlerDriver的设置
			Configuration jobCDconf = new Configuration();
			Job jobCD = new Job(jobCDconf,"crawlerDriver");
			jobCD.setJarByClass(crawlerDriver.class);
			
			jobCD.setMapperClass(org.zp.crawlerDriver.crawlerDriver.Map.class);
			jobCD.setReducerClass(org.zp.crawlerDriver.crawlerDriver.Reduce.class);
			
			jobCD.setMapOutputKeyClass(Text.class);
			jobCD.setMapOutputValueClass(LongWritable.class);			
			jobCD.setOutputKeyClass(Text.class);
			jobCD.setOutputValueClass(Text.class);
			
			//ParserDriver的设置
			Configuration jobPDconf = new Configuration();
			Job jobPD = new Job(jobPDconf, "ParserDriver");
			jobPD.setJarByClass(parserDriver.class);
			
			jobPD.setMapperClass(org.zp.ParserDriver.parserDriver.Map.class);
			
			jobPD.setInputFormatClass(KeyValueTextInputFormat.class);
			jobPD.setOutputFormatClass(TextOutputFormat.class);
			jobPD.setMapOutputKeyClass(Text.class);
			jobPD.setMapOutputValueClass(Text.class);
			
			//MergeDriver的设置
			Configuration jobMDconf = new Configuration();
			Job jobMD = new Job(jobMDconf,"MergeDriver");
			jobMD.setJarByClass(MergeDriver.class);
			
			jobMD.setMapperClass(org.zp.MergeDriver.MergeDriver.Map.class);
			jobMD.setReducerClass(org.zp.MergeDriver.MergeDriver.Reduce.class);
			
			jobMD.setInputFormatClass(KeyValueTextInputFormat.class);
			jobMD.setOutputFormatClass(TextOutputFormat.class);
			jobMD.setMapOutputKeyClass(Text.class);
			jobMD.setMapOutputValueClass(Text.class);
			
			//设置输入路径		
			FileInputFormat.addInputPath(jobCD, new Path(same+"url/url"+Integer.toString(i)));
			FileInputFormat.addInputPath(jobPD, new Path(same+"doc/doc"+Integer.toString(i+1)));
			FileInputFormat.addInputPath(jobMD, new Path(same+"links/link"+Integer.toString(i+1)));
			//设置输出路径
			FileOutputFormat.setOutputPath(jobCD, new Path(same+"doc/doc"+Integer.toString(i+1)));
			FileOutputFormat.setOutputPath(jobPD, new Path(same+"links/link"+Integer.toString(i+1)));
			FileOutputFormat.setOutputPath(jobMD, new Path(same+"url/url"+Integer.toString(i+1)) );
			
			//顺序等待完成
			jobCD.waitForCompletion(true);
			jobPD.waitForCompletion(true);
			jobMD.waitForCompletion(true);
			
		}
		
		//optimizerDriver的设置
		Configuration jobODconf = new Configuration();
		Job jobOD = new Job(jobODconf,"optimizerDriver");
		jobOD.setJarByClass(optimizerDriver.class);
		
		jobOD.setMapperClass(org.zp.optimizerDriver.optimizerDriver.Map.class);
		jobOD.setReducerClass(org.zp.optimizerDriver.optimizerDriver.Reduce.class);
		
		jobOD.setInputFormatClass(KeyValueTextInputFormat.class);
		jobOD.setOutputFormatClass(TextOutputFormat.class);
		jobOD.setMapOutputKeyClass(Text.class);
		jobOD.setMapOutputValueClass(Text.class);
		
		for(int j=0;j<count;j++){
			FileInputFormat.addInputPath(jobOD, new Path(same+"doc/doc"+Integer.toString(j+1)));
		}
		
		FileOutputFormat.setOutputPath(jobOD, new Path(same+"doc_final/"));
		jobOD.waitForCompletion(true);
		*/
		
		//docHBase的设置
		Configuration jobdocHBaseconf = new Configuration();
		Job jobdocHbase = new Job(jobdocHBaseconf, "docHbase");
		jobdocHbase.setJarByClass(docHBase.class);
		
		jobdocHbase.setMapperClass(docHBase.Map.class);
		jobdocHbase.setNumReduceTasks(0);
		
		jobdocHbase.setInputFormatClass(KeyValueTextInputFormat.class);
		jobdocHbase.setMapOutputKeyClass(NullWritable.class);
		jobdocHbase.setMapOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(jobdocHbase, new Path(same+"doc_final/"));
		Path temPath = new Path(same+"temp/");
		FileOutputFormat.setOutputPath(jobdocHbase, temPath);
		jobdocHbase.waitForCompletion(true);
		
		//删除tempPath对应目录
		FileSystem fileSystem = temPath.getFileSystem(jobdocHBaseconf);
		fileSystem.delete(temPath, true);
	}
}
