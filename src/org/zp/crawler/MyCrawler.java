package org.zp.crawler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.zp.MergeDriver.MergeDriver;
import org.zp.ParserDriver.parserDriver;
import org.zp.crawlerDriver.crawlerDriver;
import org.zp.docHBase.HBaseUtil;
import org.zp.optimizerDriver.optimizerDriver;

public class MyCrawler {
	private static int COUNT=2;
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		String same = "hdfs://localhost:9000/user/zp/crawler/";
		//创建Document表
		HBaseUtil.createTable();
		crawlerConf.confCrawler(same, COUNT);
	}
}
