package org.zp.docHBase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {
	
	private static String tablename = "Document";
	private static String tableFamily = "URL";
	
	private static Configuration configuration  = null;
	static{
		configuration = HBaseConfiguration.create();
	}
	
	/*
	 * 创建存储网页的Document表
	 */
	public static void createTable() throws IOException{
		HBaseAdmin admin = new HBaseAdmin(configuration);
		if(admin.tableExists(tablename)){
	        System.out.println("table already exists!");     
		}else{
			HTableDescriptor descriptor = new HTableDescriptor(tablename);
			HColumnDescriptor columnDescriptor = new HColumnDescriptor(tableFamily);
			descriptor.addFamily(columnDescriptor);
			admin.createTable(descriptor);
	        System.out.println("create table " + tablename + " ok.");     
		}
		admin.close();
	}
	
	//向表中添加新URL
	public static void addURL(String url,String doc) throws IOException{
		HTable table = new HTable(configuration, tablename);
		Put put = new Put(Bytes.toBytes(url));
		put.add(Bytes.toBytes(tableFamily), Bytes.toBytes("doc"), Bytes.toBytes(doc));
		table.put(put);
		table.close();
	}
	
	/**   
	 * 显示所有数据   
	 */    
		public static void getAllRecord () {     
		    try{     
		         HTable table = new HTable(configuration, tablename);     
		         Scan s = new Scan();     
		         ResultScanner ss = table.getScanner(s);     
		         for(Result r:ss){     
		             for(KeyValue kv : r.raw()){     
		                System.out.print(new String(kv.getRow()) + " ");     
		                System.out.print(new String(kv.getFamily()) + ":");     
		                System.out.print(new String(kv.getQualifier()) + " ");     
		                System.out.print(kv.getTimestamp() + " ");     
		                System.out.println(new String(kv.getValue()));     
		             }     
		         }     
		    } catch (IOException e){     
		        e.printStackTrace();     
		    }     
		}    
}
