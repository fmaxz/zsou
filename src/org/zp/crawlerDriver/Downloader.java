package org.zp.crawlerDriver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.util.HttpURLConnection;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.util.EntityUtils;

public class Downloader {

	//下载网页
	public static String  DownloadURL(String url) throws IOException{
		String docString = "";
//
//		URL pageURl = new URL(url);
//		HttpURLConnection uc = new HttpURLConnection(method, url)
//		uc.setRequestProperty("User-Agent", "Internet Explorer");
//		uc.setRequestProperty("Host",pageURl.getHost());
//		uc.setRequestProperty("Accept-Language", "zh-cn");
//		uc.setRequestProperty("Accept-Charset", "gb2312,utf-8;q=0.7,*;q=0.7n");
//		uc.connect();
//		BufferedReader reader = new BufferedReader(new InputStreamReader(uc.getInputStream()));
//		while(reader.readLine()!=null){
//			docString+=reader.readLine();
//		}
//		
		/*
		 * ------------------------
		 */
//
		try {
			CloseableHttpClient httpClient = HttpClients.createDefault();
			//设置超时连接
			HttpGet httpGet = new HttpGet(url.trim());
			RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(5000).setConnectTimeout(5000).build();
			httpGet.setConfig(requestConfig);
			
			CloseableHttpResponse response = httpClient.execute(httpGet);

			//判断访问的状态码
			int statusCode = response.getStatusLine().getStatusCode();
			if(statusCode!=HttpStatus.SC_OK){
				System.err.println("Method failed: "+response.getStatusLine());
			}
				
			HttpEntity entity = response.getEntity();
			
			//获得字符集，默认为gb2312
			String charset = "gb2312";
			if(entity.getContentType().toString().contains("charset")){
				int index = entity.getContentType().toString().indexOf("charset");
				charset = entity.getContentType().toString().substring(index+8);
			}
			System.out.println("已爬取："+url);
			docString = EntityUtils.toString(entity,charset);
		}catch(Exception e){
			e.printStackTrace();
			docString = null;
		}
		return docString;
	}
	
	public static void main(String[] args) throws IOException{
		//定义过滤器，提取以http://www.lietu.com开头的链接
//		LinkFilter filter = new LinkFilter(){
//
//			@Override
//			public boolean accecpt(String url) {
//				// TODO Auto-generated method stub
//				if(url.startsWith("http://"))
//					return true;
//				else
//					return false;
//			}
//		};
		String string=Downloader.DownloadURL("http://www.oschina.net/code/snippet_256373_34556");
		System.out.println(string);
//		Set<String> urlsSet = HtmlParser.extracLinks(string, filter);
//		for(String str:urlsSet){
//			System.out.println(str);
//		}

	}
}
