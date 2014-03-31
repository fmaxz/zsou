package org.zp.ParserDriver;

import java.util.HashSet;
import java.util.Set;

import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.NodeClassFilter;
import org.htmlparser.filters.OrFilter;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;

public class HtmlParser {
	//获取一个网站上的链接，filter用来过虑链接
	public static Set<String> extracLinks(String url,LinkFilter filter){
		Set<String> links = new HashSet<String>();
		try{
			Parser parser = new Parser(url);
			parser.setEncoding("GB2312");
			//过滤<frame>标签的filter，用来提取frame标签的src属性
			NodeFilter frameFilter = new NodeFilter(){
			    private static final long serialVersionUID = 1L;  
			    
				public boolean accept(Node node){
					if(node.getText().startsWith("frame src=")){
						return true;
					}else{
						return false;
					}
				}
			};
			
			//OrFilter来设置过滤<a>标签和<frame>标签
			OrFilter linkFilter = new OrFilter(new NodeClassFilter(LinkTag.class),frameFilter);
			//得到所有经过过滤的标签
			NodeList list = parser.extractAllNodesThatMatch(linkFilter);
			for(int i=0;i<list.size();i++){
				Node tag = list.elementAt(i);
				if(tag instanceof LinkTag) //<a>标签
				{
					LinkTag link = (LinkTag)tag;
					String linkUrl = link.getLink(); //url
					if(filter.accecpt(linkUrl))
					{
						links.add(linkUrl);
					}
				}else
				{
					//提取frame里src属性的链接，如<frame src="XX.htm" />
					String frame = tag.getText();
					int start = frame.indexOf("src=");
					frame = frame.substring(start);
					int end = frame.indexOf(" ");
					if(end==-1)
						end = frame.indexOf(">");
					String frameUrl = frame.substring(5,end-1);
					if(filter.accecpt(frameUrl))
						links.add(frameUrl);
				}
			}
		}catch(ParserException e){
			e.printStackTrace();
			links=null;
		}
		
		return links;
	}
}
