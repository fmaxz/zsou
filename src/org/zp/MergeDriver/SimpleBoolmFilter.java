package org.zp.MergeDriver;

import java.util.BitSet;
/*
 * 通过布隆过滤器，过滤重复URL
 */
public class SimpleBoolmFilter {
	
	private static final int DEFAULT_SIZE = 2<<24;
	private static final int[] seeds = new int[]{7,11,13,37,61};
	private BitSet bits = new BitSet(DEFAULT_SIZE);
	private SimpleHash[] func = new SimpleHash[seeds.length];
	
	public SimpleBoolmFilter(){
		for(int i=0;i<seeds.length;i++){
			func[i] = new SimpleHash(DEFAULT_SIZE, seeds[i]);
		}
	}
	
	//添加URL
	public void add(String url){
		for(SimpleHash f:func){
			bits.set(f.hash(url),true);
		}
	}
	
	//是否包含URL
	public boolean contains(String url){
		if(url==null)
			return false;
		boolean ret = true;
		for(SimpleHash f:func){
			ret &= bits.get(f.hash(url));
		}
		return ret;
	}
	
	public static class SimpleHash{
		private int cap;
		private int seed;
		public SimpleHash(int cap,int seed){
			this.cap = cap;
			this.seed = seed;
		}
		//获取对应位值
		public int hash(String value){
			int result = 0;
			int len = value.length();
			for(int i=0;i<len;i++){
				result = seed*result +value.charAt(i);
			}
			return (cap-1)&result;
		}
	}
}
