package org.zp.docHBase;

import java.io.IOException;

import org.junit.Test;

public class test {
	
	@Test
	public void testHbase() throws IOException{
		HBaseUtil.createTable();
		HBaseUtil.getAllRecord();
	}
}
