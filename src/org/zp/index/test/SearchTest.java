package org.zp.index.test;

import java.io.File;
import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class SearchTest {
	
	static IndexReader reader = null;
	static Directory directory = null;
	static{
		try {
			directory = FSDirectory.open(new File("/home/zp/indexTest"));
			reader = IndexReader.open(directory);
		} catch (CorruptIndexException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void search(){
		
	}
}
