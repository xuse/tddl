package com.taobao.tddl.parser;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;

/**
 * 用于将小写的sql转换为大写
 * @author shenxun
 *
 */
public class AntlrStringStream extends ANTLRStringStream{
	public AntlrStringStream(String input) {
		super(input);
	}
	@Override
	public int LA(int i) {
		
		if ( i==0 ) { 
			return 0; // undefined 
			} 
			if ( i<0 ) { 
			i++; // e.g.， translate LA(-1) to use offset 0 
			} 
			if ( (p+i-1) >= n ) { 
			return CharStream.EOF; 
			} 
			return Character.toUpperCase(data[p+i-1]); 
			} 
}