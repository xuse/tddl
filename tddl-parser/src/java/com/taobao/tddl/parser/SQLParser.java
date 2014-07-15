package com.taobao.tddl.parser;

import com.taobao.tddl.sqlobjecttree.SqlParserResult;

/**
 * SQL½âÎöÆ÷»ùÀà
 * 
 * @author shenxun 
 *
 */
public interface SQLParser{
	SqlParserResult parse(String sql, boolean isMySQL);
}
