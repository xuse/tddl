package com.taobao.tddl.parser;

import com.taobao.tddl.sqlobjecttree.SqlParserResult;

/**
 * SQL解析器基类
 * 
 * @author shenxun 
 *
 */
public interface SQLParser{
	SqlParserResult parse(String sql, boolean isMySQL);
}
