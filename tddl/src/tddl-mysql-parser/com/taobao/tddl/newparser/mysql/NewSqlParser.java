package com.taobao.tddl.newparser.mysql;

import java.sql.SQLSyntaxErrorException;

import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLDeleteStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLInsertStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLReplaceStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLSelectStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLUpdateStatement;
import com.alibaba.cobar.parser.visitor.SQLASTVisitor;
import com.taobao.tddl.common.channel.SqlMetaData;
import com.taobao.tddl.common.channel.impl.SqlMetaDataImpl;
import com.taobao.tddl.newparser.mysql.parserresult.MySqlStandardParserResult;
import com.taobao.tddl.parser.SQLParser;
import com.taobao.tddl.sqlobjecttree.SqlParserResult;

public abstract class NewSqlParser implements SQLParser {
	
	public abstract SQLStatement parse2AST(final String sql,
			final boolean isMysql);
	
	/**
	 * 从SQLStatement转换成SqlParserResult
	 * @param statement
	 * @param sql
	 * @return
	 * @throws SQLSyntaxErrorException
	 */
	public static SqlParserResult visit(SQLStatement statement, String sql) {
		MySqlStandardParserResult result = new MySqlStandardParserResult();
		result.setAst(statement);
		SQLASTVisitor visitor;
		if (statement instanceof DMLSelectStatement) {
			visitor = new SelectVisitor(result);
			statement.accept(visitor);
		} else if (statement instanceof DMLUpdateStatement) {
			visitor = new UpdateVisitor(result);
			statement.accept(visitor);
		} else if (statement instanceof DMLDeleteStatement) {
			visitor = new DeleteVisitor(result);
			statement.accept(visitor);
		} else if (statement instanceof DMLInsertStatement) {
			visitor = new InsertVisitor(result);
			statement.accept(visitor);
		} else if (statement instanceof DMLReplaceStatement) {
			visitor = new ReplaceIntoVisitor(result);
			statement.accept(visitor);
		} else {
			throw new IllegalArgumentException("not support '" + sql + "'");
		}
		return result;
	}
	
	/**
	 * 填充parser result的sql format字段
	 * @param statement
	 * @param result
	 */
//	public static void commonVisit(SQLStatement statement, MySqlStandardParserResult result){
//		SqlFormatVisitor sqlFormatVisitor = new SqlFormatVisitor(result);
//		statement.accept(sqlFormatVisitor);
//	}
	
	/**
	 * 填充parser result的sql format字段
	 * @param statement
	 * @param result
	 */
	public static SqlMetaData commonVisit(SQLStatement statement, String sql){
		SqlMetaDataImpl sqlMetaData = new SqlMetaDataImpl();
		SqlFormatVisitor sqlFormatVisitor = new SqlFormatVisitor(sqlMetaData, sql);
		statement.accept(sqlFormatVisitor);
		sqlMetaData.setParsed(true);
		return sqlMetaData;
	}
	
	
	
	
	public static void throwSyntaxErrorException(String sql, Exception e){
		throw new RuntimeException("sql syntax error!sql is:" + sql, e);
	}
}
