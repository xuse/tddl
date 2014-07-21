package com.taobao.tddl.newparser.mysql;

import java.sql.SQLSyntaxErrorException;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.recognizer.SQLParserDelegate;
import com.taobao.tddl.common.util.GoogleConcurrentLruCache;
import com.taobao.tddl.sqlobjecttree.SqlParserResult;

public class StatementParser extends NewSqlParser {
	// private static final Map<String, FutureTask<SQLStatement>> cache = new
	// BoundedConcurrentHashMap<String, FutureTask<SQLStatement>>(
	// 389);
	private static final GoogleConcurrentLruCache<String, FutureTask<SQLStatement>> cache = new GoogleConcurrentLruCache<String, FutureTask<SQLStatement>>(
			389);

	private boolean useCache = true;

	public StatementParser() {
	}

	public StatementParser(boolean useCache) {
		this.useCache = useCache;
	}
	
	public SqlParserResult parse(String sql, boolean isMySQL) {
		SQLStatement statement = this.parse2AST(sql, isMySQL);
		return visit(statement, sql);
	}

	public SQLStatement parse2AST(final String sql,
			final boolean isMysql) {
		if (sql == null) {
			throw new IllegalArgumentException("sql must not be null");
		}
		SQLStatement statement = null;
		if (useCache) {
			FutureTask<SQLStatement> future = cache.get(sql);
			if (future == null) {
				Callable<SQLStatement> handle = new Callable<SQLStatement>() {
					public SQLStatement call() {
						SQLStatement statement = null;
						try {
							statement = SQLParserDelegate
									.parse(sql);
							return statement;
						} catch (SQLSyntaxErrorException e) {
							throwSyntaxErrorException(sql, e);
						}
						return statement;
					}
				};
				future = new FutureTask<SQLStatement>(handle);
				cache.put(sql, future);
				future.run();
			}
			// 确保抛出异常
			try {
				statement = future.get();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			try {
				statement = SQLParserDelegate.parse(sql);
			} catch (SQLSyntaxErrorException e) {
				throwSyntaxErrorException(sql, e);
			}
		}
		return statement;
	}

	public int getCacheSize() {
		return cache.size();
	}
}
