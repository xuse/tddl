//package com.taobao.tddl.client.jdbc.retry;
//
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import javax.sql.DataSource;
//
//import com.taobao.tddl.client.jdbc.ConnectionAndDatasource;
//import com.taobao.tddl.client.jdbc.DBSelector;
//import com.taobao.tddl.client.jdbc.sorter.ExceptionSorter;
//import com.taobao.tddl.client.jdbc.sorter.MySQLExceptionSorter;
//import com.taobao.tddl.client.jdbc.sorter.OracleExceptionSorter;
//import com.taobao.tddl.client.util.ExceptionUtils;
//
///**
// * import 因为内部状态会发生改变，因此必须保证这个上下文在方法内作为参数传递。
// * 
// * 重试时需要的上下文环境
// * @author shenxun
// *
// */
//public class RetringContext {
//	private List<SQLException> sqlExceptions;
//	private static final MySQLExceptionSorter MYSQL_EXCEPTION_SORTER = new MySQLExceptionSorter();
//	private static final OracleExceptionSorter ORACLE_EXCEPTION_SORTER = new OracleExceptionSorter();
//	private final ExceptionSorter exceptionSorter;
//
//	/**
//	 * 是否是一个走主库的读或写操作
//	 */
//	private boolean needRetry;
//	private int alreadyRetringTimes;
//
//	public RetringContext(boolean isMySQL) {
//		if (isMySQL)
//			this.exceptionSorter = MYSQL_EXCEPTION_SORTER;
//		else
//			this.exceptionSorter = ORACLE_EXCEPTION_SORTER;
//	}
//
//	public boolean isNeedRetry() {
//		return needRetry;
//	}
//
//	public void setNeedRetry(boolean needRetry) {
//		this.needRetry = needRetry;
//	}
//
//	public boolean isExceptionFatal(SQLException sqlException) {
//		return exceptionSorter.isExceptionFatal(sqlException);
//	}
//
//	public int getAlreadyRetringTimes() {
//		return alreadyRetringTimes;
//	}
//
//	public void setAlreadyRetringTimes(int alreadyRetringTimes) {
//		this.alreadyRetringTimes = alreadyRetringTimes;
//	}
//
//	public void addRetringTimes() {
//		this.alreadyRetringTimes++;
//	}
//
//	public void addSQLException(SQLException sqlException) {
//		if(sqlExceptions == null){
//			//not thread safe
//			sqlExceptions = new ArrayList<SQLException>(3);
//		}
//		sqlExceptions.add(sqlException);
//	}
//
//	public List<SQLException> getSqlExceptions() {
//		return Collections.unmodifiableList(sqlExceptions);
//	}
//
//}
