package com.taobao.tddl.client.jdbc.resultset.newImp;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.client.jdbc.ConnectionManager;
import com.taobao.tddl.client.jdbc.TStatementImp;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.jdbc.sqlexecutor.RealSqlExecutor;
/**
 * @author guangxia
 * @author junyu
 *
 */
public abstract class BaseTResultSet extends PlainAbstractTResultSet {
	private static final Log log = LogFactory.getLog(BaseTResultSet.class);
	
	private int fetchSize;
	private int fetchDirection = FETCH_FORWARD;
	
	protected int limitTo = -1;
	protected int limitFrom = 0;
	
	public BaseTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,RealSqlExecutor realSqlExecutor)
			throws SQLException {
		super(tStatementImp, connectionManager, executionPlan,realSqlExecutor);
	}

	public BaseTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,RealSqlExecutor realSqlExecutor,
			boolean init) throws SQLException {
		super(tStatementImp, connectionManager, executionPlan,realSqlExecutor,init);
	}

	public boolean next() throws SQLException {
		checkClosed();

		if (actualResultSets.size() == 1) {
			return actualResultSets.get(0).next();
		}

		if (limitTo == 0) {
			return false;
		}

		return internNext();
	}

	protected abstract boolean internNext() throws SQLException;
	
	public int getLimitTo() {
		return limitTo;
	}

	public void setLimitTo(int limitTo) {
		this.limitTo = limitTo;
	}

	public void setLimitFrom(int limitFrom) {
		this.limitFrom = limitFrom;
	}

	public int getLimitFrom() {
		return limitFrom;
	}

	public static class CompareTypeUnsupported extends Exception {
		private static final long serialVersionUID = 1L;
		
		public CompareTypeUnsupported(String message){
			super(message);
		}
	}

	/**
	 * TODO: unused
	 */
	public int getFetchDirection() throws SQLException {
		if (log.isDebugEnabled()) {
			log.debug("invoke getFetchDirection");
		}

		checkClosed();

		return fetchDirection;
	}

	/**
	 * TODO: unused
	 */
	public void setFetchDirection(int direction) throws SQLException {
		if (log.isDebugEnabled()) {
			log.debug("invoke setFetchDirection");
		}

		checkClosed();

		if (direction != FETCH_FORWARD) {
			throw new SQLException("only support fetch direction FETCH_FORWARD");
		}

		this.fetchDirection = direction;
	}

	/**
	 * TODO: unused
	 */
	public int getFetchSize() throws SQLException {
		if (log.isDebugEnabled()) {
			log.debug("invoke getFetchSize");
		}

		checkClosed();

		return fetchSize;
	}

	/**
	 * TODO: unused
	 */
	public void setFetchSize(int rows) throws SQLException {
		if (log.isDebugEnabled()) {
			log.debug("invoke setFetchSize");
		}

		checkClosed();

		if (rows < 0) {
			throw new SQLException("fetch size must greater than or equal 0");
		}

		this.fetchSize = rows;
	}
}
