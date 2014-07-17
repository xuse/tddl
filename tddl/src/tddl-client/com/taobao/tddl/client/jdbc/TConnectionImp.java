package com.taobao.tddl.client.jdbc;

import static com.taobao.tddl.client.util.ExceptionUtils.appendToExceptionList;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.client.RouteCondition.ROUTE_TYPE;
import com.taobao.tddl.client.jdbc.TDataSource.TDSProperties;
import com.taobao.tddl.client.jdbc.listener.Context;
import com.taobao.tddl.client.jdbc.listener.HookPoints;
import com.taobao.tddl.client.jdbc.sqlexecutor.parallel.ParallelRealSqlExecutor;
import com.taobao.tddl.client.jdbc.sqlexecutor.serial.SerialRealSqlExecutor;
import com.taobao.tddl.client.jdbc.sqlexecutor.serial.SimpleSerialRealSqlExecutor;
import com.taobao.tddl.client.pipeline.PipelineFactory;
import com.taobao.tddl.client.pipeline.bootstrap.Bootstrap;
import com.taobao.tddl.client.pipeline.bootstrap.PipelineBootstrap;
import com.taobao.tddl.client.util.ExceptionUtils;
import com.taobao.tddl.common.client.ThreadLocalString;
import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.common.unitprotected.UnitDeployProtect;

public class TConnectionImp implements ConnectionManager, Connection {
	private TDSProperties properties;
	protected static final Log log = LogFactory.getLog(TConnectionImp.class);

	// TODO: 以后让这个值真正的起作用
	private int transactionIsolation = -1;

	private boolean closed;

	private final boolean enableProfileRealDBAndTables;

	private final PipelineFactory pipelineFactory;

	protected boolean isAutoCommit = true;

	protected Set<TStatementImp> openedStatements = new HashSet<TStatementImp>(
			2);

	private HookPoints hookPoints;
	
	private String appName;

	private final Context context = new Context();

	private final static boolean closeInvokedByTStatement = false;

	public TConnectionImp(String appName, boolean enableProfileRealDBAndTables,
			PipelineFactory pipelineFactory) {
		this.appName = appName;
		this.enableProfileRealDBAndTables = enableProfileRealDBAndTables;
		this.pipelineFactory = pipelineFactory;
	}

	public TConnectionImp(String username, String password,
			String appName, boolean enableProfileRealDBAndTables,
			PipelineFactory pipelineFactory) {
		this(appName, enableProfileRealDBAndTables, pipelineFactory);
	}

	protected void checkClosed() throws SQLException {
		if (closed) {
			throw new SQLException(
					"No operations allowed after connection closed.");
		}
	}

	public boolean isClosed() throws SQLException {
		return closed;
	}

	/**
	 * 单库事务限制
	 */
	private final static int maxTransactionDSCount = 1;

	protected Map<String, DataSource> dsMap = Collections.emptyMap();

	Map<String, Connection> connectionMap = new HashMap<String, Connection>(2);

	/**
	 * 尝试获取一个连接 :
	 * 
	 * 
	 * 事务中:
	 * 
	 * 如果goMaster == true ，数据源超过一个， 会抛错误。
	 * 
	 * 如果在非事务。 不会抛错误。
	 * 
	 * 在每次尝试从新获取连接的时候，都必须显示的将连接设置RetryableDatasourceGrooup.autocommit() 为指定的值。
	 * 
	 * 存疑的地方是，如果在事务中，查询其他库，是否应该允许他查询呢？
	 * 
	 * @param dbIndex
	 * @param goSlave
	 *            TODO: 兼容支付宝的实现。他们要求在事务中，可以通过select查询其他数据源。
	 * @return
	 */
	public Connection getConnection(String dbIndex, boolean goSlave)
			throws SQLException {

		Connection conn = connectionMap.get(dbIndex);
		if (conn == null) {
			DataSource datasource = dsMap.get(dbIndex);
			if (datasource == null) {
				throwMissGroupException(dbIndex);
			}
			// 当前dbIndex没有被其他对象使用，初始化dsGroupImp,
			if (isAutoCommit) {
				conn = datasource.getConnection();
				conn.setAutoCommit(isAutoCommit);
				connectionMap.put(dbIndex, conn);
			} else {
				// 事务状态中
				validTransactionCondition(true);
				conn = datasource.getConnection();
				conn.setAutoCommit(isAutoCommit);
				connectionMap.put(dbIndex, conn);
			}
		} else {
			// 表示当前dbIndex已经被其他对象使用。那么将当前引用添加到连接引用中。
			// 这里没有显示的设置autoCommit状态，原因是能够修改autoCommit状态的地方只有两个
			// 第一个是新建的那一下，还有一个就是setAutoCommit的那一下。这里只需要保持状态。
			return conn;
		}

		return conn;
	}

	/**
	 * 验证在事务状态中，是否可以创建数据源
	 * 
	 * @param requiredObject
	 * @param goSlave
	 * @param dbSelector
	 * @throws SQLException
	 */
	protected void validTransactionCondition(boolean createNew)
			throws SQLException {
		// 如果事务数据源超过限制,在新建连接的时候，要抛出要新建的那个连接，而在setAutoCommit的时候，不能去掉
		if (connectionMap.size() > (maxTransactionDSCount - (createNew ? 1 : 0)/*
																				 * 最大允许事务datasource数
																				 * -
																				 * 当前要加入的ds数
																				 * 。
																				 */)) {
			//have a nice log
			StringBuilder sb=new StringBuilder("事务中跨库个数超过预期，预期值: ");
			sb.append(maxTransactionDSCount);
			sb.append(",current dbIndexes in connectionMap is:");
			for(Map.Entry<String, Connection> entry:connectionMap.entrySet()){
				sb.append(entry.getKey());
				sb.append(";");
			}

			throw new SQLException(sb.toString());
			// TODO: 这里暂时不允许在事务中，使用非事务数据源进行查询。否则太复杂了。
		}
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		TStatementImp stmt = (TStatementImp) createStatement();
		stmt.setResultSetType(resultSetType);
		stmt.setResultSetConcurrency(resultSetConcurrency);
		return stmt;
	}

	public Statement createStatement() throws SQLException {
		checkClosed();
		Bootstrap bootstrap = new PipelineBootstrap(this, pipelineFactory);
		TStatementImp stmt = new TStatementImp(this, bootstrap);

		SerialRealSqlExecutor serialRealSqlExecutor = new SerialRealSqlExecutor(
				this);
		ParallelRealSqlExecutor parallelRealSqlExecutor = new ParallelRealSqlExecutor(
				this);
		SerialRealSqlExecutor simpleSerialRealSqlExecutor = new SimpleSerialRealSqlExecutor(
				this);

		stmt.setTimeoutThreshold(properties.timeoutThreshold);
		stmt.setHookPoints(hookPoints);
		stmt.setContext(context);
		stmt.setEnableProfileRealDBAndTables(enableProfileRealDBAndTables);
		stmt.setProperties(properties);
		stmt.setSerialRealSqlExecutor(serialRealSqlExecutor);
		stmt.setParallelRealSqlExecutor(parallelRealSqlExecutor);
		stmt.setSimpleSerialRealSqlExecutor(simpleSerialRealSqlExecutor);

		openedStatements.add(stmt);
		return stmt;
	}

	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		TStatementImp stmt = (TStatementImp) createStatement(resultSetType,
				resultSetConcurrency);

		stmt.setResultSetHoldability(resultSetHoldability);
		return stmt;
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		checkClosed();
		Bootstrap bootstrap = new PipelineBootstrap(this, pipelineFactory);
		TPreparedStatementImp stmt = new TPreparedStatementImp(this, sql,
				bootstrap);

		SerialRealSqlExecutor serialRealSqlExecutor = new SerialRealSqlExecutor(
				this);
		ParallelRealSqlExecutor parallelRealSqlExecutor = new ParallelRealSqlExecutor(
				this);
		SerialRealSqlExecutor simpleSerialRealSqlExecutor = new SimpleSerialRealSqlExecutor(
				this);

		stmt.setTimeoutThreshold(properties.timeoutThreshold);
		stmt.setHookPoints(hookPoints);
		stmt.setContext(context);
		stmt.setEnableProfileRealDBAndTables(enableProfileRealDBAndTables);
		stmt.setProperties(properties);
		stmt.setSerialRealSqlExecutor(serialRealSqlExecutor);
		stmt.setParallelRealSqlExecutor(parallelRealSqlExecutor);
		stmt.setSimpleSerialRealSqlExecutor(simpleSerialRealSqlExecutor);

		openedStatements.add(stmt);
		return stmt;
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		TPreparedStatementImp stmt = (TPreparedStatementImp) prepareStatement(sql);
		stmt.setResultSetType(resultSetType);
		stmt.setResultSetConcurrency(resultSetConcurrency);
		return stmt;
	}

	public boolean containDBIndex(String dbIndex) {
		return dsMap.containsKey(dbIndex);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		TPreparedStatementImp stmt = (TPreparedStatementImp) prepareStatement(
				sql, resultSetType, resultSetConcurrency);
		stmt.setResultSetHoldability(resultSetHoldability);
		return stmt;
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		TPreparedStatementImp stmt = (TPreparedStatementImp) prepareStatement(sql);
		stmt.setAutoGeneratedKeys(autoGeneratedKeys);
		return stmt;
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		TPreparedStatementImp stmt = (TPreparedStatementImp) prepareStatement(sql);
		stmt.setColumnIndexes(columnIndexes);
		return stmt;
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		TPreparedStatementImp stmt = (TPreparedStatementImp) prepareStatement(sql);
		stmt.setColumnNames(columnNames);
		return stmt;
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new UnsupportedOperationException("prepareCall");
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		throw new UnsupportedOperationException("prepareCall");
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new UnsupportedOperationException("prepareCall");
	}

	public void commit() throws SQLException {
		log.debug("invoke commit");

		checkClosed();

		if (isAutoCommit) {
			return;
		}

		hookPoints.getBeforeExecute().execute(context);

		// txStart = true;

		List<SQLException> exceptions = null;
		// 这里会遍历整个数组
		for (Entry<String, Connection> conn : connectionMap.entrySet()) {
			try {
				// 如果在setAutoCommit中发生了异常，则不需要重建连接，将那些存在的连接内的connection
				// commit即可
				if (isTransactionConnection(conn.getKey())) {
					conn.getValue().commit();
				}
			} catch (SQLException e) {
				if (exceptions == null) {
					exceptions = new LinkedList<SQLException>();
				}
				exceptions.add(e);
			}
		}

		ExceptionUtils.throwSQLException(exceptions, null, (List<Object>) null);

		hookPoints.getAfterExecute().execute(context);
		context.reset();
	}

	public void rollback() throws SQLException {
		if (log.isDebugEnabled()) {
			log.debug("invoke rollback");
		}

		checkClosed();

		if (isAutoCommit) {
			return;
		}

		List<SQLException> exceptions = null;

		for (Map.Entry<String, Connection> entry : connectionMap.entrySet()) {
			try {
				Connection conn = entry.getValue();
				if (isTransactionConnection(entry.getKey())) {
					conn.rollback();
				}
			} catch (SQLException e) {
				if (exceptions == null) {
					exceptions = new ArrayList<SQLException>();
				}
				exceptions.add(e);

				log.error(
						new StringBuilder("data source name: ").append(
								entry.getKey()).toString(), e);
			}
		}

		context.reset();

		ExceptionUtils.throwSQLException(exceptions, null, (List<Object>) null);
	}

	/**
	 * @param dbIndex
	 * @throws SQLException
	 */
	public void tryClose(String dbIndex) throws SQLException {
		Connection conn = connectionMap.get(dbIndex);
		if (conn == null) {
			// 如果当前dsGroup没有在map内，那么简单的返回
			// 碰到一个典型的场景是在setAutoCommit(false->true)的过程中，也要显示的关闭
			// 在异常状态中也要关闭，所以还是打log关闭吧。
			// log.warn("should not be here ");
			return;
		}
		
		if (isAutoCommit && openedStatements.size() <= 1) {
			// 非事务状态中,并且打开的statement只有一个。
			try {
				// 仅有当前引用的前提下，表示外部已经没有再持有当前引用了。关闭连接。
				conn.close();
			} finally {
				// 移除当前数据源
				connectionMap.remove(dbIndex);
			}
			// todo:这里还有个可以优化的地方就是如果openedStatements.size
			// >1的时候，遍历整个statements，如果所有statement.isResultSetClosed都为true，则可以关闭连接
		}
	}

	/**
	 * 设置当前事务状态。
	 * 
	 * 如果事务中的Manager(autoCommit=false) ,被调用了Connection.setAutoCommit(true) 方法，那么
	 * 这时候只有可能是maxTransactionDSCount个 如果当前的所有打开的statement内有未关闭的结果集，则所有连接必须保持打开。
	 * 如果没有statement,或者有statement没有resultSet,或者有resultSet而resultSet关闭。都可以关闭当前连接
	 * 
	 * 当从autoCommit=true 变为autoCommit = false时，会先检查当前Manager中持有了几个Connection.
	 * 
	 * 目前定义中，应判断在这个时候Connection的个数，如果超过一个 抛出特定的TransactionException
	 * 
	 * 然后将当前connection设置为autoCommit = false;
	 * 
	 * 
	 * 如果在DatasourceGroup.setAutoCommit的时候发生了异常，则应该关闭连接（datasourceGroup会自动重试，
	 * 抛出异常则 表示重试失败。
	 * 
	 * setAutoCommit可能在两个场景下被使用，第一个是connection刚刚获取的时候，这时候只是设置
	 * 标志位，不会抛出异常。第二种则是在运行中设置自动提交。这种情况下，会抛出异常
	 * 
	 * 应该将当前标志位变为关闭。并关闭内部所有连接。
	 * 
	 * @param isAutoCommit
	 */
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		checkClosed();
		// 先排除两种最常见的状态,true==true 和false == false
		if (this.isAutoCommit == autoCommit) {
			// 什么也不做
			return;
		}
		List<SQLException> sqlExceptions = null;
		if (autoCommit) {
			this.isAutoCommit = autoCommit;
			// this.autoCommit false -> true
			// 遍历整个结果集，如果引用计数不为1，则表示还在使用，这种会被显示的设置
			// autoCommit为true.从而丢失所有现有更新。
			// 将autoCommit false-> true
			sqlExceptions = setAutoCommitFalse2True(autoCommit, sqlExceptions);
		} else {

			// 这里根据holdability的要求，要显示的关闭所有的resultSet
			Iterator<TStatementImp> iterator = openedStatements.iterator();
			while (iterator.hasNext()) {
				TStatementImp it = iterator.next();
				if (!it.isCurrentRSClosedOrNull()) {
					try {
						it.getResultSet().close();
					} catch (SQLException e) {
						sqlExceptions = appendToExceptionList(sqlExceptions, e);
					}
				}
			}
			this.isAutoCommit = autoCommit;
			// this.autoCommit == true |autoCommit == false;
			// this.autoCommit true ->false
			sqlExceptions = setAutoCommitTrue2False(autoCommit, sqlExceptions);
		}
		// 抛出异常。
		if (sqlExceptions != null && !sqlExceptions.isEmpty()) {
			throw ExceptionUtils.mergeException(sqlExceptions);
		}
	}

	/**
	 * 
	 * @param autoCommit
	 * @param sqlExceptions
	 * @return
	 * @throws SQLException
	 *             当跨库事务数超过预期时。但已经确保内部除了这个异常外没有其他异常。
	 */
	protected List<SQLException> setAutoCommitTrue2False(boolean autoCommit,
			List<SQLException> sqlExceptions) throws SQLException {

		validTransactionCondition(false);

		for (Entry<String, Connection> entry : connectionMap.entrySet()) {
			if (isTransactionConnection(entry.getKey())) {
				sqlExceptions = setAutoCommitAndPutSQLExceptionToList(
						autoCommit, sqlExceptions, entry);
			}
		}
		return sqlExceptions;
	}

	/**
	 * 这时候只有可能是maxTransactionDSCount个
	 * 如果当前的事物中的statement为0个。表示事务结束，没有一个Tstatement还在使用连接.那么这时候会将链接关闭。
	 * 如果当前事务中的statement个数不为0个
	 * ，则表示事务结束，还有Tstatement可能在使用连接，这时候只是将链接的状态变为autoCommit
	 * 
	 * @param autoCommit
	 * @param sqlExceptions
	 * @param clearRetryableDSGroup
	 * @return
	 */
	protected List<SQLException> setAutoCommitFalse2True(boolean autoCommit,
			List<SQLException> sqlExceptions) {
		boolean closeAndclearRetryableDSGroup = true;
		// 判断是否可以归还当前连接，综合来说，只要连接内
		// resultSet未关闭，就不能归还连接，如果statement内的resultSet为空，或者resultSet已经全部关闭，则可以关闭当前连接。
		Iterator<TStatementImp> iterator = openedStatements.iterator();
		while (iterator.hasNext()) {
			TStatementImp it = iterator.next();
			if (!it.isCurrentRSClosedOrNull()) {
				// 只要有一个statment or preparedStatement没有关闭，就不能关闭当前连接。
				closeAndclearRetryableDSGroup = false;
			}
		}

		for (Entry<String, Connection> entry : connectionMap.entrySet()) {
			// 如果已经没有打开的statement,则认为当前connection可以关闭了
			try {
				if (isTransactionConnection(entry.getKey())) {
					// 为了保持语义的一致性，所以这里先显示的变false->true
					sqlExceptions = setAutoCommitAndPutSQLExceptionToList(
							autoCommit, sqlExceptions, entry);
				}
				if (closeAndclearRetryableDSGroup) {
					Connection conn = entry.getValue();
					if (conn.getAutoCommit() != true) {
						log.info("trying to close a not auto commit connection ,connection map is "
								+ connectionMap);
					}
					conn.close();
				}
			} catch (SQLException e) {
				sqlExceptions = appendToExceptionList(sqlExceptions, e);
			}

		}
		if (closeAndclearRetryableDSGroup) {
			connectionMap.clear();
		}
		return sqlExceptions;
	}

	protected boolean isTransactionConnection(String dbIndex) {
		return true;
	}

	protected List<SQLException> setAutoCommitAndPutSQLExceptionToList(
			boolean autoCommit, List<SQLException> sqlExceptions,
			Entry<String, Connection> entry) {
		try {
			entry.getValue().setAutoCommit(autoCommit);
		} catch (SQLException e) {
			sqlExceptions = appendToExceptionList(sqlExceptions, e);
		}
		return sqlExceptions;
	}

	public boolean getAutoCommit() throws SQLException {
		checkClosed();
		return isAutoCommit;
	}

	/**
	 * 最终清空缓存，无论是否在TStatement的时候清空了hint.
	 */
	public static void flush_hint() {
		flushOne(ThreadLocalString.ROUTE_CONDITION);
		flushOne(ThreadLocalString.DB_SELECTOR);
		flushOne(ThreadLocalString.RULE_SELECTOR);
	}

	private static void flushOne(String key) {
		RouteCondition rc = (RouteCondition) ThreadLocalMap.get(key);
		if (rc != null) {
			if (ROUTE_TYPE.FLUSH_ON_CLOSECONNECTION.equals(rc.getRouteType())) {
				ThreadLocalMap.put(key, null);
			}
		}
	}

	/**
	 * 关闭的顺序是 先设置标志位 ; 然后关闭持有的孩子 然后消除对持有孩子的引用 最后关闭自己持有的资源 最后消除自己对持有资源的引用
	 * 
	 * 必须要注意的是关闭自己持有的资源的时候，所有资源都会抛出异常，这时不能终止关闭流程。
	 * 
	 */
	@SuppressWarnings("unchecked")
	public void close() throws SQLException {
		if (log.isDebugEnabled()) {
			log.debug("invoke close");
		}

		if (closed) {
			return;
		}

		closed = true;

		List<SQLException> exceptions = null;
		try {
			// 关闭statement
			for (TStatementImp stmt : openedStatements) {
				try {
					stmt.closeInterval(closeInvokedByTStatement);
				} catch (SQLException e) {
					exceptions = appendToExceptionList(exceptions, e);
				}
			}
			// 关闭connection
			for (Connection conn : connectionMap.values()) {
				try {
					conn.close();
				} catch (SQLException e) {
					exceptions = appendToExceptionList(exceptions, e);
				}
			}
		} finally {
			flush_hint();
			UnitDeployProtect.clearUnitValidThreadLocal();
			openedStatements.clear();
			openedStatements = null;
			connectionMap.clear();
			connectionMap = null;
			// sqlExecuteEvents.clear();
		}
		ExceptionUtils.throwSQLException(exceptions, "close tconnection",
				Collections.EMPTY_LIST);
	}

	public int getTransactionIsolation() throws SQLException {
		checkClosed();

		return transactionIsolation;
	}
	
	public String getAppName() {
		return appName;
	}

	public void setTransactionIsolation(int transactionIsolation)
			throws SQLException {
		checkClosed();

		this.transactionIsolation = transactionIsolation;
	}

	public Connection getProxyConnection() {
		return this;
	}

	public void removeCurrentStatement(Statement statement) {
		if (!openedStatements.remove(statement)) {
			log.warn("current statmenet ：" + statement + " doesn't exist!");
		}
	}

	/*---------------------后面是未实现的方法------------------------------*/

	public void rollback(Savepoint savepoint) throws SQLException {
		throw new UnsupportedOperationException("rollback");
	}

	public Savepoint setSavepoint() throws SQLException {
		throw new UnsupportedOperationException("setSavepoint");
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		throw new UnsupportedOperationException("setSavepoint");
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new UnsupportedOperationException("releaseSavepoint");
	}

	public String getCatalog() throws SQLException {
		throw new UnsupportedOperationException("getCatalog");
	}

	public void setCatalog(String catalog) throws SQLException {
		throw new UnsupportedOperationException("setCatalog");
	}

	public int getHoldability() throws SQLException {
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	public void setHoldability(int holdability) throws SQLException {
		/*
		 * 如果你看到这里，那么恭喜，哈哈 mysql默认在5.x的jdbc driver里面也没有实现holdability 。
		 * 所以默认都是.CLOSE_CURSORS_AT_COMMIT 为了简化起见，我们也就只实现close这种
		 */
		throw new UnsupportedOperationException("setHoldability");
	}

	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	public void clearWarnings() throws SQLException {
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new UnsupportedOperationException("getTypeMap");
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new UnsupportedOperationException("setTypeMap");
	}

	public String nativeSQL(String sql) throws SQLException {
		throw new UnsupportedOperationException("nativeSQL");
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		checkClosed();
		return new TDatabaseMetaData();
	}

	public Map<String, DataSource> getDsMap() {
		return dsMap;
	}

	public void setDsMap(Map<String, DataSource> dsMap) {
		this.dsMap = dsMap;
	}

	/**
	 * 保持可读可写
	 */
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	/**
	 * 不做任何事情
	 */
	public void setReadOnly(boolean readOnly) throws SQLException {
		// do nothing
	}

	public void setHookPoints(HookPoints hookPoints) {
		this.hookPoints = hookPoints;
	}

	public HookPoints getHookPoints() {
		return hookPoints;
	}

	public TDSProperties getProperties() {
		return properties;
	}

	public void setProperties(TDSProperties properties) {
		this.properties = properties;
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return this.getClass().isAssignableFrom(iface);
	}

	@SuppressWarnings("unchecked")
	public <T> T unwrap(Class<T> iface) throws SQLException {
		try {
			return (T) this;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
	
	protected void throwMissGroupException(String groupKey) throws SQLException{
		throw new SQLException(
				"can't find datasource by your dbIndex : " + groupKey + " , appName is : " + getAppName());
	}

	public Clob createClob() throws SQLException {
		throw new SQLException("not support exception");
	}

	public Blob createBlob() throws SQLException {
		throw new SQLException("not support exception");
	}

	public NClob createNClob() throws SQLException {
		throw new SQLException("not support exception");
	}

	public SQLXML createSQLXML() throws SQLException {
		throw new SQLException("not support exception");
	}

	public boolean isValid(int timeout) throws SQLException {
		throw new SQLException("not support exception");
	}

	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		throw new RuntimeException("not support exception");
	}

	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		throw new RuntimeException("not support exception");
	}

	public String getClientInfo(String name) throws SQLException {
		throw new SQLException("not support exception");
	}

	public Properties getClientInfo() throws SQLException {
		throw new SQLException("not support exception");
	}

	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		throw new SQLException("not support exception");
	}

	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		throw new SQLException("not support exception");
	}

}
