package com.taobao.tddl.client.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map.Entry;

import javax.sql.DataSource;

import com.taobao.tddl.client.pipeline.PipelineFactory;

/**
 * 允许多连接，有一个连接为主连接，其余连接为辅助连接 主连接允许进行事务，辅助连接则不允许进行事务处理，但允许读取 如果只有主连接，那么必须复用主连接
 * 
 * 主连接一旦建立就不会丢失，必须将当前连接彻底关闭，或者将autoCommit 从false->true，然后调用tryClose方法关闭主连接
 * 否则主连接不会消失。
 * 
 * @author shenxun
 * 
 */
public class AllowReadLevelTConnection extends TConnectionImp {
	public AllowReadLevelTConnection(String appName, boolean enableProfileRealDBAndTables,PipelineFactory pipelineFactory) {
		super(appName, enableProfileRealDBAndTables, pipelineFactory);
	}

	protected String transactionKey;

	@Override
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
				boolean needSetAutoCommit = validThrowSQLException(dbIndex,
						goSlave);
				conn = datasource.getConnection();
				if (needSetAutoCommit) {
					// 如果需要新建连接
					conn.setAutoCommit(isAutoCommit);
				}
//				else
				//这里不显示的设置为true了
//				{
//					conn.setAutoCommit(true);
//				}
				connectionMap.put(dbIndex, conn);
			}
			return conn;
		} else {
			if (!isAutoCommit) {
				// 这是用于判断这种情况，如果连接有多个，那么获取的连接如果不是事务连接
				// 并且又要进行写入或者select for update操作，抛出异常
				validThrowSQLException(dbIndex, goSlave);
			}
			// else{
			// //如果在事务中，包含事务的连接被关闭掉了。那么就会走到这个选择,实际场景中
			// 不会出现，因为只有tryClose方法，不允许事务连接关闭
			// }
			return conn;
		}
	}

	@Override
	protected List<SQLException> setAutoCommitTrue2False(boolean autoCommit,
			List<SQLException> sqlExceptions) throws SQLException {
		validTransactionCondition(false);
		boolean firstIn = true;
		for (Entry<String, Connection> entry : connectionMap
				.entrySet()) {
			if(isTransactionConnection(entry.getKey())){
				if(firstIn){
					
					firstIn = false;
					this.transactionKey = entry.getKey();
				}
				sqlExceptions = setAutoCommitAndPutSQLExceptionToList(autoCommit,
						sqlExceptions, entry);
			}
		}
		return sqlExceptions;
	}
	
	@Override
	protected List<SQLException> setAutoCommitFalse2True(boolean autoCommit,
			List<SQLException> sqlExceptions) {
		try {
			return super.setAutoCommitFalse2True(autoCommit, sqlExceptions);
		} finally{
			//先清空事务连接
			this.transactionKey = null;
		}
	}

	@Override
	protected boolean isTransactionConnection(String dbIndex) {
		if (dbIndex == null) {
			return true;
		}
		return dbIndex.equals(transactionKey);
	}

	@Override
	public void tryClose(String dbIndex) throws SQLException {
		Connection conn = connectionMap.get(dbIndex);
		if (conn == null) {
			// 如果当前dsGroup没有在map内，那么简单的返回
			// 碰到一个典型的场景是在setAutoCommit(false->true)的过程中，也要显示的关闭
			// 在异常状态中也要关闭，所以还是打log关闭吧。
			log.warn("should not be here ");
			return;
		}
		if (isAutoCommit && openedStatements.size() <= 1) {
			// 非事务状态中,并且打开的statement只有一个。
			try {
				//如果是非事务状态，并且transactionKey不为空，并且transactionKey和dbIndex key相同
				if(transactionKey != null&&transactionKey.equals(dbIndex)){
					log.warn("should not be here! transaction Key is not null !"+transactionKey);
					transactionKey = null;
				}
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

	protected boolean validThrowSQLException(String dbIndex, boolean isGoSlave)
			throws SQLException {
		if (transactionKey == null) {
			// 如果还没有指定过transactionkey,那么当前的新连接就作为默认的transaction连接存在
			if(!isGoSlave)
			{
				transactionKey = dbIndex;
				return true;
			}
			else
			{
				return false;
			}
		} else {
			if (!transactionKey.equals(dbIndex) && !isGoSlave) {
				// 如果当前key不是默认的事务key,并且是需要写入或走主库的（也就等于除了非事务性读以外其他数据库访问操作)
				throw new SQLException("不允许写入到多个不同的数据库节点中！");
			}
			return false;
		}
	}
}
