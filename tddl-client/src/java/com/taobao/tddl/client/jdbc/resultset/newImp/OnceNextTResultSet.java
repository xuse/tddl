package com.taobao.tddl.client.jdbc.resultset.newImp;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import com.taobao.tddl.client.jdbc.ConnectionManager;
import com.taobao.tddl.client.jdbc.TStatementImp;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.jdbc.sqlexecutor.RealSqlExecutor;

/**
 * @author junyu
 *
 */
public abstract class OnceNextTResultSet extends PlainAbstractTResultSet {
	private int cursor;
	
	protected Object value;
	protected boolean isNull;
	
	/**
	 * 初始方法中直接查询数据并且合并数据，然后关闭连接
	 * @param connectionManager
	 * @throws SQLException 
	 */
	public OnceNextTResultSet(TStatementImp tStatementImp, ConnectionManager connectionManager,
			ExecutionPlan executionPlan,RealSqlExecutor realSqlExecutor) throws SQLException {
		  super(tStatementImp, connectionManager, executionPlan,realSqlExecutor);
	}
	
	/**
	 * 只是给测试使用
	 * @param tStatementImp
	 * @param connectionManager
	 * @param executionPlan
	 * @param testResultSet
	 * @throws SQLException
	 */
	public OnceNextTResultSet(TStatementImp tStatementImp, ConnectionManager connectionManager,
			ExecutionPlan executionPlan,RealSqlExecutor realSqlExecutor,List<ResultSet> testResultSet,Set<Statement> testStatement) throws SQLException {
		  super(tStatementImp, connectionManager, executionPlan,realSqlExecutor,false);
		  super.actualResultSets=testResultSet;
		  super.actualStatements=testStatement;
	}
	
	protected abstract ResultSet reducer() throws SQLException;
	
	@Override
	public boolean next() throws SQLException {
		checkClosed();
		if (cursor > 0) {
			return false;
		}
		reducer();
		cursor++;
		return true;
	}
	
	protected void checkCursor() throws SQLException {
		if (cursor != 1) {
			throw new SQLException("cursor should not be "+cursor);
		}
	}
	
	protected void checkIndex(int columnIndex) throws SQLException {
		checkClosed();
		checkCursor();
		if (columnIndex < 1) {
			throw new SQLException("Column Index out of range, " + columnIndex
					+ " < 1");
		} else if (columnIndex > 1) {
			throw new SQLException("Column Index out of range, " + columnIndex
					+ " > 1");
		}
	}
	
	public int findColumn(String columnName) throws SQLException {
		checkCursor();
//		if (!columnName.equals(getMetaData().getColumnName(1))) {
//			throw new SQLException("Column '" + columnName + "' not found");
//		}
		
		if(String.CASE_INSENSITIVE_ORDER.compare(columnName, getMetaData().getColumnName(1))!=0){
			throw new SQLException("Column '" + columnName + "' not found");
		}
		
		return 1;
	}
	
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return actualResultSets.get(0).getMetaData();
	}
	
	@Override
	protected void checkRSIsClosedOrNull() throws SQLException{
		throw new UnsupportedOperationException("current resultset does not support current Operation!");
	}
	
	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		return (BigDecimal) value;
	}
	
	@Override
	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		return getBigDecimal(findColumn(columnName));
	}
	
	@Override
	public byte getByte(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		if (value == null) {
			return 0;
		} else if (value instanceof BigDecimal) {
			return ((BigDecimal) value).byteValueExact();
		} else {
			return Byte.parseByte(String.valueOf(value));
		}
	}

	@Override
	public byte getByte(String columnName) throws SQLException {
		return getByte(findColumn(columnName));
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		if (value == null) {
			return 0;
		} else if (value instanceof BigDecimal) {
			return ((BigDecimal) value).doubleValue();
		} else {
			return Double.parseDouble(String.valueOf(value));
		}
	}

	@Override
	public double getDouble(String columnName) throws SQLException {
		return getDouble(findColumn(columnName));
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		if (value == null) {
			return 0;
		} else if (value instanceof BigDecimal) {
			return ((BigDecimal) value).floatValue();
		} else {
			return Float.parseFloat(String.valueOf(value));
		}
	}

	@Override
	public float getFloat(String columnName) throws SQLException {
		return getFloat(findColumn(columnName));
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		if (value == null) {
			return 0;
		} else if (value instanceof BigDecimal) {
			return ((BigDecimal) value).intValueExact();
		} else {
			return Integer.parseInt(String.valueOf(value));
		}
	}

	@Override
	public int getInt(String columnName) throws SQLException {
		return getInt(findColumn(columnName));
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		if (value == null) {
			return 0;
		} else if (value instanceof BigDecimal) {
			return ((BigDecimal) value).longValueExact();
		} else {
			return Long.parseLong(String.valueOf(value));
		}
	}

	@Override
	public long getLong(String columnName) throws SQLException {
		return getLong(findColumn(columnName));
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		return value;
	}

	@Override
	public Object getObject(String columnName) throws SQLException {
		return getObject(findColumn(columnName));
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		checkIndex(columnIndex);
		if (value == null) {
			return 0;
		} else if (value instanceof BigDecimal) {
			return ((BigDecimal) value).shortValueExact();
		} else {
			return Short.parseShort(String.valueOf(value));
		}
	}

	@Override
	public short getShort(String columnName) throws SQLException {
		return getShort(findColumn(columnName));
	}

	@Override
	public boolean wasNull() throws SQLException {
		return this.isNull;
	}
}
