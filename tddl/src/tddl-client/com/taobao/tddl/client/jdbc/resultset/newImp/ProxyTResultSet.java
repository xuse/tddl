package com.taobao.tddl.client.jdbc.resultset.newImp;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import com.taobao.tddl.client.jdbc.ConnectionManager;
import com.taobao.tddl.client.jdbc.TStatementImp;

/**
 * @author junyu
 *
 */
public class ProxyTResultSet extends DummyTResultSet {

	protected ResultSet currentResultSet;

	/**
	 * @param connectionManager
	 */
	public ProxyTResultSet(ConnectionManager connectionManager) {
		super(connectionManager);
	}

	protected void checkRSIsClosedOrNull() throws SQLException{
		if(closed){
			throw new SQLException("No operations allowed after result set closed.");
		}

		if(currentResultSet == null){
			throw new SQLException("currentresultset is null or getXXX() is not surported!");
		}
	}

	protected void setResultSetProperty(TStatementImp tStatementImp) throws SQLException{
		setResultSetType(tStatementImp.getResultSetType());
		setResultSetConcurrency(tStatementImp.getResultSetConcurrency());
		setResultSetHoldability(tStatementImp.getResultSetHoldability());
		setFetchSize(tStatementImp.getFetchSize());
		setMaxRows(tStatementImp.getMaxRows());
		setQueryTimeout(tStatementImp.getQueryTimeout());
		setTimeoutThreshold(tStatementImp.getTimeoutThreshold());
		setEnableProfileRealDBAndTables(tStatementImp.isEnableProfileRealDBAndTables());
	}

	////////////////////////////////////////////////////////

	public boolean absolute(int row) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.absolute(row);
	}

	public void afterLast() throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.afterLast();
	}

	public void beforeFirst() throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.beforeFirst();
	}

	public void cancelRowUpdates() throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.cancelRowUpdates();
	}

	public void clearWarnings() throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.clearWarnings();
	}

	public void deleteRow() throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.deleteRow();
	}

	public int findColumn(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.findColumn(columnName);
	}

	public boolean first() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.first();
	}

	public Array getArray(int i) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getArray(i);
	}

	public Array getArray(String colName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getArray(colName);
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getAsciiStream(columnIndex);
	}

	public InputStream getAsciiStream(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getAsciiStream(columnName);
	}

	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getBigDecimal(columnIndex);
	}

	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getBigDecimal(columnName);
	}

	@SuppressWarnings("deprecation")
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getBigDecimal(columnIndex,scale);
	}

	@SuppressWarnings("deprecation")
	public BigDecimal getBigDecimal(String columnName, int scale)
			throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getBigDecimal(columnName,scale);
	}

	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getBinaryStream(columnIndex);
	}

	public InputStream getBinaryStream(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getBinaryStream(columnName);
	}

	public Blob getBlob(int i) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getBlob(i);
	}

	public Blob getBlob(String colName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getBlob(colName);
	}

	public boolean getBoolean(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getBoolean(columnIndex);
	}

	public boolean getBoolean(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getBoolean(columnName);
	}

	public byte getByte(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getByte(columnIndex);
	}

	public byte getByte(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getByte(columnName);
	}

	public byte[] getBytes(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getBytes(columnIndex);
	}

	public byte[] getBytes(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getBytes(columnName);
	}

	public Reader getCharacterStream(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getCharacterStream(columnIndex);
	}

	public Reader getCharacterStream(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getCharacterStream(columnName);
	}

	public Clob getClob(int i) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getClob(i);
	}

	public Clob getClob(String colName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getClob(colName);
	}

	public int getConcurrency() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getConcurrency();
	}

	public String getCursorName() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getCursorName();
	}

	public Date getDate(int columnIndex) throws SQLException {

		checkRSIsClosedOrNull();
		return currentResultSet.getDate(columnIndex);
	}

	public Date getDate(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getDate(columnName);
	}

	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getDate(columnIndex, cal);
	}

	public Date getDate(String columnName, Calendar cal) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getDate(columnName, cal);
	}

	public double getDouble(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getDouble(columnIndex);
	}

	public double getDouble(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getDouble(columnName);
	}

	public int getFetchDirection() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getFetchDirection();
	}

	public float getFloat(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getFloat(columnIndex);
	}

	public float getFloat(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getFloat(columnName);
	}

	public int getInt(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getInt(columnIndex);
	}

	public int getInt(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getInt(columnName);
	}

	public long getLong(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getLong(columnIndex);
	}

	public long getLong(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getLong(columnName);
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getMetaData();
	}

	public Object getObject(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getObject(columnIndex);
	}

	public Object getObject(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getObject(columnName);
	}

	public Object getObject(int i, Map<String, Class<?>> map)
			throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getObject(i,map);
	}

	public Object getObject(String colName, Map<String, Class<?>> map)
			throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getObject(colName, map);
	}

	public Ref getRef(int i) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getRef(i);
	}

	public Ref getRef(String colName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getRef(colName);
	}

	public int getRow() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getRow();
	}

	public short getShort(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getShort(columnIndex);
	}

	public short getShort(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getShort(columnName);
	}

	//FIXME:
	public Statement getStatement() throws SQLException {
		return currentResultSet.getStatement();
	}

	public String getString(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getString(columnIndex);
	}

	public String getString(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getString(columnName);
	}

	public Time getTime(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getTime(columnIndex);
	}

	public Time getTime(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getTime(columnName);
	}

	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getTime(columnIndex,cal);
	}

	public Time getTime(String columnName, Calendar cal) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getTime(columnName,cal);
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getTimestamp(columnIndex);
	}

	public Timestamp getTimestamp(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getTimestamp(columnName);
	}

	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getTimestamp(columnIndex, cal);
	}

	public Timestamp getTimestamp(String columnName, Calendar cal)
			throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getTimestamp(columnName, cal);
	}

	public int getType() throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	public URL getURL(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getURL(columnIndex);
	}

	public URL getURL(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getURL(columnName);
	}

	@SuppressWarnings("deprecation")
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getUnicodeStream(columnIndex);
	}

	@SuppressWarnings("deprecation")
	public InputStream getUnicodeStream(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getUnicodeStream(columnName);
	}

	public SQLWarning getWarnings() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.getWarnings();
	}

	public void insertRow() throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.insertRow();
	}

	public boolean isAfterLast() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.isAfterLast();
	}

	public boolean isBeforeFirst() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.isBeforeFirst();
	}

	public boolean isFirst() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.isFirst();
	}

	public boolean isLast() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.isLast();
	}

	public boolean last() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.last();
	}

	public void moveToCurrentRow() throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.moveToCurrentRow();
	}

	public void moveToInsertRow() throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.moveToInsertRow();
	}

	public boolean next() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.next();
	}

	public boolean previous() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.previous();
	}

	public void refreshRow() throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.refreshRow();
	}

	public boolean relative(int rows) throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.relative(rows);
	}

	public boolean rowDeleted() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.rowDeleted();
	}

	public boolean rowInserted() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.rowInserted();
	}

	public boolean rowUpdated() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.rowUpdated();
	}

	public void setFetchDirection(int direction) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.setFetchDirection(direction);
	}

	public void updateArray(int columnIndex, Array x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateArray(columnIndex, x);
	}

	public void updateArray(String columnName, Array x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateArray(columnName, x);
	}

	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateAsciiStream(columnIndex, x, length);
	}

	public void updateAsciiStream(String columnName, InputStream x, int length)
			throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateAsciiStream(columnName, x, length);
	}

	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateBigDecimal(columnIndex, x);
	}

	public void updateBigDecimal(String columnName, BigDecimal x)
			throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateBigDecimal(columnName, x);
	}

	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateBinaryStream(columnIndex, x, length);
	}

	public void updateBinaryStream(String columnName, InputStream x, int length)
			throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateBinaryStream(columnName, x, length);
	}

	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateBlob(columnIndex, x);
	}

	public void updateBlob(String columnName, Blob x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateBlob(columnName, x);
	}

	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateBoolean(columnIndex, x)
		;
	}

	public void updateBoolean(String columnName, boolean x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateBoolean(columnName, x);
	}

	public void updateByte(int columnIndex, byte x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateByte(columnIndex, x);
	}

	public void updateByte(String columnName, byte x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateByte(columnName, x);
	}

	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateBytes(columnIndex, x);
	}

	public void updateBytes(String columnName, byte[] x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateBytes(columnName, x);
	}

	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateCharacterStream(columnIndex, x, length)
		;
	}

	public void updateCharacterStream(String columnName, Reader reader,
			int length) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateCharacterStream(columnName, reader, length);
	}

	public void updateClob(int columnIndex, Clob x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateClob(columnIndex, x);
	}

	public void updateClob(String columnName, Clob x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateClob(columnName, x);
	}

	public void updateDate(int columnIndex, Date x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateDate(columnIndex, x);
	}

	public void updateDate(String columnName, Date x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateDate(columnName, x);
	}

	public void updateDouble(int columnIndex, double x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateDouble(columnIndex, x);
	}

	public void updateDouble(String columnName, double x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateDouble(columnName, x);
	}

	public void updateFloat(int columnIndex, float x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateFloat(columnIndex, x);
	}

	public void updateFloat(String columnName, float x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateFloat(columnName, x);
	}

	public void updateInt(int columnIndex, int x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateInt(columnIndex, x);
	}

	public void updateInt(String columnName, int x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateInt(columnName, x);
	}

	public void updateLong(int columnIndex, long x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateLong(columnIndex, x);
	}

	public void updateLong(String columnName, long x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateLong(columnName, x);
	}

	public void updateNull(int columnIndex) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateNull(columnIndex);
	}

	public void updateNull(String columnName) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateNull(columnName);
	}

	public void updateObject(int columnIndex, Object x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateObject(columnIndex, x);
	}

	public void updateObject(String columnName, Object x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateObject(columnName, x);
	}

	public void updateObject(int columnIndex, Object x, int scale)
			throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateObject(columnIndex, x, scale);
	}

	public void updateObject(String columnName, Object x, int scale)
			throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateObject(columnName, x, scale);
	}

	public void updateRef(int columnIndex, Ref x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateRef(columnIndex, x);
	}

	public void updateRef(String columnName, Ref x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateRef(columnName, x);
	}

	public void updateRow() throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateRow();
	}

	public void updateShort(int columnIndex, short x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateShort(columnIndex, x);
	}

	public void updateShort(String columnName, short x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateShort(columnName, x);
	}

	public void updateString(int columnIndex, String x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateString(columnIndex, x);
	}

	public void updateString(String columnName, String x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateString(columnName, x);
	}

	public void updateTime(int columnIndex, Time x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateTime(columnIndex, x);
	}

	public void updateTime(String columnName, Time x) throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateTime(columnName, x);
	}

	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateTimestamp(columnIndex, x);
	}

	public void updateTimestamp(String columnName, Timestamp x)
			throws SQLException {
		checkRSIsClosedOrNull();
		currentResultSet.updateTimestamp(columnName, x);
	}

	public boolean wasNull() throws SQLException {
		checkRSIsClosedOrNull();
		return currentResultSet.wasNull();
	}

	public RowId getRowId(int columnIndex) throws SQLException
	{
		checkRSIsClosedOrNull();
		return currentResultSet.getRowId(columnIndex);
	}

	public RowId getRowId(String columnLabel) throws SQLException
	{
		checkRSIsClosedOrNull();
		return currentResultSet.getRowId(columnLabel);
	}

	public void updateRowId(int columnIndex, RowId x) throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateRowId(columnIndex, x);
	}

	public void updateRowId(String columnLabel, RowId x) throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateRowId(columnLabel, x);
	}

	public int getHoldability() throws SQLException
	{
		checkRSIsClosedOrNull();
		return currentResultSet.getHoldability();
	}

	public void updateNString(int columnIndex, String nString)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateNString(columnIndex, nString);
	}

	public void updateNString(String columnLabel, String nString)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateNString(columnLabel, nString);
	}

	public void updateNClob(int columnIndex, NClob nClob) throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateNClob(columnIndex, nClob);
	}

	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateNClob(columnLabel, nClob);
	}

	public NClob getNClob(int columnIndex) throws SQLException
	{
		checkRSIsClosedOrNull();
		return currentResultSet.getNClob(columnIndex);
	}

	public NClob getNClob(String columnLabel) throws SQLException
	{
		checkRSIsClosedOrNull();
		return currentResultSet.getNClob(columnLabel);
	}

	public SQLXML getSQLXML(int columnIndex) throws SQLException
	{
		checkRSIsClosedOrNull();
		return currentResultSet.getSQLXML(columnIndex);
	}

	public SQLXML getSQLXML(String columnLabel) throws SQLException
	{
		checkRSIsClosedOrNull();
		return currentResultSet.getSQLXML(columnLabel);
	}

	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateSQLXML(columnIndex, xmlObject);
	}

	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateSQLXML(columnLabel, xmlObject);
	}

	public String getNString(int columnIndex) throws SQLException
	{
		checkRSIsClosedOrNull();
		return currentResultSet.getNString(columnIndex);
	}

	public String getNString(String columnLabel) throws SQLException
	{
		checkRSIsClosedOrNull();
		return currentResultSet.getNString(columnLabel);
	}

	public Reader getNCharacterStream(int columnIndex) throws SQLException
	{
		checkRSIsClosedOrNull();
		return currentResultSet.getNCharacterStream(columnIndex);
	}

	public Reader getNCharacterStream(String columnLabel) throws SQLException
	{
		checkRSIsClosedOrNull();
		return currentResultSet.getNCharacterStream(columnLabel);
	}

	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateNCharacterStream(columnIndex, x, length);
	}

	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateNCharacterStream(columnLabel, reader, length);
	}

	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateAsciiStream(columnIndex, x, length);
	}

	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateBinaryStream(columnIndex, x, length);
	}

	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateCharacterStream(columnIndex, x, length);
	}

	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateAsciiStream(columnLabel, x, length);
	}

	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateBinaryStream(columnLabel, x, length);
	}

	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateCharacterStream(columnLabel, reader,length);
	}

	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateBlob(columnIndex, inputStream, length);
	}

	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateBlob(columnLabel, inputStream,length);
	}

	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateClob(columnIndex, reader, length);
	}

	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateClob(columnLabel, reader,length);
	}

	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateNClob(columnIndex, reader, length);
	}

	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateNClob(columnLabel, reader, length);
	}

	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateNCharacterStream(columnIndex, x);
	}

	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateNCharacterStream(columnLabel, reader);
	}

	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateAsciiStream(columnIndex, x);
	}

	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateBinaryStream(columnIndex, x);
	}

	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateCharacterStream(columnIndex, x);
	}

	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateAsciiStream(columnLabel, x);
	}

	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateBinaryStream(columnLabel, x);
	}

	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateCharacterStream(columnLabel, reader);
	}

	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateBlob(columnIndex, inputStream);
	}

	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException
	{

		checkRSIsClosedOrNull();
		currentResultSet.updateBlob(columnLabel, inputStream);
	}

	public void updateClob(int columnIndex, Reader reader) throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateClob(columnIndex, reader);
	}

	public void updateClob(String columnLabel, Reader reader)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateClob(columnLabel, reader);
	}

	public void updateNClob(int columnIndex, Reader reader) throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateNClob(columnIndex, reader);
	}

	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException
	{
		checkRSIsClosedOrNull();
		currentResultSet.updateNClob(columnLabel, reader);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		return this.getClass().isAssignableFrom(iface);
	}


	@SuppressWarnings("unchecked")
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		try
		{
			return (T) this;
		} catch (Exception e)
		{
			throw new SQLException(e);
		}
	}
}
