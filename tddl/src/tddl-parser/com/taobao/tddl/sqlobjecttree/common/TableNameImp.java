package com.taobao.tddl.sqlobjecttree.common;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.SQLFragment;
import com.taobao.tddl.sqlobjecttree.JoinClause;
import com.taobao.tddl.sqlobjecttree.TableName;
import com.taobao.tddl.sqlobjecttree.TableWrapper;


public class TableNameImp implements TableName{
	private String alias;
	private String tablename;
	private String schemaName;
	private boolean isOracle;
	private JoinClause joinClause;
	public TableNameImp() {}
	public TableNameImp(boolean isOracle) {
		this.isOracle = isOracle;
	}
	
	public String getSchemaName() {
		return schemaName;
	}
	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
	public String getAlias() {
		return alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}
	public Set<String> getTableName() {
		Set<String> tableNames = new HashSet<String>(2);
		tableNames.add(tablename);
		if(joinClause != null && joinClause.getTableName() != null){
			tableNames.addAll(joinClause.getTableName().getTableName());
		}
		return tableNames;
	}
	
	public void appendSQL(StringBuilder sb) {
		if (getSchemaName() != null) {
			sb.append(schemaName).append(".");
		}
			sb.append(tablename);
		if (getAlias() != null) {
			sb.append(isOracle ? " " : " as ").append(getAlias());
		}
		if(this.joinClause != null){
			joinClause.appendSQL(sb);
		}
	}
	
	
	
	public JoinClause getJoinClause() {
		return joinClause;
	}
	public void setJoinClause(JoinClause joinClause) {
		this.joinClause = joinClause;
	}
	
	@Override
	public int hashCode() {
		StringBuilder sb=new StringBuilder();
		sb.append(this.schemaName).append(".").append(this.tablename);
		return sb.toString().hashCode();
		
	}
	public boolean equals(Object obj) {
		//FIXME : equals 没有对null进行判断
		if(obj==null){
			return false;
		}
		if(!(obj instanceof TableNameImp)){
			return false;
		}
		TableNameImp imp=(TableNameImp)obj;
		JoinClause joinClause = imp.getJoinClause();
		boolean joinEqual = false;
		if(joinClause == null){
			joinEqual = (joinClause==this.joinClause);
		}else{
			joinEqual = (joinClause.equals(this.joinClause));
		}
		return imp.tablename.equals(this.tablename)&&imp.schemaName.equals(this.schemaName)&&joinEqual;
	}
 
	public String getTableNameStr(){
		return tablename;
	}
//	public void regTableModifiable(String oraTabName,
//			List<ModifiableTableName> list) {
//		if(tablename!=null&&tablename.equalsIgnoreCase(oraTabName)){
//			list.add(this);
//		}
//	}
	public StringBuilder regTableModifiable(Set<String> logicTableNames, List<Object> list,
			StringBuilder sb) {
		if (getSchemaName() != null) {
			sb.append(schemaName).append(".");
		}
		if(tablename!=null&&logicTableNames.contains(tablename)){
			list.add(sb.toString());
			TableWrapper wr=new TableWrapper();
			wr.setOriTable(tablename);
			list.add(wr);
			sb=new StringBuilder();
		}else if(tablename!=null){
			sb.append(tablename);
		}else{
			throw new IllegalArgumentException("表名对象中不能缺少表名");
		}
		if (getAlias() != null) {
			sb.append(isOracle ? " " : " as ").append(getAlias());
		}
		if(this.joinClause != null){
			joinClause.regTableModifiable(logicTableNames, list, sb);
		}
		return sb;
	}
	public void appendAliasToSQLMap(Map<String, SQLFragment> map) {
		if(getAlias()!=null){
			map.put(getAlias().toUpperCase(), this);
		}
		if(joinClause != null){
			joinClause.appendAliasToSQLMap(map);
		}
	}
}
