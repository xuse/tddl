package com.taobao.tddl.sqlobjecttree;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.Column;

/**
 * @author junyu 
 *@author mazhidan.pt
 */
public class Distinct implements Function {
	protected Columns columns = new Columns();
//	private List<Column> columns = new ArrayList<Column>();
	
	public void addColumn(String table, String column, String alias) {
		columns.addColumn(table, column, alias);
	}

	public void addColumn(Column col) {
		columns.addColumn(col);
	}
	
	@Override
	public StringBuilder regTableModifiable(Set<String> logicTableNames,
			List<Object> list, StringBuilder sb) {
	    sb.append(" DISTINCT ");
		return sb;
	}

	@Override
	public String toString() {
		return " DISTINCT ";
	}

	
	public String getFuncName() {
		// TODO Auto-generated method stub
		return "distinct";
	}
	
	//此处因为distinct和普通的只有一个参数的函数不同，所以重写父类的此方法
	//不同点1：distinct并不是严格意义上的函数，distinct 关键字后面用空格，列名 来表示，也不是通过函数的括号
	//不同点2：distinct 后面可能为一列，也可能为多列（此列是Column接口）
	@Override
	public void appendSQL(StringBuilder sb) {
		sb.append(getFuncName());
		sb.append(" ");
		columns.appendSQL(sb);
		sb.append(" ");
	}

	@Override
	public Comparable<?> eval() {
		return null;
	}

	@Override
	public Comparable<?> getVal(List<Object> args) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setValue(List<Object> values) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getNestedColName() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	public Columns getColumns() {
		return columns;
	}

	public void setColumns(Columns columns) {
		this.columns = columns;
	}


}
