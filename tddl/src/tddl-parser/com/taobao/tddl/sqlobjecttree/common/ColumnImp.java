package com.taobao.tddl.sqlobjecttree.common;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.Column;
import com.taobao.tddl.common.sqlobjecttree.SQLFragment;
import com.taobao.tddl.common.sqlobjecttree.SubQueryValue;
import com.taobao.tddl.sqlobjecttree.Function;
import com.taobao.tddl.sqlobjecttree.Select;
import com.taobao.tddl.sqlobjecttree.TableWrapper;
import com.taobao.tddl.sqlobjecttree.common.value.UnknowValueObject;

public class ColumnImp implements Column, SubQueryValue {
	private Map<String, SQLFragment> aliasMap;
	String column = null;
	String modifiedTableName = null;
	String table = null;
	String alias = null;

	/**
	 * 为空请填null
	 * 
	 * @param table
	 * @param column
	 * @param alias
	 */
	public ColumnImp(String table, String column, String alias) {
		if (column == null) {
			throw new IllegalArgumentException("列的列名不能为空");
		}
		this.column = column;
		this.alias = alias;
		this.table = table;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getAlias() {
		return alias;
	}

	public void setAliasMap(Map<String, SQLFragment> aliasMap) {
		this.aliasMap = aliasMap;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}



	public void appendSQL(StringBuilder sb) {

		if (table != null) {
			sb.append(table).append(".");
		}
		sb.append(column);
		if (alias != null) {
			sb.append(" as ");
			sb.append(alias);
		}

	}

	public StringBuilder regTableModifiable(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		
		if (table != null) {
			if (oraTabName.contains(table)) {
				list.add(sb.toString());
				TableWrapper wrapper = new TableWrapper();
				wrapper.setOriTable(table);
				list.add(wrapper);
				sb = new StringBuilder();
				sb.append(".");
			} else {
				sb.append(table);
				sb.append(".");
			}
		}
		sb.append(column);
		if (alias != null) {
			sb.append(" as ");
			sb.append(alias);
		}
		return sb;
	}

	public Comparable<?> eval() {
		return this;
	}

	// tab.id=v.id 处理这个情况用
	public Comparable<?> getVal(List<Object> args) {
		if (aliasMap == null) {
			throw new IllegalArgumentException("必须输入aliasMap");
		}
		SQLFragment fragement;
		fragement = aliasMap.get(table.toUpperCase());

		if (fragement != null) {
			if (fragement instanceof TableNameFunctionImp) {
				Function func = ((TableNameFunctionImp) fragement)
						.getFunction();
				return func.getVal(args);
			} else if (fragement instanceof TableNameSubQueryImp) {
				Select subSelect = ((TableNameSubQueryImp) fragement)
						.getSubSelect();
				TableNameFunctionImp tbNameFunc = subSelect
						.getTableNameFunction();
				if (tbNameFunc == null) {
					throw new IllegalArgumentException("列名字段:" + table + "."
							+ column + "不能映射到一个可以赋值的" + "虚拟列名中");
				} else {
					Function func = tbNameFunc.getFunction();
					return func.getVal(args);
				}
			} else {
				return UnknowValueObject.getUnknowValueObject();
			}
		} else {
			return UnknowValueObject.getUnknowValueObject();
		}
	}

	public int compareTo(Object o) {
		throw new IllegalStateException("should not be here");
	}

	public Class<String> getNestClass() {
		return String.class;
	}
}
