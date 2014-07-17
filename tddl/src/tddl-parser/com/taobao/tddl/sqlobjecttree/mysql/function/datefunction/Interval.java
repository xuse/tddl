package com.taobao.tddl.sqlobjecttree.mysql.function.datefunction;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.sqlobjecttree.Function;
import com.taobao.tddl.sqlobjecttree.Utils;

/**
 * @author junyu
 *
 */
public class Interval implements Function {
	protected Object expr;
	protected Object dateUnit;
    
	@Override
	public String getNestedColName() {
		return null;
	}

	@Override
	public void setValue(List<Object> values) {
		if(values.size()!=2){
			throw new IllegalArgumentException("参数少于两个");
		}
		expr=values.get(0);
		dateUnit= values.get(1);
	}

	@Override
	public Comparable<?> eval() {
		return this;
	}

	@Override
	public void appendSQL(StringBuilder sb) {
		sb.append(" INTERVAL ");
//		Utils.appendSQL(expr, sb);
		//fixed by mazhidan.pt
		//因为expr可能为-1或者1这样的形式，以前的代码，为-1时，返回的是StringBuilder，能正常返回
		//当返回的为1时，类型为String，则使用Utils.appened。  输出的是'1'，与原文意思不符合
		sb.append(expr.toString());
		Utils.appendSQL(dateUnit,sb);
	}

	@Override
	public StringBuilder regTableModifiable(Set<String> logicTableNames,
			List<Object> list, StringBuilder sb) {
		sb.append(" INTERVAL ");
		Utils.appendSQLWithList(logicTableNames, expr, sb, list);
		Utils.appendSQLWithList(logicTableNames, dateUnit, sb,list);
		return sb;
	}

	@Override
	public int compareTo(Object o) {
		return 0;
	}
	
	@Override
	public String toString() {
		StringBuilder sb=new StringBuilder();
		sb.append(" INTERVAL ");
		Utils.appendSQL(expr, sb);
		Utils.appendSQL(dateUnit, sb);
		return sb.toString();
	}

	@Override
	public Comparable<?> getVal(List<Object> args) {
		return null;
	}
}
