package com.taobao.tddl.sqlobjecttree.common.value;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.Column;
import com.taobao.tddl.sqlobjecttree.Function;
import com.taobao.tddl.sqlobjecttree.Utils;


public abstract class OneArgFunction implements Function{
	protected Object arg1;
	public void setValue(List<Object> values) {
		if(values.size()!=1){
			throw new IllegalArgumentException("参数不为一个");
		}
		arg1=values.get(0);
	}

	public final Comparable<?> eval() {
		return this;
	}

	public String getNestedColName() {
		String ret=null;
		
		if(arg1 instanceof Column){
			ret=((Column)arg1).getColumn();
		}
		return ret;
	}

	public abstract  String getFuncName(); 
	
	public void appendSQL(StringBuilder sb) {
		sb.append(getFuncName());
		sb.append("(");
		Utils.appendSQLList(arg1, sb);
		sb.append(")");
	}

//	public void regTableModifiable(String oraTabName,
//			List<ModifiableTableName> list) {
//		ModifiedTableUtils.regModifiedTab(arg1, oraTabName, list);
//	}
	public StringBuilder regTableModifiable(Set<String> oraTabName, List<Object> list,
			StringBuilder sb) {
		sb.append(getFuncName());
		sb.append("(");
		sb=Utils.appendSQLListWithList(oraTabName, arg1, sb, list);
		sb.append(")");
		return sb;
	}
	public Comparable<?> getVal(List<Object> args) {
		return UnknowValueObject.getUnknowValueObject();
	}
	public int compareTo(Object o) {
		throw new IllegalStateException("should not be here");
	}
}
