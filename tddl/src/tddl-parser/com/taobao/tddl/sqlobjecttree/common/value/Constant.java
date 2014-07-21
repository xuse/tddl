package com.taobao.tddl.sqlobjecttree.common.value;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.sqlobjecttree.Function;

public abstract class Constant implements Function{
	public void setValue(List<Object> values) {
		if(values.size()!=0){
			throw new IllegalArgumentException("参数不为零个");
		}
	}

	public Comparable<?> eval() {
		return this;
	}

	public String getNestedColName() {
		return null;
	}

	public abstract  String getFuncName(); 
	
	public void appendSQL(StringBuilder sb) {
		sb.append(getFuncName());
	}
	public StringBuilder regTableModifiable(Set<String> oraTabName, List<Object> list,
			StringBuilder sb) {
		sb.append(getFuncName());
		return sb;
	}
	public int compareTo(Object o) {
		throw new IllegalStateException("should not be here");
	}
}
