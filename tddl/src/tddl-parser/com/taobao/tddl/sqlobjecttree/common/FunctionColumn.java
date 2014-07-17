package com.taobao.tddl.sqlobjecttree.common;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.Column;
import com.taobao.tddl.common.sqlobjecttree.SQLFragment;
import com.taobao.tddl.sqlobjecttree.Function;

public class FunctionColumn implements Function,Column {
	Function function;
	String alias;
	
	public FunctionColumn(Function function){
		this.function=function;
	}
	
	public FunctionColumn(){
		
	}
	
	public void setAlias(String alias) {
		this.alias = alias;
	}

	public Function getFunction() {
		return function;
	}

	public void setFunction(Function function) {
		this.function = function;
	}

	public void setValue(List<Object> values) {
		function.setValue(values);
	}

	public Comparable<?> getVal(List<Object> args) {
		return function.getVal(args);
	}
	public Comparable<?> eval() {
		return this;
	}

	public String getNestedColName() {
		return function.getNestedColName();
	}

	public void appendSQL(StringBuilder sb) {
		function.appendSQL(sb);
		if(alias!=null){
			sb.append(" as ").append(alias);
		}
	}

	public String getAlias() {
		return alias;
	}

	public String getColumn() {
		StringBuilder sb=new StringBuilder();
		appendSQL(sb);
		return sb.toString();
	}

	public String getTable() {
		throw new RuntimeException("还不支持");
	}

	public void setModifiedTableName(String table) {
		throw new IllegalStateException("不应该被调用");
	}
	public String getModifiedTableName() {
		throw new IllegalStateException("不应该被调用");
	}
	public StringBuilder regTableModifiable(Set<String> oraTabName, List<Object> list,
			StringBuilder sb) {
		sb=function.regTableModifiable(oraTabName, list, sb);
		if(alias!=null){
			sb.append(" as ").append(alias);
		}
		return sb;
	}

	public int compareTo(Object arg0) {
		throw new IllegalStateException("should not be here");
	}

	public void setAliasMap(Map<String, SQLFragment> aliasMap) {
		if(alias!=null){
			aliasMap.put(alias, this);
		}
	}

	public Class<? extends Function> getNestClass() {
		return function.getClass();
	}
}
