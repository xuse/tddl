package com.taobao.tddl.sqlobjecttree.oracle.hint;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.taobao.tddl.sqlobjecttree.HintSetter;

public class UseNL implements HintSetter {
	List<String> args=Collections.emptyList();
	public List<String> getArguments() {
		return args;
	}

	public void appendSQL(StringBuilder sb) {
		sb.append("USE_NL").append("(");
		boolean firstElement=true;
		for(String str:args){
			if(firstElement){
				firstElement=false;
			}else{
				sb.append(",");
			}
			sb.append(str);
		}
		sb.append(")");
	}

	public StringBuilder regTableModifiable(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		//TODO:重构加入修改index名的方法
		sb.append("USE_NL").append("(");
		boolean firstElement=true;
		for(String str:args){
			if(firstElement){
				firstElement=false;
			}else{
				sb.append(",");
			}
			sb.append(str);
		}
		sb.append(")");
		return sb;
	}

	public void addHint(List<String> args) {
		this.args=args;
	}

}
