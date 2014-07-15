package com.taobao.tddl.sqlobjecttree.oracle.hint;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.taobao.tddl.sqlobjecttree.HintSetter;

public class Ordered implements HintSetter {
	
	public List<String> getArguments() {
		return Collections.emptyList();
	}

	public void appendSQL(StringBuilder sb) {
		sb.append("ORDERED");
	}

	public StringBuilder regTableModifiable(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		return sb.append("ORDERED");
	}

	public void addHint(List<String> args) {
		//do nothing
	}
}
