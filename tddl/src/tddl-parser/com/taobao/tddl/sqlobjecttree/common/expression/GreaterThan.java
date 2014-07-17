package com.taobao.tddl.sqlobjecttree.common.expression;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.sqlobjecttree.oracle.OracleFrom;

public class GreaterThan extends ComparableExpression {

	protected int getComparativeOperation() {
		return Comparative.GreaterThan;
	}

	public String getRelationString() {
		return " > ";
	}

	public StringBuilder regTableModifiable(Set<String> oraTabName, List<Object> list,
			StringBuilder sb) {
		if(this.isRownum()) {
			return  new OracleFrom(this).regTableModifiable(oraTabName, list, sb);
		} else {
			return super.regTableModifiable(oraTabName, list, sb);
		}
	}

}
