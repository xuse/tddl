package com.taobao.tddl.sqlobjecttree.common.expression;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.sqlobjecttree.oracle.OracleTo;

public class LessThanOrEquivalent extends ComparableExpression {

	protected int getComparativeOperation() {
		return Comparative.LessThanOrEqual;
	}

	public String getRelationString() {
		return " <= ";
	}

	public StringBuilder regTableModifiable(Set<String> oraTabName, List<Object> list,
			StringBuilder sb) {
		if(this.isRownum()) {
			return  new OracleTo(this).regTableModifiable(oraTabName, list, sb);
		} else {
			return super.regTableModifiable(oraTabName, list, sb);
		}
	}

}
