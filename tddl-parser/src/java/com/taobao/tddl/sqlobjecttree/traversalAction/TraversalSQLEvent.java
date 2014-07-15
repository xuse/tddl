package com.taobao.tddl.sqlobjecttree.traversalAction;

import com.taobao.tddl.sqlobjecttree.DMLCommon;

public class TraversalSQLEvent {
	public enum StatementType{
		/**
		 * 表明sql来自表名 
		 */
		TABLE,
		/**
		 * 表明sql来自where条件
		 */
		WHERE
		,
		/**
		 * 表明来自最底层
		 */
		NORMAL
	}
	private final DMLCommon currStatement;
	private final StatementType type;
	public TraversalSQLEvent(StatementType type,DMLCommon currentStatement) {
		this.type=type;
		this.currStatement=currentStatement;
	}
	public StatementType getType() {
		return type;
	}

	public DMLCommon getCurrStatement() {
		return currStatement;
	}
}
