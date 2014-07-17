package com.taobao.tddl.sqlobjecttree.oracle;

import com.taobao.tddl.sqlobjecttree.Expression;
import com.taobao.tddl.sqlobjecttree.WhereCondition;
import com.taobao.tddl.sqlobjecttree.common.expression.ExpressionGroup;

public class OracleWhereCondition extends WhereCondition{
	
	private ExpressionGroup rownum=new ExpressionGroup();
	public void addRowNum(Expression exp){
		rownum.addExpression(exp);
	}
	

	
}
