package com.taobao.tddl.sqlobjecttree.oracle;

import java.util.Map;

import com.taobao.tddl.sqlobjecttree.common.expression.ComparableExpression;
import com.taobao.tddl.sqlobjecttree.common.expression.GreaterThanOrEquivalent;
import com.taobao.tddl.sqlobjecttree.common.value.BindVar;

/*
 * @author guangxia
 * @since 1.0, 2009-9-3 上午10:47:48
 */
public class OracleFrom extends OraclePageWrapper {
	
	private int add = 0;
	
	public OracleFrom(ComparableExpression comp) {
		super(comp);
		if(comparableExpression instanceof GreaterThanOrEquivalent) {
			add = 1;
		}
	}

	public void modifyParam(Number skip, Number max,Map<Integer, Object> changeParam) {
		Object obj=null;
		if(skip instanceof Long){
			obj=(Long)skip + add;
		}else if(skip instanceof Integer){
			obj=(Integer)skip + add;
		}else{
			throw new IllegalArgumentException("只支持int long的情况");
		}
		changeParam.put(((BindVar)comparableExpression.getRight()).getIndex(), obj);
	}
	
	public String getSqlReturn(Number skip, Number max) {
		if(skip instanceof Long){
			return ((Long)((Long)skip + add)).toString();
		}else if(skip instanceof Integer){
			return ((Integer)((Integer)skip + add)).toString();
		}else{
			throw new IllegalArgumentException("只支持int long的情况");
		}
	}

}
