package com.taobao.tddl.sqlobjecttree.oracle;

import java.util.Map;

import com.taobao.tddl.sqlobjecttree.common.expression.ComparableExpression;
import com.taobao.tddl.sqlobjecttree.common.expression.LessThan;
import com.taobao.tddl.sqlobjecttree.common.value.BindVar;

/*
 * @author guangxia
 * @since 1.0, 2009-9-3 上午10:52:43
 */
public class OracleTo extends OraclePageWrapper {
	
	private int add = 0;

	public OracleTo(ComparableExpression comp) {
		super(comp);
		if(comparableExpression instanceof LessThan) {
			add = 1;
		}
	}
	

	public String getSqlReturn(Number skip, Number max) {
		if(max instanceof Long){
			return ((Long)((Long)max + add)).toString();
		}else if(max instanceof Integer){
			return ((Integer)((Integer)max + add)).toString();
		}else{
			throw new IllegalArgumentException("只支持int long的情况");
		}
	}

	public void modifyParam(Number skip, Number max,
			Map<Integer, Object> modifiedMap) {
		Object obj=null;
		if(max instanceof Long){
			obj=(Long)max + add;
		}else if(max instanceof Integer){
			obj=(Integer)max + add;
		}else{
			throw new IllegalArgumentException("只支持int long的情况");
		}
		modifiedMap.put(((BindVar)comparableExpression.getRight()).getIndex(), obj);
	}

}
