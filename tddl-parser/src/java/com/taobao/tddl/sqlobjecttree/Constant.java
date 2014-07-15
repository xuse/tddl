package com.taobao.tddl.sqlobjecttree;

import com.taobao.tddl.sqlobjecttree.common.TableNameSubQueryImp;
import com.taobao.tddl.sqlobjecttree.common.expression.ComparableExpression;
import com.taobao.tddl.sqlobjecttree.common.expression.ExpressionGroup;

/**
 * String utils ,not be used at other position yet
 * @author guangxia
 * @since 1.0, 2009-3-24 10:18:39
 */
public class Constant {
	
	private final static Class<?>[] HAS_TOSTRING = {ExpressionGroup.class, ComparableExpression.class, 
					Select.class, Delete.class, WhereCondition.class, TableNameSubQueryImp.class};
	public static boolean useToString(Object obj) {
		for(Class<?> clazz : HAS_TOSTRING) {
			if(clazz.isInstance(obj)) {
				return true;
			}
		}
		return false;
	}
	

}
