package com.taobao.tddl.rule.ruleengine.impl.type;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.common.exception.runtime.CantFindTargetTabRuleTypeHandlerException;
import com.taobao.tddl.rule.ruleengine.TableRuleType;

public class TypeRegister {
	private static final Map<TableRuleType,TableNameTypeHandler> handler=new HashMap<TableRuleType, TableNameTypeHandler>();
	static{
		handler.put(TableRuleType.PREFIX, new PrefixTypeHandler());
		handler.put(TableRuleType.SUFFIX, new SuffixTypeHandler());
	}
	public static TableNameTypeHandler getTableNameHandler(TableRuleType tablrRunleType){
		if(tablrRunleType==null){
			throw new IllegalArgumentException("未输入TableRuleType");
		}
		TableNameTypeHandler tHandler=handler.get(tablrRunleType);
		if(tHandler==null){
			throw new CantFindTargetTabRuleTypeHandlerException(tablrRunleType.toString());
		}
		return tHandler;
	}
}
