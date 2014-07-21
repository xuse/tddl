package com.taobao.tddl.util.IDAndDateCondition.routeCondImp;

import java.util.Map;

import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.sqlobjecttree.SqlParserResult;

/**
 * 走规则引擎的条件表达式
 * @author shenxun
 *
 */
public interface RuleRouteCondition extends RouteCondition{
	/**
	 * 兼容老实现
	 * @return
	 */
	public Map<String, Comparative> getParameters() ;

	public SqlParserResult getSqlParserResult();
}
