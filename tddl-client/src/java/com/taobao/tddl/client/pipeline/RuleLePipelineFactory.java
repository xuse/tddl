//Copyright(c) Taobao.com
package com.taobao.tddl.client.pipeline;

import java.sql.SQLException;
import java.util.Map;

import com.taobao.tddl.client.dispatcher.SqlDispatcher;
import com.taobao.tddl.client.handler.executionplan.BatchTargetSqlHandler;
import com.taobao.tddl.client.handler.executionplan.ExecutionPlanHandler;
import com.taobao.tddl.client.handler.executionplan.SqlDirectDispatchHandler;
import com.taobao.tddl.client.handler.rulematch.RuleLeRouteMatchHandler;
import com.taobao.tddl.client.handler.sqlparse.RouteConditionHandler;
import com.taobao.tddl.client.handler.sqlparse.SqlParseHandler;
import com.taobao.tddl.client.handler.validate.SqlDispatchHandler;
import com.taobao.tddl.common.SQLPreParser;
import com.taobao.tddl.common.client.ThreadLocalString;
import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.interact.rule.VirtualTable;
import com.taobao.tddl.interact.rule.bean.DBType;
import com.taobao.tddl.rule.le.TddlRuleInner;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.DirectlyRouteCondition;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-5-3下午01:31:37
 */
public class RuleLePipelineFactory extends AbstractPipelineFactory {
	private Pipeline defaultPipeline = new DefaultPipeline();

	@SuppressWarnings("unused")
	private RuleLePipelineFactory() {}

	public RuleLePipelineFactory(TddlRuleInner tddlRule) {
		defaultPipeline.addLast(RouteConditionHandler.HANDLER_NAME,new RouteConditionHandler());
		defaultPipeline.addLast(SqlParseHandler.HANDLER_NAME,new SqlParseHandler());
		defaultPipeline.addLast(RuleLeRouteMatchHandler.HANDLER_NAME,new RuleLeRouteMatchHandler(tddlRule));
		defaultPipeline.addLast(SqlDirectDispatchHandler.HANDLER_NAME, new SqlDirectDispatchHandler());
		defaultPipeline.addLast(SqlDispatchHandler.HANDLER_NAME,new SqlDispatchHandler());
		defaultPipeline.addLast(ExecutionPlanHandler.HANDLER_NAME, new ExecutionPlanHandler());
	    defaultPipeline.addLast(BatchTargetSqlHandler.HANDLER_NAME, new BatchTargetSqlHandler());
	}

	@Override
	public Pipeline getPipeline() {
		return defaultPipeline;
	}

	@Override
	public DirectlyRouteCondition sqlPreParse(String sql) throws SQLException {
		//如果用户指定了ROUTE_CONDITION或者DB_SELECTOR，那么跳过预解析，防止干扰
		if (null != ThreadLocalMap.get(ThreadLocalString.ROUTE_CONDITION)
				|| null != ThreadLocalMap.get(ThreadLocalString.DB_SELECTOR)
				|| null != ThreadLocalMap.get(ThreadLocalString.RULE_SELECTOR)) {
			return null;
		}

		String firstTable = SQLPreParser.findTableName(sql);
		if (null != firstTable) {
			Map<String, VirtualTable> vtabMap = this.defaultDispatcher
					.getVtabroot().getVirtualTableMap();

			if(null!=vtabMap.get(firstTable)){
				return null;
			}
		}

		logger.debug("no logic table in defaultDispather's logicTableMap,try to produce DirectlyRouteCondition");

		DirectlyRouteCondition condition = new DirectlyRouteCondition();
		//检查表名是否在dbIndexMap中 add by jiechen.qzm
		Map<String, String> dbIndexMap = this.defaultDispatcher.getVtabroot().getDbIndexMap();
		if(dbIndexMap != null && dbIndexMap.get(firstTable) != null){
			condition.setDBId(dbIndexMap.get(firstTable));
			return condition;
		}

		String defaultDbIndex = this.defaultDispatcher.getVtabroot().getDefaultDbIndex();
		if(defaultDbIndex == null){
		    throw new SQLException("the defaultDispatcher have no dbIndexMap and defaultDbIndex");
		}
		//表名不存在logicTable map 和 dbIndexMap中，指定执行dbIndex返回
		condition.setDBId(defaultDbIndex);
		return condition;
	}

	@Override
	public DBType decideDBType(String sql,SqlDispatcher sqlDispatcher)throws SQLException{
		String firstTable = SQLPreParser.findTableName(sql);
		if (null != firstTable) {
			Map<String, VirtualTable> vtabMap = sqlDispatcher.getVtabroot().getVirtualTableMap();
			DBType findInLogicTab=null;
			if(null!=vtabMap.get(firstTable)){
				findInLogicTab=vtabMap.get(firstTable).getDbType();
			}
			return findInLogicTab;
		}

		return sqlDispatcher.getVtabroot().getDbTypeEnumObj();
	}
}
