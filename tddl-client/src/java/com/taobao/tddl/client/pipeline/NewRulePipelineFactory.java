//Copyright(c) Taobao.com
package com.taobao.tddl.client.pipeline;

import java.sql.SQLException;
import java.util.Map;

import com.taobao.tddl.client.dispatcher.SqlDispatcher;
import com.taobao.tddl.client.handler.executionplan.BatchTargetSqlHandler;
import com.taobao.tddl.client.handler.executionplan.ExecutionPlanHandler;
import com.taobao.tddl.client.handler.executionplan.SqlDirectDispatchHandler;
import com.taobao.tddl.client.handler.rulematch.NewRuleRouteMatchHandler;
import com.taobao.tddl.client.handler.sqlparse.RouteConditionHandler;
import com.taobao.tddl.client.handler.sqlparse.SqlParseHandler;
import com.taobao.tddl.client.handler.validate.SqlDispatchHandler;
import com.taobao.tddl.common.SQLPreParser;
import com.taobao.tddl.common.client.ThreadLocalString;
import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.interact.rule.VirtualTable;
import com.taobao.tddl.interact.rule.bean.DBType;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.DirectlyRouteCondition;

/**
 * @description 244新规则支持,增加NewRuleRouteMatchHandler,工厂重新
 *              实现一个,实例化新的管线,同时重写AbstractPipelineFactory的
 *              sql预解析和表级别dbType定义(规则结构变化所以需要重写).
 *
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.3
 * @since 1.6
 * @date 2010-12-01下午03:35:43
 */
public class NewRulePipelineFactory extends AbstractPipelineFactory {

	private Pipeline defaultPipeline = new DefaultPipeline();

	{
		/**
		 * 初始化各个管线，管线都为单例
		 * 每次查询和数据更新都会返回同一个管线实例。
		 * 唯一与一个操作对应的是总线数据，并且以
		 * 方法的局部变量在管线中的不同处理器之间传递。
		 * 从而从数据上避免多线程问题。
		 */
		defaultPipeline.addLast(RouteConditionHandler.HANDLER_NAME,new RouteConditionHandler());
		defaultPipeline.addLast(SqlParseHandler.HANDLER_NAME,new SqlParseHandler());
		defaultPipeline.addLast(NewRuleRouteMatchHandler.HANDLER_NAME,new NewRuleRouteMatchHandler());
		defaultPipeline.addLast(SqlDirectDispatchHandler.HANDLER_NAME, new SqlDirectDispatchHandler());
		defaultPipeline.addLast(SqlDispatchHandler.HANDLER_NAME,new SqlDispatchHandler());
		defaultPipeline.addLast(ExecutionPlanHandler.HANDLER_NAME, new ExecutionPlanHandler());
	    defaultPipeline.addLast(BatchTargetSqlHandler.HANDLER_NAME, new BatchTargetSqlHandler());
	}

	public NewRulePipelineFactory() {}

	public NewRulePipelineFactory(Pipeline pipeline) {
		this.defaultPipeline=pipeline;
	}

	public Pipeline getPipeline() {
		return defaultPipeline;
	}

	public void setDefaultPipeline(Pipeline defaultPipeline) {
		this.defaultPipeline = defaultPipeline;
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
