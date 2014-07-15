//Copyright(c) Taobao.com
package com.taobao.tddl.client.handler.executionplan;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.client.controller.DatabaseExecutionContext;
import com.taobao.tddl.client.databus.DataBus;
import com.taobao.tddl.client.databus.PipelineRuntimeInfo;
import com.taobao.tddl.client.dispatcher.DispatcherResult;
import com.taobao.tddl.client.handler.AbstractHandler;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.interact.bean.TargetDB;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.DirectlyRouteCondition;

/**
 * @description 这个handler和ExecutionPlanHandler功能上是平行的,
 *              只处理batch相关逻辑,并且batch基本上是处理数据更新, 所以其不必支持order by,group by,某些聚合函数等
 *              query普遍使用的特性,所以只是进行表名替换,返回目标库和 库上执行的sql即可.
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.3
 * @since 1.6
 * @date 2010-09-03下午01:12:44
 */
public class BatchTargetSqlHandler extends AbstractHandler {
	public static final String HANDLER_NAME = "BatchTargetSqlHandler";
	private static final Log log = LogFactory
			.getLog(BatchTargetSqlHandler.class);

	public void handleDown(DataBus dataBus) throws SQLException {
		FlowType flowType = getPipeLineRuntimeInfo(dataBus).getFlowType();
		if (FlowType.BATCH_DIRECT == flowType || FlowType.BATCH == flowType
				|| FlowType.BATCH_NOSQLPARSER == flowType) {
			makeUp(dataBus);
		}
	}

	public void makeUp(DataBus dataBus) throws SQLException {
		PipelineRuntimeInfo runtime = super.getPipeLineRuntimeInfo(dataBus);
		DirectlyRouteCondition directlyRouteCondition = (DirectlyRouteCondition) runtime
				.getStartInfo().getDirectlyRouteCondition();
		String sql = runtime.getStartInfo().getSql();
		boolean isParameterBatch = runtime.getStartInfo().isParameterBatch();

		if (directlyRouteCondition != null) {
			if (isParameterBatch) {
				targetSqlWithParameterDirect(directlyRouteCondition, sql,
						runtime);
			} else {
				targetSqlWithOutParameterDirect(directlyRouteCondition, sql,
						runtime);
			}
		} else {
			DispatcherResult dispatcherResult = runtime.getMetaData();
			if (isParameterBatch) {
				targetSqlWithParameter(dispatcherResult, sql, runtime);
			} else {
				targetSqlWithOutParameter(dispatcherResult, sql, runtime);
			}
		}
	}

	private void targetSqlWithParameterDirect(DirectlyRouteCondition conditon,
			String sql, PipelineRuntimeInfo runtime) {

		Map<String, Map<String, List<List<ParameterContext>>>> targetSqls = runtime
				.getStartInfo().getTargetSqls();
		String dbId = conditon.getDbRuleID();
		Map<String, List<List<ParameterContext>>> sqls = targetSqls.get(dbId);
		if(sqls == null) {
			sqls = new HashMap<String, List<List<ParameterContext>>>();
			targetSqls.put(dbId, sqls);
		}
		List<List<ParameterContext>> paramsList = sqls.get(sql);
		if(paramsList == null) {
			paramsList = new ArrayList<List<ParameterContext>>();
			sqls.put(sql, paramsList);
		}
		paramsList.add(getBatchParametersList(runtime.getStartInfo()
				.getSqlParam()));

	}

	private void targetSqlWithOutParameterDirect(
			DirectlyRouteCondition conditon, String sql,
			PipelineRuntimeInfo runtime) {
		Map<String, List<String>> targetSqls = runtime.getStartInfo()
				.getTargetSqlsNoParameter();
		List<String> sqlList = targetSqls.get(conditon.getDbRuleID());
		if(sqlList == null) {
			sqlList = new ArrayList<String>();
			targetSqls.put(conditon.getDbRuleID(), sqlList);
		}
		sqlList.add(sql);
	}
	
	@SuppressWarnings("deprecation")
	public void targetSqlWithParameter(DispatcherResult dispatcherResult,
			String sql, PipelineRuntimeInfo runtime) {
		Map<String, Map<String, List<List<ParameterContext>>>> targetSqls = runtime
				.getStartInfo().getTargetSqls();
		List<TargetDB> targets = dispatcherResult.getTarget();
		String virtualTableName = dispatcherResult.getVirtualTableName()
				.toString();
		for (TargetDB target : targets) {
			String targetName = target.getDbIndex();
			if (!targetSqls.containsKey(targetName)) {
				targetSqls.put(targetName,
						new HashMap<String, List<List<ParameterContext>>>());
			}

			Map<String, List<List<ParameterContext>>> sqls = targetSqls
					.get(targetName);

			Set<String> actualTables = target.getTableNames();
			int i = 0;
			for (String tab : actualTables) {
				String targetSql = replaceTableName(sql, virtualTableName, tab,
						log);
				if (!sqls.containsKey(targetSql)) {
					List<List<ParameterContext>> paramsList = new ArrayList<List<ParameterContext>>();
					sqls.put(targetSql, paramsList);
				}
				sqls.get(targetSql).add(
						getBatchParametersList(runtime.getStartInfo()
								.getSqlParam()));
				i++;
			}
		}
	}

	public void targetSqlWithOutParameter(DispatcherResult dispatcherResult,
			String sql, PipelineRuntimeInfo runtime) {
		List<DatabaseExecutionContext> targets = dispatcherResult
				.getDataBaseExecutionContexts();
		Map<String, List<String>> targetSqls = runtime.getStartInfo()
				.getTargetSqlsNoParameter();
		for (DatabaseExecutionContext target : targets) {
			String targetName = target.getDbIndex();
			if (!targetSqls.containsKey(targetName)) {
				targetSqls.put(targetName, new ArrayList<String>(8));
			}

			List<String> sqls = targetSqls.get(targetName);

			List<Map<String, String>> actualTables = target.getTableNames();
			for (Map<String, String> tab : actualTables) {
				String targetSql = replaceMultiTableName(sql, tab);
				sqls.add(targetSql);
			}
		}
	}

	public List<ParameterContext> getBatchParametersList(
			Map<Integer, ParameterContext> parameterSettings) {
		List<ParameterContext> batchedParameterSettings = new ArrayList<ParameterContext>();
		for (ParameterContext context : parameterSettings.values()) {
			batchedParameterSettings.add(context);
		}
		return batchedParameterSettings;
	}
}
