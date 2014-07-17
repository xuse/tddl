//Copyright(c) Taobao.com
package com.taobao.tddl.client.handler.executionplan;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.client.databus.DataBus;
import com.taobao.tddl.client.databus.PipelineRuntimeInfo;
import com.taobao.tddl.client.dispatcher.SingleLogicTableName;
import com.taobao.tddl.client.handler.AbstractHandler;
import com.taobao.tddl.client.jdbc.RealSqlContext;
import com.taobao.tddl.client.jdbc.RealSqlContextImp;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlanImp;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.sqlobjecttree.DMLCommon;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.DirectlyRouteCondition;

/**
 * @description 此handler主要功能是对于直接指定库的sql生成ExecutionPlan,功能
 *              上与ExecutionPlanHandler平行.
 *              
 *              我们常常需要通过TDataSource执行分库分表的sql和不做分库分表的sql,
 *              这在2.4.x之前相对来说是比较困难的事情,那么我们在后续版本中提供了
 *              这么一种解决方案,只要你告诉我在哪个库上执行,那么TDDL将不对这种sql
 *              进行解析和规则计算,直接在目标库上执行掉sql.而这个handler也就是将
 *              这种直接执行的上下文封装成ExecutionPlan提供给之后的执行引擎执行.
 *              
 *              我们可以使用RouteHelper.executeByDB(dbIndex)告诉TDDL你所要直接执行
 *              sql的目标库,当然你现在也可以在规则文件中的ShardRule Bean下配上
 *              defaultDbIndex,那么你不告诉我直接执行的目标库,并且sql中的第一张表
 *              没有分库分表,我们将会把这条sql在defaultDbIndex上执行掉.
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.4
 * @since 1.6
 * @date 2010-09-08下午06:33:32
 */
public class SqlDirectDispatchHandler extends AbstractHandler {
	public static final String HANDLER_NAME = "SqlDirectDispatchHandler";
	private final Log log = LogFactory.getLog(SqlDirectDispatchHandler.class);

	/**
	 * DirectDispatchHandler主要使用于不分库，也不分表的case
	 */
	public void handleDown(DataBus dataBus) throws SQLException {
		if (FlowType.DIRECT == getPipeLineRuntimeInfo(dataBus).getFlowType()) {
			dispatch(dataBus);
		}
	}

	/**
	 * 直接构建执行计划，主体数据结构式DirectlyRouteCondition
	 * 
	 * @param dataBus
	 * @throws SQLException
	 */
	protected void dispatch(DataBus dataBus) throws SQLException {
		PipelineRuntimeInfo runtime = super.getPipeLineRuntimeInfo(dataBus);
		DirectlyRouteCondition directlyRouteCondition = (DirectlyRouteCondition) runtime
				.getStartInfo().getDirectlyRouteCondition();
		String sql = runtime.getStartInfo().getSql();
		Map<Integer, ParameterContext> sqlParam = runtime.getStartInfo()
				.getSqlParam();

		/**
		 * 构建执行计划
		 */
		ExecutionPlan executionPlan = getDirectlyExecutionPlan(sql, sqlParam,
				directlyRouteCondition);

		/**
		 * 设置执行计划
		 */
		setResult(executionPlan, runtime);

		debugLog(log, new Object[] { "sql direct dispatch end." });
	}

	/**
	 * 得到直接执行的执行计划
	 * 
	 * @param sql
	 * @param parameterSettings
	 * @param metaData
	 * @return
	 * @throws SQLException
	 */
	private ExecutionPlan getDirectlyExecutionPlan(String sql,
			Map<Integer, ParameterContext> parameterSettings,
			DirectlyRouteCondition metaData) throws SQLException {
		ExecutionPlanImp executionPlanImp = new ExecutionPlanImp();
		Map<String/* db index key */, List<Map<String/* original table */, String/* targetTable */>>> shardTableMap = metaData
				.getShardTableMap();

		Map<String, List<RealSqlContext>> sqlMap = new HashMap<String, List<RealSqlContext>>(
				shardTableMap.size());
		for (Entry<String/* db index key */, List<Map<String/* original table */, String/* targetTable */>>> entry : shardTableMap
				.entrySet()) {
			List<Map<String, String>> tableMapList = entry.getValue();
			List<RealSqlContext> realSqlContexts = new ArrayList<RealSqlContext>(
					tableMapList.size());
			boolean isUsingRealConnection = false;
			if (tableMapList.isEmpty()) {
				// 如果为空，则直接使用原sql
				RealSqlContextImp realSqlContext = new RealSqlContextImp();
				realSqlContext.setArgument(parameterSettings);
				realSqlContext.setSql(replaceMultiTableName(sql, new HashMap<String, String>()));
				realSqlContexts.add(realSqlContext);
				isUsingRealConnection = true;
			} else {
				for (Map<String, String> targetMap/* logicTable->realTable */: entry
						.getValue()) {

					if (!isUsingRealConnection) {
						// 第一次进入的时候表示使用真实连接;
						isUsingRealConnection = true;
					} else {
						// isUsingRealConnection = true.因为默认为false.所以为true一定是因为
						// 有多于一条sql需要执行。
						isUsingRealConnection = false;
					}
					RealSqlContextImp realSqlContext = new RealSqlContextImp();
					realSqlContext.setArgument(parameterSettings);
					realSqlContext.setRealTable(targetMap.values().toString());
					// 替换表名
					realSqlContext
							.setSql(replaceMultiTableName(sql, targetMap));
					realSqlContexts.add(realSqlContext);
				}
			}
			sqlMap.put(entry.getKey()/* dbIndex */, realSqlContexts);
		}
		executionPlanImp.setSqlMap(sqlMap);
		executionPlanImp.setOriginalArgs(parameterSettings);
		executionPlanImp.setOrderByColumns(metaData.getOrderByMessages()
				.getOrderbyList());
		executionPlanImp
				.setSkip(metaData.getSkip() == DMLCommon.DEFAULT_SKIP_MAX ? 0
						: metaData.getSkip());
		executionPlanImp
				.setMax(metaData.getMax() == DMLCommon.DEFAULT_SKIP_MAX ? -1
						: metaData.getMax());
		executionPlanImp.setGroupFunctionType(metaData.getGroupFunctionType());
		executionPlanImp.setVirtualTableName(new SingleLogicTableName(metaData
				.getVirtualTableName()));
		executionPlanImp.setEvents(null);
		executionPlanImp.setOriginalSql(sql);
		return executionPlanImp;
	}

	private void setResult(ExecutionPlan executionPlan,
			PipelineRuntimeInfo runtime) {
		runtime.setExecutionPlan(executionPlan);
	}
}
