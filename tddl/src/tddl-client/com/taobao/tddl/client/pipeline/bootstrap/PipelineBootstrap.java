//Copyright(c) Taobao.com
package com.taobao.tddl.client.pipeline.bootstrap;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.client.RouteCondition.ROUTE_TYPE;
import com.taobao.tddl.client.databus.DataBus;
import com.taobao.tddl.client.databus.PipelineContextDataBus;
import com.taobao.tddl.client.databus.PipelineRuntimeInfo;
import com.taobao.tddl.client.databus.StartInfo;
import com.taobao.tddl.client.dispatcher.DispatcherResult;
import com.taobao.tddl.client.dispatcher.SqlDispatcher;
import com.taobao.tddl.client.handler.AbstractHandler.FlowType;
import com.taobao.tddl.client.jdbc.ConnectionManager;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.pipeline.Pipeline;
import com.taobao.tddl.client.pipeline.PipelineFactory;
import com.taobao.tddl.common.channel.SqlMetaData;
import com.taobao.tddl.common.channel.impl.SqlMetaDataFactory;
import com.taobao.tddl.common.client.ThreadLocalString;
import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;
import com.taobao.tddl.util.HintParser;
import com.taobao.tddl.util.HintParserHelper;
import com.taobao.tddl.util.SimpleHintParser;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.DirectlyRouteCondition;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.SimpleCondition;

/**
 * @description 管线任务流转启动器接口实现类
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.3
 * @since 1.6
 * @date 2010-09-14上午11:45:43
 */
public class PipelineBootstrap implements Bootstrap {
	protected PipelineFactory pipelineFactory;
	protected ConnectionManager connectionManager;

	public PipelineBootstrap(ConnectionManager connectionManager,
			PipelineFactory pipelineFactory) {
		this.pipelineFactory = pipelineFactory;
		this.connectionManager = connectionManager;
	}

	public ExecutionPlan bootstrap(StartInfo startInfo) throws SQLException {
		DataBus dataBus = null;
		//最好不要做多次
//		RouteCondition rcFromHint = HintParser.convertHint2RouteCondition(startInfo);
		RouteCondition rcFromHint = SimpleHintParser.convertHint2RouteCondition(startInfo);
		// 如果hint中没有指定任何指示性的信息，走原来逻辑
		// TDDL Hint和ThreadLocal中指定的数据以TDDL Hint优先
		if (rcFromHint == null) {
			DirectlyRouteCondition condition = pipelineFactory.sqlPreParse(startInfo.getSql());
			if (null == condition) {
				dataBus = this.bootstrap0(startInfo);
			} else {
				dataBus = this.bootstrap0Direct(startInfo, condition);
			}
		} else {
			//TDDL hint完成使命，移除，防止sql解析和后续操作出错
			HintParserHelper.removeTddlHintAndParameter(startInfo);
			//如果hint中有指示性信息，则不需要进行SQL预解析
			dataBus = this.bootstrap0(startInfo,rcFromHint);
		}

		return getExecutionPlan(dataBus);
	}

	public void bootstrapForBatch(StartInfo startInfo,
			boolean needRowCopy, Map<String, List<String>> targetSqls,
			String selectKey) throws SQLException {
		this.bootstrap0ForBatch(startInfo,selectKey,false,
				targetSqls, null);
	}

	public void bootstrapForPrepareBatch(StartInfo startInfo, boolean needRowCopy,
			Map<String, Map<String, List<List<ParameterContext>>>> targetSqls,
			String selectKey) throws SQLException {
		this.bootstrap0ForBatch(startInfo,
				selectKey, true, null, targetSqls);
	}

	public DispatcherResult bootstrapForGetDBAndTabs(RouteCondition rc,
			SqlDispatcher sqlDispatcher) throws SQLException {
		DataBus dataBus = this.bootstrap0ForGetDBAndTabs(rc, null, null,
				sqlDispatcher);
		return getDispatcherResult(dataBus);
	}

	public DispatcherResult bootstrapForGetDBAndTabs(String sql,
			List<Object> args, SqlDispatcher sqlDispatcher) throws SQLException {
		DataBus dataBus = this.bootstrap0ForGetDBAndTabs(null, sql, args,
				sqlDispatcher);
		return getDispatcherResult(dataBus);
	}

	/**
	 * 从总线中取得DispatcherResult
	 * 
	 * @param dataBus
	 * @return
	 */
	private DispatcherResult getDispatcherResult(DataBus dataBus) {
		PipelineRuntimeInfo runtime = (PipelineRuntimeInfo) dataBus
				.getPluginContext(PipelineRuntimeInfo.INFO_NAME);
		return runtime.getMetaData();
	}

	/**
	 * 从总线中取得ExecutionPlan
	 * 
	 * @param dataBus
	 * @return
	 */
	private ExecutionPlan getExecutionPlan(DataBus dataBus) {
		PipelineRuntimeInfo runtime = (PipelineRuntimeInfo) dataBus
				.getPluginContext(PipelineRuntimeInfo.INFO_NAME);
		ExecutionPlan executionPlan = runtime.getExecutionPlan();
		if(dataBus.getSqlMetaData() != null){
			executionPlan.setSqlMetaData(dataBus.getSqlMetaData());
		}
		return executionPlan;
	}

	/**
	 * 兼容性方法,避免修改过多的测试
	 * 
	 * @param rc
	 * @param sql
	 * @param args
	 * @return
	 * @throws SQLException
	 */
	private DataBus bootstrap0ForGetDBAndTabs(RouteCondition rc, String sql,
			List<Object> args, SqlDispatcher sqlDispatcher) throws SQLException {
		StartInfo startInfo = new StartInfo();

		FlowType flowType = null;
		if (null != rc) {
			flowType = FlowType.DBANDTAB_RC;
			startInfo.setRc(rc);
		} else {
			flowType = FlowType.DBANDTAB_SQL;
		}

		DataBus dataBus = new PipelineContextDataBus();
		PipelineRuntimeInfo runtime = new PipelineRuntimeInfo();

		dataBus.registerPluginContext(PipelineRuntimeInfo.INFO_NAME, runtime);

		startInfo.setSql(sql);
		startInfo.setDbType(pipelineFactory.decideDBType(sql, sqlDispatcher));
		startInfo.setSqlArgs(args);
		runtime.setFlowType(flowType);
		runtime.setStartInfo(startInfo);
		runtime.setSqlDispatcher(sqlDispatcher);

		Pipeline pipeline = pipelineFactory.getPipeline();
		pipeline.startFlow(dataBus);
		return dataBus;
	}

	/**
	 * 专门提供给批量sql使用
	 * 
	 * @param sql
	 * @param originalParameterSettings
	 * @param sqlType
	 * @param needRowCopy
	 * @param selectKey
	 * @param isPreparedBatch
	 * @param targetSqls
	 * @param prePareTargetSqls
	 * @return
	 * @throws SQLException
	 */
	private DataBus bootstrap0ForBatch(
			StartInfo startInfo,
			String selectKey,
			boolean isPreparedBatch,
			Map<String, List<String>> targetSqls,
			Map<String, Map<String, List<List<ParameterContext>>>> prePareTargetSqls)
			throws SQLException {
		// Batch支持ThreadLocal,设置参数位置.
		SimpleCondition rc = (SimpleCondition) getRouteContiongFromThreadLocal(ThreadLocalString.ROUTE_CONDITION);
		DirectlyRouteCondition condition = pipelineFactory
				.sqlPreParse(startInfo.getSql());
		FlowType flowType = null;
		if (rc != null) {
			List<Object> params = getSqlParameters(startInfo.getSqlParam());
			Map<String, Integer> paramIndexs = rc.getParametersIndexForBatch();

			for (Map.Entry<String, Integer> index : paramIndexs.entrySet()) {
				rc.put(index.getKey(),
						(Comparable<?>) params.get(index.getValue()));
			}
			flowType = FlowType.BATCH_NOSQLPARSER;
		} else if (condition != null) {
			flowType = FlowType.BATCH_DIRECT;
		} else {
			flowType = FlowType.BATCH;
		}

		DataBus dataBus = getPluginDataBus(startInfo,
				connectionManager.getAutoCommit(), flowType, selectKey, null,
				condition);

		PipelineRuntimeInfo runtime = (PipelineRuntimeInfo) dataBus
				.getPluginContext(PipelineRuntimeInfo.INFO_NAME);
		runtime.getStartInfo().setParameterBatch(isPreparedBatch);
		runtime.getStartInfo().setTargetSqls(prePareTargetSqls);
		runtime.getStartInfo().setRc(rc);
		runtime.getStartInfo().setTargetSqlsNoParameter(targetSqls);

		Pipeline pipeline = pipelineFactory.getPipeline();
		pipeline.startFlow(dataBus);
		return dataBus;
	}
	
	private List<Object> getSqlParameters(Map<Integer, ParameterContext> sqlParam) {
		if (sqlParam != null) {
			List<Object> parameters = new ArrayList<Object>();
			for (ParameterContext context : sqlParam.values()) {
				if (context.getParameterMethod() != ParameterMethod.setNull1
						&& context.getParameterMethod() != ParameterMethod.setNull2) {
					parameters.add(context.getArgs()[1]);
				} else {
					parameters.add(null);
				}
			}
			return parameters;
		} else {
			return Collections.emptyList();
		}
	}

	private DataBus bootstrap0(StartInfo startInfo) throws SQLException {
		return bootstrap0(startInfo, null);
	}

	/**
	 * 给普通查询更新使用
	 * 
	 * @param sql
	 * @param originalParameterSettings
	 * @param sqlType
	 * @param needRowCopy
	 * @return
	 * @throws SQLException
	 */
	private DataBus bootstrap0(StartInfo startInfo, RouteCondition fromHintRc) throws SQLException {
		RouteCondition rc = null;
		DirectlyRouteCondition ruleCondition = null;
		DirectlyRouteCondition directlyRouteCondition = null;
		//如果TDDL HINT为空，那么走原来的路
		if (null == fromHintRc) {
			rc = (RouteCondition) getRouteContiongFromThreadLocal(ThreadLocalString.ROUTE_CONDITION);
			ruleCondition = (DirectlyRouteCondition) getRouteContiongFromThreadLocal(ThreadLocalString.RULE_SELECTOR);
			directlyRouteCondition = (DirectlyRouteCondition) getRouteContiongFromThreadLocal(ThreadLocalString.DB_SELECTOR);
		} else {
			if (fromHintRc instanceof DirectlyRouteCondition) {
                //FIXME:如何区分这个ruleCondition还是directlyRouteCondition,现在默认处理为directlyRouteCondition
				//也就是不支持多套规则的选择
				directlyRouteCondition=(DirectlyRouteCondition) fromHintRc;
			} else {
				rc = fromHintRc;
			}
		}
		
		FlowType flowType;
		String ruleId = null;
		SqlMetaData sqlMetaData = null;
		if (directlyRouteCondition != null) {
			String dbRuleId = ((DirectlyRouteCondition) directlyRouteCondition)
					.getDbRuleID();
			if (connectionManager.containDBIndex(dbRuleId)) {
				flowType = FlowType.DIRECT; // 直接执行sql，不做解析和路由
				sqlMetaData = generateSqlMetaData(startInfo.getSql());
			} else {
				throw new SQLException("找不到目标执行库: " + dbRuleId);
			}
		} else if (ruleCondition != null) {
			ruleId = ((DirectlyRouteCondition) ruleCondition).getDbRuleID();
			if (rc != null) {
				flowType = FlowType.NOSQLPARSE; // 选择规则，并且跳过sql解析
				sqlMetaData = generateSqlMetaData(startInfo.getSql(), rc.getVirtualTableName());
			} else {
				flowType = FlowType.DEFAULT; // 选择规则，并且默认执行
			}
		} else if (null != rc) {
			flowType = FlowType.NOSQLPARSE; // 不选择规则，但是跳过sql解析
			sqlMetaData = generateSqlMetaData(startInfo.getSql(), rc.getVirtualTableName());
		} else {
			flowType = FlowType.DEFAULT; // 既不选择规则，也不跳过sql解析，默认执行
		}

		PipelineContextDataBus dataBus = getPluginDataBus(startInfo,
				connectionManager.getAutoCommit(), flowType, ruleId,
				rc, directlyRouteCondition);
		Pipeline pipeline = pipelineFactory.getPipeline();

		pipeline.startFlow(dataBus);
		
		if(flowType != FlowType.DEFAULT){
			sqlMetaData = generateSqlMetaData(startInfo.getSql());
			dataBus.setSqlMetaData(sqlMetaData);
		}

		return dataBus;
	}
	
	private SqlMetaData generateSqlMetaData(String sql, String... logicTables){
		SqlMetaData sqlMetaData = SqlMetaDataFactory.getSqlMetaData(sql, logicTables);
		return sqlMetaData;
	}

	/**
	 * 直接执行sql，主要提供给sql预解析后发现规则中没有 定义该表的规则，那么直接在defaultDbIndex上执行SQL
	 * 
	 * @param sql
	 * @param originalParameterSettings
	 * @param sqlType
	 * @param condition
	 * @return
	 * @throws SQLException
	 */
	private DataBus bootstrap0Direct(StartInfo startInfo, DirectlyRouteCondition condition)
			throws SQLException {
		FlowType flowType = null;
		if (condition != null) {
			String dbRuleId = ((DirectlyRouteCondition) condition)
					.getDbRuleID();

			if (connectionManager.containDBIndex(dbRuleId)) {
				flowType = FlowType.DIRECT;
			} else {
				throw new SQLException("找不到目标执行库: " + dbRuleId);
			}
		}

		PipelineContextDataBus dataBus = getPluginDataBus(startInfo,
				connectionManager.getAutoCommit(), flowType, null,
				null, condition);
		Pipeline pipeline = pipelineFactory.getPipeline();
		pipeline.startFlow(dataBus);
		
		if(flowType == FlowType.DIRECT){
			dataBus.setSqlMetaData(generateSqlMetaData(startInfo.getSql()));
		}

		return dataBus;
	}

	/**
	 * 获取一条数据总线，这条总线中注册自定义配置，可注册多个。 每次初始化一条数据总线，应对多线程问题。 add by junyu
	 * 
	 * @return
	 * @throws SQLException
	 */
	protected PipelineContextDataBus getPluginDataBus(StartInfo startInfo, boolean autoCommit,
		    FlowType flowType, String selectKey,
			RouteCondition rc, DirectlyRouteCondition directlyRouteCondition)
			throws SQLException {
		PipelineContextDataBus dataBus = new PipelineContextDataBus();
		PipelineRuntimeInfo runtime = new PipelineRuntimeInfo();

		dataBus.registerPluginContext(PipelineRuntimeInfo.INFO_NAME, runtime);
        if(startInfo==null){
		    startInfo = new StartInfo();
        }
		startInfo.setDbType(pipelineFactory.decideDBType(startInfo.getSql(),
				pipelineFactory.selectSqlDispatcher(selectKey)));
		startInfo.setAutoCommit(autoCommit);
		startInfo.setRc(rc);
		startInfo.setDirectlyRouteCondition(directlyRouteCondition);
		runtime.setStartInfo(startInfo);
		runtime.setFlowType(flowType);
		runtime.setSqlDispatcher(pipelineFactory.selectSqlDispatcher(selectKey));
		return dataBus;
	}

	/**
	 * 从threadLocal中取得RouteCondition,并且按需求清除ThreadLocalMap里面的值
	 * 
	 * @param key
	 * @return
	 */
	protected RouteCondition getRouteContiongFromThreadLocal(String key) {
		RouteCondition rc = (RouteCondition) ThreadLocalMap.get(key);
		if (rc != null) {
			ROUTE_TYPE routeType = rc.getRouteType();
			if (ROUTE_TYPE.FLUSH_ON_EXECUTE.equals(routeType)) {
				ThreadLocalMap.put(key, null);
			}
		}
		return rc;
	}
}
