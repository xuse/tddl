//Copyright(c) Taobao.com
package com.taobao.tddl.client.handler.validate;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.client.controller.ControllerUtils;
import com.taobao.tddl.client.controller.DatabaseExecutionContext;
import com.taobao.tddl.client.controller.DispatcherResultImp;
import com.taobao.tddl.client.controller.OrderByMessagesImp;
import com.taobao.tddl.client.databus.DataBus;
import com.taobao.tddl.client.databus.PipelineRuntimeInfo;
import com.taobao.tddl.client.dispatcher.DispatcherResult;
import com.taobao.tddl.client.dispatcher.EXECUTE_PLAN;
import com.taobao.tddl.client.dispatcher.MultiLogicTableNames;
import com.taobao.tddl.client.handler.AbstractHandler;
import com.taobao.tddl.interact.bean.MatcherResult;
import com.taobao.tddl.interact.rule.bean.DBType;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.sqlobjecttree.OrderByEle;
import com.taobao.tddl.sqlobjecttree.SqlParserResult;

/**
 * @description 此handler主要功能就是中间结果的校验,包括限制多库多表和单库多表的
 *              GroupBy,Having,有条件DISTINCT支持(因为性能和内存安全原因只有
 *              规则配置completeDistinct属性才会完全打开Distinct多表支持)
 * 
 *              如果可以反向输出,那么做反向输出,包括需要数据复制时加入version
 *              或者变更version, limit m,n中m与n的变换(多库与单库有本质的不同), 
 *              表名替换等行为.
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.4
 * @since 1.6
 * @date 2010-09-05下午01:33:32
 */
@SuppressWarnings("unchecked")
public class SqlDispatchHandler extends AbstractHandler {
	public static final String HANDLER_NAME = "SqlDispatchHandler";
	private static final Log log = LogFactory.getLog(SqlDispatchHandler.class);

	public void handleDown(DataBus dataBus) throws SQLException {
		FlowType flowType = getPipeLineRuntimeInfo(dataBus).getFlowType();
		if (FlowType.DEFAULT == flowType || FlowType.NOSQLPARSE == flowType
				|| FlowType.BATCH == flowType
				|| FlowType.BATCH_NOSQLPARSER == flowType
				|| FlowType.DBANDTAB_RC == flowType
				|| FlowType.DBANDTAB_SQL == flowType) {
			dispatch(dataBus);
		}
	}

	protected void dispatch(DataBus dataBus) throws SQLException {
		PipelineRuntimeInfo runtime = super.getPipeLineRuntimeInfo(dataBus);
		MatcherResult matcherResult = runtime.getMatcherResult();
		List<DatabaseExecutionContext> databaseExecutionContexts = runtime
				.getDataBaseExecutionContext();

		SqlParserResult sqlParserResult = runtime.getSqlParserResult();
		List<Object> sqlParameters = runtime.getStartInfo().getSqlParameters();
		DBType dbType = runtime.getStartInfo().getDbType();
		boolean allowReverseOutput = runtime.isAllowReverseOutput();
		boolean isSqlParsed = runtime.getIsSqlParsed();
		boolean needRowCopy = runtime.isNeedRowCopy();
		List<String> uniqueColumns = runtime.getUniqueColumns();
		boolean completeDistinct = runtime.isCompleteDistinct();

		DispatcherResult metaData = null;

		if (null != matcherResult) {
			metaData = getDispatcherResult(databaseExecutionContexts,
					matcherResult, sqlParserResult, sqlParameters, dbType,
					uniqueColumns, needRowCopy, allowReverseOutput,
					isSqlParsed, completeDistinct);
		} else {
			metaData = getDispatcherResult(databaseExecutionContexts, null,
					sqlParserResult, sqlParameters, dbType,
					Collections.EMPTY_LIST, false, allowReverseOutput,
					isSqlParsed, completeDistinct);
		}

		setResult(metaData, runtime);

		debugLog(log, new Object[] { "sql dispatch end." });
	}

	/**
	 * 根据匹配结果，进行最终给TStatement的结果的拼装,不同的matcher可以共用
	 * 
	 * @param matcherResult
	 * @return
	 */
	protected DispatcherResult getDispatcherResult(
			List<DatabaseExecutionContext> databaseExecutionContexts,
			MatcherResult matcherResult, SqlParserResult sqlParserResult,
			List<Object> args, DBType dbType, List<String> uniqueColumnSet,
			boolean isNeedRowCopy, boolean isAllowReverseOutput,
			boolean isSqlParser, boolean completeDistinct) {

		DispatcherResultImp dispatcherResult = getTargetDatabaseMetaDataBydatabaseGroups(
				databaseExecutionContexts, sqlParserResult, args,
				isNeedRowCopy, isAllowReverseOutput);

		// 虽然判断sql输入输出的逻辑应该放到规则里，但因为觉得没必要在走了规则以后就放在TargetDBList里面多传递一次
		// 在这里搞一次就可以了

		ControllerUtils.buildExecutePlan(dispatcherResult,
				databaseExecutionContexts);

		// Group,Distinct,Having走多库或者多表情况下都不能通过
		validGroupByFunction(sqlParserResult, dispatcherResult);
		validHavingByFunction(sqlParserResult, dispatcherResult);

		// 完全打开distinct支持的话就不做多库多表或者单库多表限制
		if (!completeDistinct) {
			validDistinctByFunction(sqlParserResult, dispatcherResult);
		}

		if (isSqlParser) {
			// 做过sql parse才有可能做反向输出
			ControllerUtils.buildReverseOutput(args, sqlParserResult,
					dispatcherResult, DBType.MYSQL.equals(dbType));
		}

		if (dispatcherResult.needRowCopy()) {

			// @Important 这里注意一定不能点到次序，否则会出现数据复制的sql中重复出现相同结果的列的问题
			buildUniqueKeyToBeReturn(sqlParserResult, args, uniqueColumnSet,
					dispatcherResult);

			if (matcherResult != null) {

				ControllerUtils.appendDatabaseSharedMetaData(
						matcherResult.getDatabaseComparativeMap(),
						dispatcherResult);
				ControllerUtils.appendTableSharedMetaData(
						matcherResult.getTableComparativeMap(),
						dispatcherResult);
				// @Important end
			}
		}

		return dispatcherResult;
	}

	protected DispatcherResultImp getTargetDatabaseMetaDataBydatabaseGroups(
			List<DatabaseExecutionContext> targetDatabases,
			SqlParserResult sqlParserResult, List<Object> arguments,
			boolean isNeedRowCopy, boolean isAllowReverseOutput) {
		MultiLogicTableNames logicTablename = new MultiLogicTableNames();
		logicTablename.setLogicTables(sqlParserResult.getTableName());
		// targetDatabase.set
		DispatcherResultImp dispatcherResultImp = new DispatcherResultImp(
				logicTablename, targetDatabases, isNeedRowCopy,
				isAllowReverseOutput, sqlParserResult.getSkip(arguments),
				sqlParserResult.getMax(arguments), new OrderByMessagesImp(
						sqlParserResult.getOrderByEles()),
				sqlParserResult.getGroupFuncType(),
				sqlParserResult.getDistinctColumn());
		return dispatcherResultImp;
	}

	/**
	 * 如果有groupby函数并且执行计划为单库单表或单库无表或无库无表 则允许通过
	 * 
	 * @param sqlParserResult
	 * @param dispatcherResult
	 */
	protected void validGroupByFunction(SqlParserResult sqlParserResult,
			DispatcherResult dispatcherResult) {
		List<OrderByEle> groupByElement = sqlParserResult.getGroupByEles();
		if (groupByElement.size() != 0) {
			if (dispatcherResult.getDatabaseExecutePlan() == EXECUTE_PLAN.MULTIPLE) {
				throw new IllegalArgumentException("多库的情况下，不允许使用group by 函数");
			}
			if (dispatcherResult.getTableExecutePlan() == EXECUTE_PLAN.MULTIPLE) {
				throw new IllegalArgumentException("多表的情况下，不允许使用group by函数");
			}
		}
	}

	/**
	 * 走多库或者多表不允许使用Distinct
	 * 
	 * @param sqlParserResult
	 * @param dispatcherResult
	 */
	protected void validDistinctByFunction(SqlParserResult sqlParserResult,
			DispatcherResult dispatcherResult) {
		List<String> dc = sqlParserResult.getDistinctColumn();
		if (dc != null && dc.size() != 0) {
			if (dispatcherResult.getDatabaseExecutePlan() == EXECUTE_PLAN.MULTIPLE) {
				throw new IllegalArgumentException("多库的情况下，不允许使用Distinct关键字");
			}
			if (dispatcherResult.getTableExecutePlan() == EXECUTE_PLAN.MULTIPLE) {
				throw new IllegalArgumentException("多表的情况下，不允许使用Distinct关键字");
			}
		}
	}

	/**
	 * 走多库或者多表不允许使用Having
	 * 
	 * @param sqlParserResult
	 * @param dispatcherResult
	 */
	protected void validHavingByFunction(SqlParserResult sqlParserResult,
			DispatcherResult dispatcherResult) {
		boolean having = sqlParserResult.hasHavingCondition();
		if (having) {
			if (dispatcherResult.getDatabaseExecutePlan() == EXECUTE_PLAN.MULTIPLE) {
				throw new IllegalArgumentException("多库的情况下，不允许使用Having关键字");
			}
			if (dispatcherResult.getTableExecutePlan() == EXECUTE_PLAN.MULTIPLE) {
				throw new IllegalArgumentException("多表的情况下，不允许使用Having关键字");
			}
		}
	}

	protected void buildUniqueKeyToBeReturn(SqlParserResult sqlParserResult,
			List<Object> args, List<String> uniqueColumnSet,
			DispatcherResultImp dispatcherResult) {
		Set<String> tempSet = new HashSet<String>(1);
		for (String str : uniqueColumnSet) {
			tempSet.clear();
			tempSet.add(str);
			Map<String, Comparative> uniqueMap = sqlParserResult
					.getComparativeMapChoicer().getColumnsMap(args, tempSet);
			if (!uniqueMap.isEmpty()) {
				ControllerUtils.appendUniqueKeysMetaData(uniqueMap,
						dispatcherResult);
			}
		}
	}

	private void setResult(DispatcherResult metaData,
			PipelineRuntimeInfo runtime) {
		runtime.setMetaData(metaData);
	}
}
