//Copyright(c) Taobao.com
package com.taobao.tddl.client.handler.executionplan;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.common.lang.StringUtil;
import com.taobao.tddl.client.controller.DatabaseExecutionContext;
import com.taobao.tddl.client.databus.DataBus;
import com.taobao.tddl.client.databus.PipelineRuntimeInfo;
import com.taobao.tddl.client.dispatcher.DispatcherResult;
import com.taobao.tddl.client.dispatcher.SqlDispatcher;
import com.taobao.tddl.client.handler.AbstractHandler;
import com.taobao.tddl.client.jdbc.RealSqlContext;
import com.taobao.tddl.client.jdbc.RealSqlContextImp;
import com.taobao.tddl.client.jdbc.SqlExecuteEvent;
import com.taobao.tddl.client.jdbc.SqlExecuteEventUtil;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlanImp;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;
import com.taobao.tddl.interact.bean.Field;
import com.taobao.tddl.interact.bean.ReverseOutput;
import com.taobao.tddl.interact.rule.bean.SqlType;
import com.taobao.tddl.sqlobjecttree.DMLCommon;
import com.taobao.tddl.sqlobjecttree.InExpressionObject;
import com.taobao.tddl.sqlobjecttree.SqlParserResult;

/**
 * @description 生成最终的执行计划,主要做的工作是生成RealSqlContext Map(即那些库执行哪些sql),
 *              设置isGoSlave(多库事务判定标准之一),Limit M,N属性(skip,max),聚合函数实例, order
 *              by实例,数据复制上下文(setEvent())等属性.
 * 
 *              tips: 1.生成RealSqlContext包括带参和不带参2种形式,
 *              2.根据是否反向输出属性决定是否使用sql解析的反向输出, 如果不反向输出,那么进行表名替换
 *              3.根据needIdInGroup配置决定是否对id in形式的sql进行 归组优化,这对id
 *              in包含较多分散id的sql有较好的性能 提升.
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.3
 * @since 1.6
 * @date 2010-12-6下午03:40:50
 */
public class ExecutionPlanHandler extends AbstractHandler {
	public static final String HANDLER_NAME = "ExecutionPlanHandler";
	private static final Log log = LogFactory
			.getLog(ExecutionPlanHandler.class);

	public void handleDown(DataBus dataBus) throws SQLException {
		if (FlowType.DEFAULT == getPipeLineRuntimeInfo(dataBus).getFlowType()
				|| FlowType.NOSQLPARSE == getPipeLineRuntimeInfo(dataBus)
						.getFlowType()) {
			makeUp(dataBus);
		}
	}

	/**
	 * 生成ExecutionPlan对象.
	 * 
	 * @param dataBus
	 * @throws SQLException
	 */
	private void makeUp(DataBus dataBus) throws SQLException {
		PipelineRuntimeInfo runtime = super.getPipeLineRuntimeInfo(dataBus);
		List<String> virtualJoinTableNames = runtime.getVirtualJoinTableNames();
		DispatcherResult metaData = runtime.getMetaData();
		boolean isAutoCommit = runtime.getStartInfo().isAutoCommit();
		Map<Integer, ParameterContext> parameterSettings = runtime
				.getStartInfo().getSqlParam();
		SqlDispatcher sqlDispatcher = runtime.getSqlDispatcher();
		String originalSql = runtime.getStartInfo().getSql();
		SqlType sqlType = runtime.getStartInfo().getSqlType();
		boolean needRowCopy = runtime.isNeedRowCopy();
		SqlParserResult spr = runtime.getSqlParserResult();
		boolean needIdInGroup = runtime.isNeedIdInGroup();

		/**
		 * 如果在SqlParserHandler中沒有設置virtualJoinTableNames 那么这个设置是没用的。
		 */
		metaData.setVirtualJoinTableNames(virtualJoinTableNames);

		// 目标库和表bean
		List<DatabaseExecutionContext> targets = metaData
				.getDataBaseExecutionContexts();

		ExecutionPlanImp executionPlan = new ExecutionPlanImp();

		// FIXME:先不支持mapping rule
		if (targets == null || targets.isEmpty()) {
			throw new SQLException("找不到目标库，请检查配置");
		} else {
			buildExecutionContext(originalSql, executionPlan, sqlType,
					metaData, targets, sqlDispatcher, parameterSettings,
					isAutoCommit, needRowCopy, spr, needIdInGroup);
		}

		executionPlan.setUseParallel(getUseParallelFromThreadLocal());

		setResult(executionPlan, runtime);

		debugLog(log, new Object[] { "sql dispatch end." });
	}

	/**
	 * 构建最终的执行计划
	 * 
	 * @param originalSql
	 * @param executionPlanImp
	 * @param sqlType
	 * @param metaData
	 * @param targets
	 * @param sqlDispatcher
	 * @param parameterSettings
	 * @param isAutoCommit
	 * @param needRowCopy
	 * @throws SQLException
	 */
	private void buildExecutionContext(String originalSql,
			ExecutionPlanImp executionPlanImp, SqlType sqlType,
			DispatcherResult metaData, List<DatabaseExecutionContext> targets,
			SqlDispatcher sqlDispatcher,
			Map<Integer, ParameterContext> parameterSettings,
			boolean isAutoCommit, boolean needRowCopy, SqlParserResult spr,
			boolean needIdInGroup) throws SQLException {
		int size = targets.size();

		Map<String/* dbIndex */, List<RealSqlContext>> sqlMap = new HashMap<String, List<RealSqlContext>>(
				size);

		// 拼装返回的结果
		for (DatabaseExecutionContext target : targets) {
			// 数据库dbSelectorId
			String dbSelectorId = target.getDbIndex();
			List<Map<String, String>> actualTables = target.getTableNames();

			printLog(dbSelectorId, actualTables);

			// valid
			if (dbSelectorId == null || dbSelectorId.length() < 1) {
				throw new SQLException("invalid dbSelectorId:" + dbSelectorId);
			}

			if (actualTables == null || actualTables.isEmpty()) {
				throw new SQLException("找不到目标表");
			}

			List<RealSqlContext> sqlContext = null;

			// 这个List保证不为null,多个in的情况下按原始的方式走(一般不会出现),
			// 另外id in归组优化默认不使用,需要使用可以在ShardRule bean中配置
			// needIdInGroup属性
			if (needIdInGroup && spr.getInExpressionObjectList().size() == 1) {
				debugLog(log, new Object[] { "use id in group!columnName:",
						spr.getInExpressionObjectList().get(0).columnName });
				sqlContext = buildDBRealSqlContextWithInReplace(originalSql,
						metaData, parameterSettings, target, spr
								.getInExpressionObjectList().get(0));
			} else {
				sqlContext = buildDBRealSqlContext(originalSql, metaData,
						parameterSettings, target);
			}
			// put dbIndex ->sql
			sqlMap.put(dbSelectorId, sqlContext);
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
		executionPlanImp.setVirtualTableName(metaData.getVirtualTableName());
		executionPlanImp.setEvents(createEvent(metaData, sqlType, originalSql,
				needRowCopy));
		// 这里需要注意的
		// boolean needRetry = (sqlDispatcher == writeDispatcher?false:true);
		// modified by shenxun：去掉了事务判断，isAutoCommit不需要进行判断，后面会针对不同的sql进行适当判断
		boolean goSlave = SqlType.SELECT.equals(sqlType);
		executionPlanImp.setGoSlave(goSlave);
		executionPlanImp.setOriginalSql(originalSql);
		executionPlanImp.setDistinctColumns(metaData.getDistinctColumns());
	}

	/**
	 * 填入数据
	 * 
	 * @param originalSql
	 * @param metaData
	 * @param parameterSettings
	 * @param target
	 * @param actualTables
	 * @param sqlContext
	 * @throws SQLException
	 */
	private List<RealSqlContext> buildDBRealSqlContext(String originalSql,
			DispatcherResult metaData,
			Map<Integer, ParameterContext> parameterSettings,
			DatabaseExecutionContext target) throws SQLException {
		List<RealSqlContext> sqlContext = new ArrayList<RealSqlContext>(target
				.getTableNames().size());
		List<Map<String/* logic table */, String/* actual table */>> actualTables = target
				.getTableNames();

		if (actualTables == null || actualTables.isEmpty()) {
			throw new SQLException("找不到目标表");
		}

		// 循环填入数据
		if (!metaData.allowReverseOutput()) {
			for (Map<String, String> tab : actualTables) {
				RealSqlContextImp realSqlContext = new RealSqlContextImp();
				sqlContext.add(realSqlContext);
				String sql = replaceMultiTableName(originalSql, tab);

				// realSqlContext.setSql();
				// 如果metaData(也就是DispatcherResult)里面有join表名，那么就替换掉;
				// sql = replaceJoinTableName(metaData.getVirtualTableName()
				// .toString(), metaData.getVirtualJoinTableNames(), tab,
				// sql,log);
				realSqlContext.setRealTable(tab.values().toString());
				realSqlContext.setSql(sql);
				realSqlContext.setArgument(parameterSettings);
				// 打印下最终会执行的sql和参数,方便查找问题
				debugLog(
						log,
						new Object[] {
								"use table replace,one of final to be executed sql is:",
								sql, ";final parameter is:", parameterSettings });
			}
		} else {
			List<ReverseOutput> sqlInfos = target.getOutputSQL();
			if (sqlInfos == null || sqlInfos.isEmpty()) {
				throw new SQLException("找不到目标表");
			}

			// 替换
			// TODO: 这里需要重构，将整个替换表名的过程顺流过来，目前没精力
			Map<Integer, Object> changedParameters = sqlInfos.get(0)
					.getParams();
			changeParameters(changedParameters, parameterSettings);

			for (ReverseOutput sqlInfo : sqlInfos) {
				RealSqlContextImp realSqlContext = new RealSqlContextImp();
				sqlContext.add(realSqlContext);
//				realSqlContext.setSql(sqlInfo.getSql());
				String sql = replaceMultiTableName(sqlInfo.getSql(), new HashMap<String, String>());
				realSqlContext.setSql(sql);
				realSqlContext.setRealTable(sqlInfo.getTable());
				realSqlContext.setArgument(parameterSettings);
				debugLog(
						log,
						new Object[] {
								"use reverse output,one of final to be executed sql is:",
								sqlInfo.getSql(), ";final parameter is:",
								parameterSettings });
			}

			// 因为所有SQL绑定参数都一样，所以只要取第一个。
		}
		return sqlContext;
	}

	/**
	 * 具有id in归组功能的RealSqlContext构建方法
	 * 
	 * @param originalSql
	 * @param metaData
	 * @param parameterSettings
	 * @param target
	 * @param in
	 * @return
	 * @throws SQLException
	 */
	private List<RealSqlContext> buildDBRealSqlContextWithInReplace(
			String originalSql, DispatcherResult metaData,
			Map<Integer, ParameterContext> parameterSettings,
			DatabaseExecutionContext target, InExpressionObject in)
			throws SQLException {
		List<RealSqlContext> sqlContext = new ArrayList<RealSqlContext>(target
				.getTableNames().size());
		List<Map<String/* logic table */, String/* actual table */>> actualTables = target
				.getTableNames();

		if (actualTables == null || actualTables.isEmpty()) {
			throw new SQLException("找不到目标表");
		}

		// 循环填入数据
		if (!metaData.allowReverseOutput()) {
			for (Map<String, String> tab : actualTables) {
				RealSqlContextImp realSqlContext = new RealSqlContextImp();
				sqlContext.add(realSqlContext);
				String sql = replaceMultiTableName(originalSql, tab);
				Map<Integer, ParameterContext> replacedParameterSettings = parameterSettings;
				// 只对prepared statement形式进行处理
				if (in.bindVarIndexs != null && in.bindVarIndexs.size() > 0) {
					if (in.bindVarValues != null && in.bindVarValues.size() > 0) {
						throw new SQLException(
								"id in group feature not support inExpression with both '?' and real value!");
					}
					sql = changePrepareStatementSql(sql,
							StringUtil.substringBetween(
									tab.values().toString(), "[", "]"),
							target.getRealTableFieldMap(), in);

					replacedParameterSettings = changeParameterContext(
							parameterSettings, StringUtil.substringBetween(tab
									.values().toString(), "[", "]"),
							target.getRealTableFieldMap(), in);
				}
				// statement形式不做处理
				realSqlContext.setRealTable(tab.values().toString());
				
				realSqlContext.setSql(sql);
				realSqlContext.setArgument(replacedParameterSettings);
				// 打印下最终会执行的sql和参数,方便查找问题
				debugLog(
						log,
						new Object[] {
								"use table replace,one of final to be executed sql is:",
								sql, ";final parameter is:",
								replacedParameterSettings });
			}
		} else {
			List<ReverseOutput> sqlInfos = target.getOutputSQL();
			if (sqlInfos == null || sqlInfos.isEmpty()) {
				throw new SQLException("找不到目标表");
			}

			// 替换
			// TODO: 这里需要重构，将整个替换表名的过程顺流过来，目前没精力
			Map<Integer, Object> changedParameters = sqlInfos.get(0)
					.getParams();
			changeParameters(changedParameters, parameterSettings);

			for (ReverseOutput sqlInfo : sqlInfos) {
				Map<Integer, ParameterContext> replacedParameterSettings = parameterSettings;
				String sql = sqlInfo.getSql();
				// 只对prepared statement形式进行处理
				if (in.bindVarIndexs != null) {
					sql = changePrepareStatementSql(sqlInfo.getSql(),
							getReverseOutPutRealTable(sqlInfo.getTable()),
							target.getRealTableFieldMap(), in);

					replacedParameterSettings = changeParameterContext(
							parameterSettings,
							getReverseOutPutRealTable(sqlInfo.getTable()),
							target.getRealTableFieldMap(), in);
				}
				// statement形式不做处理
				RealSqlContextImp realSqlContext = new RealSqlContextImp();
				sqlContext.add(realSqlContext);
//				realSqlContext.setSql(sql);
				String newSql = replaceMultiTableName(sql, new HashMap<String, String>());
				realSqlContext.setSql(newSql);
				realSqlContext.setRealTable(sqlInfo.getTable());
				realSqlContext.setArgument(replacedParameterSettings);
				// 打印下最终会执行的sql和参数,方便查找问题
				debugLog(
						log,
						new Object[] {
								"use reverse output,one of final to be executed sql is:",
								sql, ";final parameter is:",
								replacedParameterSettings });
			}
		}
		return sqlContext;
	}

	private String getReverseOutPutRealTable(String tableMapStr) {
		String str = StringUtil.substringBetween(tableMapStr, "{", "}");
		return StringUtil.split(str, "=")[1];
	}

	/**
	 * 只对prepareStatement形式的sql切分用这种方式. 这里可能会有性能问题.
	 */
//	private static String patternStr = "in\\s*\\((\\s*\\?\\s*)?(,\\s*\\?\\s*)*\\)\\s*";
	private static String patternStr="in\\s*\\(.*?\\)\\s*";
	private static Pattern inpattern = Pattern.compile(patternStr);

	/**
	 * id in 下sql变更,根据参数个数
	 * 
	 * @param sql
	 * @param realTable
	 * @param filedMap
	 * @param in
	 * @return
	 * @throws SQLException
	 */
	private String changePrepareStatementSql(String sql, String realTable,
			Map<String, Field> filedMap, InExpressionObject in)
			throws SQLException {
		Field f = filedMap.get(realTable);
		if (null == f || f.equals(Field.EMPTY_FIELD)) {
			return sql;
		}
		Set<Object> sourceValues = f.sourceKeys
				.get(in.columnName.toUpperCase());
		if (null == sourceValues) {
			return sql;
		}

		String[] sqlPieces = inpattern.split(sql.toLowerCase());
		StringBuilder replacedSql = new StringBuilder();
		if (null != sqlPieces && sqlPieces.length == 1) {
			appendPrepareStatementSql(replacedSql, sqlPieces,
					sourceValues.size());
		} else if (null != sqlPieces && sqlPieces.length == 2) {
			appendPrepareStatementSql(replacedSql, sqlPieces,
					sourceValues.size());
			replacedSql.append(sqlPieces[1]);
		} else {
			// 可能替换出错,那只能返回原始sql了.
			return sql;
		}

		return replacedSql.toString();
	}

	/**
	 * 拼接preparestatement形式的sql
	 * 
	 * @param sb
	 * @param sqlPieces
	 * @param key
	 * @param size
	 */
	private void appendPrepareStatementSql(StringBuilder sb,
			String[] sqlPieces, int size) {
		sb.append(sqlPieces[0]);
		sb.append(" in (");
		for (int i = 0; i < size; i++) {
			if (i == (size - 1)) {
				sb.append("?");
			} else {
				sb.append("?,");
			}
		}
		sb.append(") ");
	}

	/**
	 * id in情况下,去除原来不需要的parameter,并且变更序号
	 * 
	 * @param parameterSettings
	 * @param realTable
	 * @param filedMap
	 * @param in
	 * @return
	 * @throws SQLException
	 */
	private Map<Integer, ParameterContext> changeParameterContext(
			Map<Integer, ParameterContext> parameterSettings, String realTable,
			Map<String, Field> filedMap, InExpressionObject in)
			throws SQLException {
		Field f = filedMap.get(realTable);
		if (null == f || f.equals(Field.EMPTY_FIELD)) {
			return parameterSettings;
		}
		Set<Object> sourceValues = f.sourceKeys
				.get(in.columnName.toUpperCase());
		if (null == sourceValues) {
			return parameterSettings;
		}
		List<Integer> bindVarIndexs = in.bindVarIndexs;
		Map<Integer, ParameterContext> re = new HashMap<Integer, ParameterContext>();
		SortedMap<Integer, ParameterContext> tempMap = new TreeMap<Integer, ParameterContext>();

		/*
		 * 从parameterSettings找出sourceValues相关的参数比如 select * from tab where
		 * gmt_create < ? and used_times=? and pk in (?,?,?,?) and name=?;参数为
		 * "2010-10-10",100,1,2,3,4,"junyu",算出本张表的pk值为
		 * 2,4,bindVarIndexs为:2,3,4,5
		 * 那么这一步我们从parameterSettings中找出2,4两个参数这一步之后,tempMap中有<4,pc(2)><6,pc(4)>
		 */
		int count = 0;
		for (Integer var : bindVarIndexs) {
			ParameterContext pc = parameterSettings.get(var + 1);
			Object obj = pc.getArgs()[1];
			for (Object s : sourceValues) {
				if (s.equals(obj)) {
					tempMap.put(bindVarIndexs.get(count) + 1, pc);
					count++;
					break;
				}
			}
		}

		/*
		 * 这一步我们将parameterSettings中不属于 id in的参数放到 tempMap中
		 * 因为tempMap是按key排序的sortedMap,
		 * 所以这一步之后,tempMap中有<1,pc("2010-10-10")><2,pc(
		 * 100)><4,pc(2)><6,pc(4)><7,pc("junyu")>
		 */
		for (Map.Entry<Integer, ParameterContext> pc : parameterSettings
				.entrySet()) {
			if (!bindVarIndexs.contains(pc.getKey() - 1)) {
				tempMap.put(pc.getKey(), pc.getValue());
			}
		}

		/*
		 * 因为我们不能动原始的 parameterSettings里面的参数(后续还需要使用),所以我们将需要的参数对象进行深度复制,并且map
		 * key序列变成间隔为1的,
		 * 最终生成<1,pc("2010-10-10")><2,pc(100)><3,pc(2)><4,pc(4)><5,pc("junyu")>
		 * 此时前面已经变换完毕的sql为 select * from tab where gmt_create < ? and
		 * used_times=? and pk in (?,?) and name=?; 从而完成id in归组
		 */
		int tempMapSize = tempMap.size();
		for (int i = 0; i < tempMapSize; i++) {
			Integer ind = tempMap.firstKey();
			ParameterContext pc = tempMap.get(ind);
			ParameterContext ele = new ParameterContext();
			ele.setParameterMethod(pc.getParameterMethod());
			ele.setArgs(new Object[2]);
			ele.getArgs()[0] = i + 1;
			ele.getArgs()[1] = pc.getArgs()[1];
			re.put(i + 1, ele);
			tempMap.remove(ind);
		}

		return re;
	}

	private void changeParameters(Map<Integer, Object> changedParameters,
			Map<Integer, ParameterContext> parameterSettings) {
		for (Map.Entry<Integer, Object> entry : changedParameters.entrySet()) {
			// 注意：SQL解析那边绑定参数从0开始计数，因此需要加1。
			ParameterContext context = parameterSettings
					.get(entry.getKey() + 1);
			if (context.getParameterMethod() != ParameterMethod.setNull1
					&& context.getParameterMethod() != ParameterMethod.setNull2) {
				context.getArgs()[1] = entry.getValue();
			}
		}
	}

	/**
	 * update in的问题：
	 */
	protected final List<SqlExecuteEvent> createEvent(
			DispatcherResult metaData, SqlType sqlType, String originalSql,
			boolean needRowCopy) throws SQLException {
		if ((sqlType == SqlType.INSERT || sqlType == SqlType.UPDATE)
				&& needRowCopy && metaData.needRowCopy()) {
			return SqlExecuteEventUtil.createEvent(metaData, sqlType,
					originalSql);
		} else {
			return null;
		}
	}

	/**
	 * 打印log
	 * 
	 * @param dbIndex
	 * @param actualTables
	 */
	private void printLog(String dbIndex, List<Map<String, String>> actualTables) {
		if (log.isDebugEnabled()) {
			log.debug("pool: " + dbIndex);

			StringBuilder buffer = new StringBuilder("actualTables: [");
			boolean firstElement = true;
			for (Map<String, String> tab : actualTables) {
				if (!firstElement) {
					buffer.append(", ");
				} else {
					firstElement = false;
				}

				buffer.append(tab);
			}
			buffer.append("]");

			log.debug(buffer.toString());
		}
	}

	public static void setResult(ExecutionPlan executionPlan,
			PipelineRuntimeInfo runtime) {
		executionPlan.setSqlMetaData(runtime.getSqlMetaData());
		runtime.setExecutionPlan(executionPlan);
	}

	public static void main(String[] args) {
		ExecutionPlanHandler ep = new ExecutionPlanHandler();
		String sql = "select /*+ INDEX(T, IDX_BILL_BILLING_STATUS)*/ ID, USER_ID, NICK,"
				+ "SOURCE, TRADE_NO, RATING_BILL_ID, EVENT_ID, CHARGE_ITEM_ID,"
				+ "BOOK_ITEM_ID, REL_RECEIVE_PAY, SUB_PROD_ID, FROM_DATE, END_DATE,"
				+ " BILL_CYCLE, FEE_TYPE,"
				+ "PAY_TIME, NOTIFY_TIME, PAYMENT_STATUS, SERV_ID, SERV_PROVIDE, TAOBAO_ALIPAY_ID,"
				+ "ALIPAY_ID, ALIPAY_EMAIL, IS_NEED, IS_FINISHED, GMT_CREATE, GMT_MODIFIED,"
				+ "ORDER_END_TIME,SERV_CODE,PROD_ID,PTRADE_ID,TRADE_ID,ACCUSE_ID,BILL_TIME,BILL_TYPE,ALI_PAY_TIME,RATE_RECEIVE_PAY,ALIPAY_COMMITE_FEE,"
				+ " ACCOUNT_BOOK_ID,ACCOUNT_BOOK_DETAIL_ID,STATUS,HASH_CODE,GROUP_ID,VERSION,SP_ID,SP_TYPE,AFTER_TAX,TAX,WRITEOFF_TIME,SC_RECORD_ID,STATICS_STATUS,"
				+ "FIN_AMOUNT,BAL_TYPE,FIN_AFTER_TAX,FIN_TAX,BIZ_TYPE,ITEM_INST"
				+ "from BILL_BILLING T WHERE PAYMENT_STATUS in (0,2) and IS_FINISHED = 0 and STATUS != 3 and STATUS != 13"
				+ " and SOURCE = 2 and BIZ_TYPE = 3";

		System.out.println(ep.replaceTableName(sql, "BILL_BILLING",
				"bill_billing_05", log));
	}
}
