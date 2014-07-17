//Copyright(c) Taobao.com
package com.taobao.tddl.client.handler.rulematch;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.common.lang.StringUtil;
import com.taobao.tddl.client.controller.DatabaseExecutionContext;
import com.taobao.tddl.client.controller.DatabaseExecutionContextImp;
import com.taobao.tddl.client.databus.DataBus;
import com.taobao.tddl.client.databus.PipelineRuntimeInfo;
import com.taobao.tddl.client.handler.AbstractHandler;
import com.taobao.tddl.interact.bean.ComparativeMapChoicer;
import com.taobao.tddl.interact.bean.Field;
import com.taobao.tddl.interact.bean.MatcherResult;
import com.taobao.tddl.interact.bean.TargetDB;
import com.taobao.tddl.interact.rule.VirtualTable;
import com.taobao.tddl.interact.rule.VirtualTableRoot;
import com.taobao.tddl.interact.rule.bean.SqlType;
import com.taobao.tddl.rule.le.TddlRuleInner;
import com.taobao.tddl.rule.le.exception.ResultCompareDiffException;
import com.taobao.tddl.sqlobjecttree.SqlParserResult;

/**
 * @description todo
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.4
 * @since 1.6
 * @date 2010-11-15下午02:23:42
 */
public class RuleLeRouteMatchHandler extends AbstractHandler {
	public static final String HANDLER_NAME = "RuleLeRouteMatchHandler";
	private final Log log = LogFactory.getLog(RuleLeRouteMatchHandler.class);
	private TddlRuleInner tddlRule=null;
   
	public RuleLeRouteMatchHandler(TddlRuleInner tddlRule){
		this.tddlRule=tddlRule;
	}
	
	/**
	 * RouteMatchHandler会对NOSQLPARSE和DEFAULT类型的执行进行处理
	 */
	public void handleDown(DataBus dataBus) throws SQLException {
		FlowType flowType = getPipeLineRuntimeInfo(dataBus).getFlowType();
		if (FlowType.DEFAULT == flowType || FlowType.NOSQLPARSE == flowType
				|| FlowType.BATCH == flowType
				|| FlowType.BATCH_NOSQLPARSER == flowType
				|| FlowType.DBANDTAB_RC == flowType
				|| FlowType.DBANDTAB_SQL == flowType) {
			try {
				match(dataBus);
			} catch (ResultCompareDiffException e) {
			    throw new SQLException(e);
			}
		}
	}

	/**
	 * 规则路由主体部分，主要的操作，就是将SQLPARSE得到的分库分表字段所对应的 值作为参数填入规则中计算出结果，根据table
	 * rule计算出来的就是需要走的 表，根据db rule计算出来就是需要走的库(库后缀)。
	 * 
	 * @param dataBus
	 * @throws ResultCompareDiffException 
	 */
	protected void match(DataBus dataBus) throws ResultCompareDiffException {
		PipelineRuntimeInfo runtime = super.getPipeLineRuntimeInfo(dataBus);

		SqlParserResult spr=runtime.getSqlParserResult();
		ComparativeMapChoicer choicer = spr.getComparativeMapChoicer();
		List<Object> sqlParameters = runtime.getStartInfo().getSqlParameters();
		SqlType sqlType=runtime.getStartInfo().getSqlType();
		Set<String> logicTableNames = runtime.getLogicTableNames();
		boolean isSqlParse = runtime.getIsSqlParsed();
		VirtualTableRoot root = runtime.getVirtualTableRoot();
		boolean needIdInGroup = root.isNeedIdInGroup();
		boolean completeDistinct = root.isCompleteDistinct();

		boolean isAllowReverseOutput = false;

		if (logicTableNames.size() == 1) {
			String logicTableName = logicTableNames.iterator().next();
			VirtualTable rule = root.getVirtualTable(StringUtil
					.toLowerCase(logicTableName));

			if (rule == null) {
				throw new IllegalArgumentException("未能找到对应规则,逻辑表:"
						+ logicTableName);
			}

			// 只有sql解析器解析过的才允许反向输出
			if (isSqlParse) {
				isAllowReverseOutput = rule.isAllowReverseOutput();
			} else {
				isAllowReverseOutput = false;
			}
            
			MatcherResult matcherResult=null;
			if(spr.getInExpressionObjectList().size()>0&&needIdInGroup){
			    matcherResult = tddlRule.routeMVerAndCompare(sqlType, logicTableName,  choicer, sqlParameters, true);
			}else{
  			    matcherResult = tddlRule.routeMVerAndCompare(sqlType,logicTableName,  choicer, sqlParameters, false);
			}
			
			List<TargetDB> targetDBs = matcherResult.getCalculationResult();
			// 多了一段转义的过程，额外开销
			List<DatabaseExecutionContext> databaseExecutionContexts = convertToDatabaseExecutionContext(
					logicTableName, targetDBs);

			setResult(databaseExecutionContexts, matcherResult, null,
					isAllowReverseOutput, rule.isNeedRowCopy(),
					rule.getUniqueKeys(), needIdInGroup, completeDistinct,
					runtime);

		} else {
			/*
			 * 这里是一个用于具体处理单表对多表join和多表对多表join的处理选择支。 会分别计算多个不同的表的规则，如果通过计算
			 * 
			 * @add by shenxun
			 */
			SqlParserResult sqlParserResult = runtime.getSqlParserResult();
			Map<String, String> alias = runtime.getAlias();

			int leftIndex = 0;
			int rightIndex = 1;

			List<List<TargetDB>> targetDBList = new ArrayList<List<TargetDB>>(
					logicTableNames.size());
			String[] logicTableArray = logicTableNames.toArray(new String[0]);
			List<DatabaseExecutionContext> databaseExecutionContexts = new LinkedList<DatabaseExecutionContext>();
			for (String logicTableName : logicTableArray) {
				VirtualTable rule = root.getVirtualTable(StringUtil
						.toLowerCase(logicTableName));

				if (rule == null) {
					if (log.isDebugEnabled()) {
						log.debug("can't find table by " + logicTableName
								+ " ,this logic table may dont need calc");
					}
				} else {
					if (isSqlParse && rule.isAllowReverseOutput()) {// 只允许false
						// 变为true,多表中有一个表需要反向，那就是都需要反向输出了
						isAllowReverseOutput = rule.isAllowReverseOutput();
					}
					
					MatcherResult matcherResult=tddlRule.routeMVerAndCompare(sqlType, logicTableName, sqlParserResult.getComparativeMapChoicer(), sqlParameters, true);
					targetDBList.add(matcherResult.getCalculationResult());
				}
			}

			// 不等于1，那么要考虑笛卡尔积的问题了
			if (targetDBList.size() > 2) {
				throw new IllegalArgumentException("暂时不支持三个表都走规则的join");
			}
			List<TargetDB> left = targetDBList.get(leftIndex);
			List<TargetDB> right = targetDBList.get(rightIndex);
			/**
			 * size个数不等，肯定是非对称join,直接丢出去
			 */

			if (left.size() != right.size()) {
				throw new IllegalArgumentException("tddl 目前只支持多表对等join");
			}

			if (left.size() == 1) {
				TargetDB leftTarget = left.get(0);
				TargetDB rightTarget = right.get(0);
				databaseExecutionContexts.add(buildOneDatabaseJoin(leftIndex,
						rightIndex, logicTableArray, leftTarget, rightTarget,
						alias));
			} else if (left.size() == 0) {
				throw new IllegalArgumentException("should not be here");
			} else {

				int count = 0;

				for (TargetDB leftTargetDB : left) {
					for (TargetDB rightTargetDB : right) {
						if (leftTargetDB.getDbIndex().equals(
								rightTargetDB.getDbIndex())) {
							databaseExecutionContexts.add(buildOneDatabaseJoin(
									leftIndex, rightIndex, logicTableArray,
									leftTargetDB, rightTargetDB, alias));
							count++;
						}
					}
				}
				if (count != left.size()) {
					throw new IllegalArgumentException("库的个数不匹配");
				}
			}

			setResult(databaseExecutionContexts, null, targetDBList,
					isAllowReverseOutput, false, null, needIdInGroup,
					completeDistinct, runtime);
		}
		debugLog(log, new Object[] { "rule match end." });
	}

	/**
	 * 
	 * @param logicTableName
	 * @param targetDBs
	 * @return
	 */
	private List<DatabaseExecutionContext> convertToDatabaseExecutionContext(
			String logicTableName, List<TargetDB> targetDBs) {
		List<DatabaseExecutionContext> databaseExecutionContexts = new ArrayList<DatabaseExecutionContext>(
				targetDBs.size());
		for (TargetDB targetDB : targetDBs) {
			DatabaseExecutionContextImp dbec = new DatabaseExecutionContextImp();
			Set<String> tableSet = targetDB.getTableNames();
			buildOneToOneJoin(targetDB.getDbIndex(), dbec, logicTableName,
					tableSet);
			dbec.setRealTableFieldMap(targetDB.getTableNameMap());
			databaseExecutionContexts.add(dbec);
		}
		return databaseExecutionContexts;
	}

	/**
	 * 
	 * @param dbec
	 * @param one
	 * @param manySet
	 */
	private void buildOneToManyJoin(String dbIndex,
			DatabaseExecutionContextImp dbec, String oneLogicTable, String one,
			String manyLogicTable, Set<String> manySet) {

		for (String many : manySet) {
			Map<String, String> tablePair = new HashMap<String, String>(1, 1);
			tablePair.put(manyLogicTable, many);
			tablePair.put(oneLogicTable, one);
			dbec.addTablePair(tablePair);
		}
		dbec.setDbIndex(dbIndex);
	}

	/**
	 * 
	 * @param dbec
	 * @param one
	 * @param manySet
	 */
	private void buildOneToOneJoin(String dbIndex,
			DatabaseExecutionContextImp dbec, String oneLogicTable,
			Set<String> targetTables) {
		for (String one : targetTables) {
			Map<String, String> tablePair = new HashMap<String, String>(1, 1);
			tablePair.put(oneLogicTable, one);
			dbec.addTablePair(tablePair);
		}
		dbec.setDbIndex(dbIndex);
	}

	/**
	 * 
	 * @param leftIndex
	 * @param rightIndex
	 * @param logicTableArray
	 * @param leftTarget
	 * @param rightTarget
	 * @param alias
	 * @return
	 */
	private DatabaseExecutionContext buildOneDatabaseJoin(int leftIndex,
			int rightIndex, String[] logicTableArray, TargetDB leftTarget,
			TargetDB rightTarget, Map<String, String> alias) {
		// 通过计算，两个规则都只有一个库
		DatabaseExecutionContextImp dbec = new DatabaseExecutionContextImp();

		if (leftTarget.getDbIndex().equals(rightTarget.getDbIndex())) {// dbIndex相同，表示在同一个库

			Map<String, Field> leftTableNameMap = leftTarget.getTableNameMap();
			Map<String, Field> rightTableNameMap = rightTarget
					.getTableNameMap();
			if (leftTableNameMap.size() == 1) {// 左边等于1个表名，右边不等，一对多笛卡尔积
				String one = leftTableNameMap.keySet().iterator().next();
				String dbIndex = leftTarget.getDbIndex();
				String oneLogicTable = logicTableArray[0];
				String manyLogicTable = logicTableArray[1];
				Set<String> manySet = rightTableNameMap.keySet();
				buildOneToManyJoin(dbIndex, dbec, oneLogicTable, one,
						manyLogicTable, manySet);
			} else if (rightTableNameMap.size() == 1) {// 右边等于1个表名，左边不等的情况
				String one = rightTableNameMap.keySet().iterator().next();
				String dbIndex = rightTarget.getDbIndex();
				String oneLogicTable = logicTableArray[1];
				String manyLogicTable = logicTableArray[0];
				Set<String> manySet = leftTableNameMap.keySet();
				buildOneToManyJoin(dbIndex, dbec, oneLogicTable, one,
						manyLogicTable, manySet);
			} else {// 都不等于1 ,那么可能需要笛卡尔积，也可能不需要
				// 处理单库多表对多表join
				if (leftTableNameMap.size() == rightTableNameMap.size()) {// 相等，可能可以直接对应
					for (Entry<String/* table name */, Field> entry : leftTableNameMap
							.entrySet()) {

						String leftTableName = entry.getKey();
						int leftIndexNumberInt = getIndexNumberInt(leftTableName);
						// 这段代码的作用是:比较两个表+参数
						// 的map,如果找得到相同的表，则比较参数，如果参数也完全相同
						// 表示左表和右表是对应的。
						// 那么这时候就可以拼成一个join替换用的map.其余的情况，则抛出不同的异常
						dbec.setDbIndex(leftTarget.getDbIndex());
						for (Entry<String/* table name */, Field> rightEntry : rightTableNameMap
								.entrySet()) {
							// 这里是个hack.
							// ，因为表名是由logicTableName+_0000这样的模式组成的，所以只需要比较后面的数值即可
							String rightTableName = rightEntry.getKey();
							int rightIndexNumberInt = getIndexNumberInt(rightTableName);
							if (rightIndexNumberInt == leftIndexNumberInt) {// 表示是同一个表
								Field rightField = rightTableNameMap.get(entry
										.getKey());
								if (rightField == null) {// 左侧的表名在右侧没有出现，表示规则不同
									throw new IllegalArgumentException(
											"多表join表名，不支持。"
													+ "只有在多表表规则完全相同的场景下，才允许进行多表M*N join");
								}
								Field leftField = entry.getValue();
								if (!rightField.equals(leftField, alias)) {
									throw new IllegalArgumentException(
											"不是规则完全相同的数据表之间进行的join，不支持。"
													+ "只有在多表表规则完全相同的场景下，才允许进行多表M*N join");
								} else {// 在同一个表内，并且决定表的参数也相同
									String leftLogicTable = logicTableArray[leftIndex];
									String rightLogicTable = logicTableArray[rightIndex];

									Map<String, String> tablePair = new HashMap<String, String>(
											1);
									tablePair
											.put(leftLogicTable, leftTableName);
									tablePair.put(rightLogicTable,
											rightTableName);
									dbec.addTablePair(tablePair);
								}
							}
						}
					}

				} else {
					// 都不等 ，直接抛出
					throw new IllegalArgumentException(
							"表名个数不等，tddl 目前不支持多对多笛卡尔积join");
				}
			}

		} else {// dbIndex不同，在不同库上
			throw new IllegalArgumentException("tddl 不允许在多库上执行join查询，数据库无法支持");
		}
		return dbec;
	}

	/**
	 * 
	 * @param indexNumber
	 * @return
	 */
	private int getIndexNumberInt(String indexNumber) {
		int indexNumberInt;
		indexNumber = getIndexNumber(indexNumber);
		try {
			indexNumberInt = Integer.valueOf(indexNumber);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("转换格式错误:" + indexNumber);
		}
		return indexNumberInt;
	}

	/**
	 * 
	 * @param indexNumber
	 * @return
	 */
	private String getIndexNumber(String indexNumber) {
		int lastIndex = StringUtil.lastIndexOf(indexNumber, "_");
		indexNumber = StringUtil.substring(indexNumber, lastIndex + 1);
		return indexNumber;
	}

	/**
	 * 设置结果，主要就是设置一个MatcherResult,提供给后面Handler使用
	 * 
	 * @param matcherResult
	 * @param runtime
	 */
	private void setResult(
			List<DatabaseExecutionContext> dataBaseExecutionContext,
			MatcherResult matcherResult, List<List<TargetDB>> targetDBList,
			boolean isAllowReverseOutput, boolean needRowCopy,
			List<String> uniqueColumns, boolean needIdInGroup,
			boolean completeDistinct, PipelineRuntimeInfo runtime) {
		runtime.setMatcherResult(matcherResult);
		runtime.setAllowReverseOutput(isAllowReverseOutput);
		runtime.setTargetDBList(targetDBList);
		runtime.setDataBaseExecutionContext(dataBaseExecutionContext);
		runtime.setNeedRowCopy(needRowCopy);
		runtime.setUniqueColumns(uniqueColumns);
		runtime.setNeedIdInGroup(needIdInGroup);
		runtime.setCompleteDistinct(completeDistinct);
	}
}
