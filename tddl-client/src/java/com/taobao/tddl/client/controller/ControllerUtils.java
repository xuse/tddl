package com.taobao.tddl.client.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.taobao.tddl.client.dispatcher.DispatcherResult;
import com.taobao.tddl.client.dispatcher.EXECUTE_PLAN;
import com.taobao.tddl.interact.bean.ReverseOutput;
import com.taobao.tddl.interact.bean.TargetDB;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.sqlobjecttree.DMLCommon;
import com.taobao.tddl.sqlobjecttree.SqlAndTableAtParser;
import com.taobao.tddl.sqlobjecttree.SqlParserResult;
import com.taobao.tddl.sqlobjecttree.Update;
import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.HandlerContainer;

public class ControllerUtils
{
	/**
	 * 添加分库的相关信息，但如果唯一键和分库键的key一致，则分库键内不会重复出现唯一键已经有的key=value对 
	 * 
	 * @param mapList
	 * @param retMeta
	 */
	public static void appendDatabaseSharedMetaData(
			Map<String, Comparative> mapList, DispatcherResultImp retMeta)
	{
		if (mapList == null)
		{
			return;
		}
		String uniqueColumnKey = getUniqueKey(retMeta);
		for (Entry<String, Comparative> oneValue : mapList.entrySet())
		{
			String sharedKey = toLowerCaseIgnoreNull(oneValue.getKey());
			// 如果分库键和唯一键重复，则分库键内不重复出现唯一键
			if (!sharedKey.equals(uniqueColumnKey))
			{
				// 给前端传入的全部是小写
				ColumnMetaData colMeta = new ColumnMetaData(sharedKey,
						oneValue.getValue());
				retMeta.addSplitDB(colMeta);
			}
		}
	}

	public static String toLowerCaseIgnoreNull(String tobeDone)
	{
		if (tobeDone != null)
		{
			return tobeDone.toLowerCase();
		}
		return null;
	}

	/**
	 * 这里返回null如果没有主键，为的是在TStatement中已经做了对pk为null的判断，为了保证测试一致性
	 * 
	 * @param retMeta
	 * @return
	 */
	protected static String getUniqueKey(DispatcherResultImp retMeta)
	{
		ColumnMetaData uniqueKey = retMeta.getPrimaryKey();
		if (uniqueKey == null)
		{
			return null;
		}
		return uniqueKey.key;
	}

	protected static Set<String> getDatabaseSharedingKeys(
			DispatcherResultImp retMeta)
	{
		List<ColumnMetaData> dbExpression = retMeta.getSplitDB();
		if (dbExpression == null || dbExpression.size() == 0)
		{
			return Collections.emptySet();
		}

		Set<String> dbkeys = new HashSet<String>(dbExpression.size());
		for (ColumnMetaData col : dbExpression)
		{
			dbkeys.add(col.key);
		}
		return dbkeys;
	}

	/**
	 * 创建执行计划
	 * 
	 * 其中表的执行计划，如果有多个库里面的多个表的个数不同，那么按照表的数量最多的那个值为准。
	 * 即：如db1~5，表的个数分别为0,0,0,0,1:那么返回的表执行计划为SINGLE
	 * 若，表的个数分别为0,1,2,3,4,5：那么返回表的执行计划为MULTIPLE.
	 * 
	 * @param dispatcherResult
	 * @param targetDBList
	 */
	public static void buildExecutePlan(DispatcherResult dispatcherResult,
			List<DatabaseExecutionContext> databaseExecutionContexts)
	{
		if (databaseExecutionContexts == null)
		{
			throw new IllegalArgumentException("targetDBList is null");
		}
		int size = databaseExecutionContexts.size();
		switch (size)
		{
		case 0:
			dispatcherResult.setDatabaseExecutePlan(EXECUTE_PLAN.NONE);
			dispatcherResult.setTableExecutePlan(EXECUTE_PLAN.NONE);
			break;
		case 1:
			DatabaseExecutionContext targetDB = databaseExecutionContexts.get(0);
			List<Map<String, String>>  set = targetDB.getTableNames();
			dispatcherResult.setTableExecutePlan(buildTableExecutePlan(set,
					null));
			// 如果表为none，那么库也为none.如果表不为none，那么库为single
			if (dispatcherResult.getTableExecutePlan() != EXECUTE_PLAN.NONE)
			{
				dispatcherResult.setDatabaseExecutePlan(EXECUTE_PLAN.SINGLE);
			} else
			{
				dispatcherResult.setDatabaseExecutePlan(EXECUTE_PLAN.NONE);
			}
			break;
		default:
			EXECUTE_PLAN currentExeutePlan = EXECUTE_PLAN.NONE;
			for (DatabaseExecutionContext oneDB : databaseExecutionContexts)
			{
				currentExeutePlan = buildTableExecutePlan(
						oneDB.getTableNames(), currentExeutePlan);
			}
			dispatcherResult.setTableExecutePlan(currentExeutePlan);
			if (dispatcherResult.getTableExecutePlan() != EXECUTE_PLAN.NONE)
			{
				dispatcherResult.setDatabaseExecutePlan(EXECUTE_PLAN.MULTIPLE);
			} else
			{
				dispatcherResult.setDatabaseExecutePlan(EXECUTE_PLAN.NONE);
			}
			break;
		}
	}

	private static EXECUTE_PLAN buildTableExecutePlan(List<Map<String, String>>  tableSet,
			EXECUTE_PLAN currentExecutePlan)
	{
		if (currentExecutePlan == null)
		{
			currentExecutePlan = EXECUTE_PLAN.NONE;
		}
		EXECUTE_PLAN tempExecutePlan = null;
		if (tableSet == null)
		{
			throw new IllegalStateException("targetTab is null");
		}
		int tableSize = tableSet.size();
		// 不可能为负数
		switch (tableSize)
		{
		case 0:
			tempExecutePlan = EXECUTE_PLAN.NONE;
			break;
		case 1:
			tempExecutePlan = EXECUTE_PLAN.SINGLE;
			break;
		default:
			tempExecutePlan = EXECUTE_PLAN.MULTIPLE;
		}
		return tempExecutePlan.value() > currentExecutePlan.value() ? tempExecutePlan
				: currentExecutePlan;
	}

	/**
	 * 添加分表的相关信息，但如果唯一键和分表键的key一致，则分表键内不会重复出现唯一键已经有的key=value对
	 * 同时，如果分库键中所有的key，也不会出现在分表键中
	 * 
	 * @param mapList
	 * @param retMeta
	 *            TODO:test
	 */
	public static void appendTableSharedMetaData(
			Map<String, Comparative> mapList, DispatcherResultImp retMeta)
	{
		if (mapList == null)
		{
			return;
		}
		for (Entry<String, Comparative> oneValue : mapList.entrySet())
		{

			String uniqueColumnKey = getUniqueKey(retMeta);
			Set<String> dbSharedingKeys = getDatabaseSharedingKeys(retMeta);

			String sharedKey = toLowerCaseIgnoreNull(oneValue.getKey());
			// 如果分表键和唯一键或分库键重复，则分表键内不重复出现唯一键和分库键
			if (!sharedKey.equals(uniqueColumnKey)
					&& !dbSharedingKeys.contains(sharedKey))
			{
				ColumnMetaData colMeta = new ColumnMetaData(sharedKey,
						oneValue.getValue());
				retMeta.addSplitTab(colMeta);
			}
		}
	}

	public static void appendUniqueKeysMetaData(
			Map<String, Comparative> mapList, DispatcherResultImp retMeta)
	{
		if (mapList == null)
		{
			return;
		}
		for (Entry<String, Comparative> oneValue : mapList.entrySet())
		{
			String key = toLowerCaseIgnoreNull(oneValue.getKey());
			ColumnMetaData colMeta = new ColumnMetaData(key,
					oneValue.getValue());
			retMeta.setUniqueKey(colMeta);
		}
	}

	/**
	 * 将单逻辑表的targetDB转换为databaseExecutionContext
	 * 
	 * @param targetDBs
	 * @return
	 */
	public static List<DatabaseExecutionContext> convertSingleTableTargetDBToDBExecutionContext(String logicTable ,List<TargetDB> targetDBs){
		List<DatabaseExecutionContext> databaseExecutionContexts = new ArrayList<DatabaseExecutionContext>(targetDBs.size());
		for(TargetDB targetDB : targetDBs)
		{
			DatabaseExecutionContextImp context = new DatabaseExecutionContextImp();
			context.setDbIndex(targetDB.getDbIndex());
			Set<String> tableNames = targetDB.getTableNames();

			for(String realTable : tableNames)
			{
				Map<String, String> tablePair = new HashMap<String, String>(1,1);
				tablePair.put(logicTable,realTable);
				context.addTablePair(tablePair);
			}
			databaseExecutionContexts.add(context);
		}
		return null;	
	}
	
	/**
	 * 创建反向输出相关的context，反向输出目前主要是解决以下问题
	 * 
	 * :1.如果sql中带有了符合表名替换pattern的字段，并且不想被替换掉。 2.如果sql中包含了跨表的limit m,n的操作，
	 * 3.update+数据复制的情况下，因为分库的version字段默认的情况下是null.
	 * 所以where条件中要加入ifnull或者nvl来保证将原来为Null的参数还原为0、
	 * 这两种情况下都需要进行反向输出，也即通过对象树反向生成sql.
	 * 
	 * 其余的情况因为反向输出本身也会带来风险因此不进行反向。
	 * 
	 * @param args
	 * @param dmlc
	 * @param max
	 * @param skip
	 * @param retMeta
	 * @param isMySQL
	 * @param needRowCopy
	 */
	public static void buildReverseOutput(List<Object> args,
			SqlParserResult dmlc, DispatcherResult retMeta,
			boolean isMySQL)
	{
		int max = retMeta.getMax();
		int skip = retMeta.getSkip();
		boolean needRowCopy = retMeta.needRowCopy();
		List<SqlAndTableAtParser> sqls = null;
		List<DatabaseExecutionContext> databaseExecutionContexts = retMeta.getDataBaseExecutionContexts();
		for (DatabaseExecutionContext databaseExecutionContext : databaseExecutionContexts)
		{

			// 如果目标数据库为一个则有可能是单库单表或单库多表
			HandlerContainer handler = new HandlerContainer();
			// 先处理特殊情况
			if (needRowCopy && dmlc instanceof Update)
			{
				if (isMySQL)
				{
					handler.changeMySQLUpdateVersion();
				} else
				{
					handler.changeOracleUpdateVersion();
				}

				retMeta.needAllowReverseOutput(true);
			}
			// 如果skip max 不为空，并且是多表查询
			if (skip != DMLCommon.DEFAULT_SKIP_MAX
					&& max != DMLCommon.DEFAULT_SKIP_MAX)
			{
				EXECUTE_PLAN dbExecutionPlan = retMeta.getDatabaseExecutePlan();
				EXECUTE_PLAN tabExecutionPlan = retMeta.getTableExecutePlan();
				// 判断是否是个多表查询
				if (dbExecutionPlan.equals(EXECUTE_PLAN.MULTIPLE)
						||tabExecutionPlan.equals(EXECUTE_PLAN.MULTIPLE))
				{ // 多表查询，并且skip max不为空
					handler.changeRange(0, max);
					retMeta.needAllowReverseOutput(true);
				}
			}
			// 处理正常情况，只需要判断是否需要反向输出，如果需要则更换表名和index
			if (retMeta.allowReverseOutput())
			{
				handler.changeIndex();
				handler.changeTable();

				sqls = dmlc.getSqlReadyToRun(databaseExecutionContext.getTableNames(), args,
						handler);
				List<ReverseOutput> reverse = new ArrayList<ReverseOutput>(
						sqls.size());
				for (SqlAndTableAtParser sql : sqls)
				{
					ReverseOutput out = new ReverseOutput();
					out.setParams(sql.modifiedMap);
					out.setSql(sql.sql);
					out.setTable(sql.table.toString());
					reverse.add(out);
				}
				databaseExecutionContext.setOutputSQL(reverse);
			}
		}
	}
}
