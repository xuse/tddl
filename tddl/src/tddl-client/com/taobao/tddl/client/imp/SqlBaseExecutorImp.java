package com.taobao.tddl.client.imp;

import java.util.List;

import org.springframework.orm.ibatis.SqlMapClientTemplate;

import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.client.SqlBaseExecutor;
import com.taobao.tddl.common.client.ThreadLocalString;
import com.taobao.tddl.common.client.util.ThreadLocalMap;

public class SqlBaseExecutorImp implements SqlBaseExecutor {
//	private static final Logger log = Logger.getLogger(SqlBaseExecutorImp.class);
	int notifyTime=0;
	private SqlMapClientTemplate sqlMapClientTemplate;
//	private DBRuleProvider dbrouteProvider = null;

	public SqlMapClientTemplate getSqlMapClientTemplate() {
		return sqlMapClientTemplate;
	}

	public void setSqlMapClientTemplate(
			SqlMapClientTemplate sqlMapClientTemplate) {
		this.sqlMapClientTemplate = sqlMapClientTemplate;
	}

	public Object insert(String statementID, Object parameterObject,
			RouteCondition rc)  {
		ThreadLocalMap.put(ThreadLocalString.ROUTE_CONDITION, rc);
		return sqlMapClientTemplate.insert(statementID, parameterObject);
	}

	public List<Object> queryForList(String statementID,
			Object parameterObject, RouteCondition rc)
			 {
		return queryForList(statementID, parameterObject, false, rc);
	}

	@SuppressWarnings("unchecked")
	public List<Object> queryForList(String statementID,
			Object parameterObject, boolean isExistQuit, RouteCondition rc)
			 {
		ThreadLocalMap.put(ThreadLocalString.ROUTE_CONDITION, rc);
		ThreadLocalMap.put(ThreadLocalString.IS_EXIST_QUITE, isExistQuit);
		return sqlMapClientTemplate.queryForList(statementID, parameterObject);
	}

	public List<Object> queryForMergeSortList(String statementID,
			Object parameterObject, RouteCondition rc)
			 {
		return this.queryForMergeSortTables(statementID, parameterObject,
			 rc);
	}

	public List<Object> queryForMergeSortTables(String statementID,
			Object parameterObject, RouteCondition rc) {
		return queryForList(statementID, parameterObject, false, rc);
	}

//	/**
//	 * 获取结果集
//	 * 
//	 * @param statementID
//	 * @param parameterObject
//	 * @param poolMap
//	 * @param tempSortList
//	 * @param retObjs
//	 * @param param
//	 * @param skip
//	 * @param count
//	 * @param max
//	 * @param range
//	 * @throws MergeSortTableCountTooBigException
//	 */
//	@SuppressWarnings("unchecked")
//	private void getResults(String statementID, Object parameterObject,
//			Map<TableNameObj, Set<String[]>> poolMap,
//			List<TableNameObj> tempSortList, List<Object> retObjs, Map param,
//			long skip, long count, long max, long range,MergeSortTablesRetVal rev)
//			throws MergeSortTableCountTooBigException {
//		Set<String[]> tableVsPoolSet;
//		long time=0;
//		boolean needCalculation = false;
//		// 针对列表中的每一项
//		for (TableNameObj tab : tempSortList) {
//			tableVsPoolSet = poolMap.get(tab);
//
//			putTableMergeConditionToThreadLocalMap(rev.getVTabName(), tableVsPoolSet, tab);
//			param = addSkipMax2ParamMap(parameterObject, skip, max);
//			// 取数据。
//			time=System.currentTimeMillis();
//			List tempList = sqlMapClientTemplate.queryForList(statementID,
//					param);
//			long tempTime=System.currentTimeMillis()-time;
//			if(tempTime>=notifyTime){
//				log.warn("run queryForList once,statementID is "+statementID+" " +
//						"param size :[" );
//			
//				log.warn(param.size()+"],elapsed time is "+tempTime);
//			}
//			log.info("run queryForList once,elapsed time is "+tempTime);
//			// 如果未取到
//			if (tempList == null || tempList.size() == 0) {
//				needCalculation = true;
//				log.debug("can't at least one element,need calculation this db");
//			} else {
//				retObjs.addAll(tempList);
//				max = max - tempList.size();
//				needCalculation = false;
//			}
//			/*
//			 * 如果一个表对应了多个读库，则表示在这次查询中涉及了多库查询，暂时不支持
//			 */
//			if (tableVsPoolSet.size() != 1) {
//				throw new TDLRunTimeException("目前还不支持多表多库查询，请指定唯一读数据库");
//			} else {
//				// 向后端提交排序表名和
//	
//				if (retObjs.size() >= range) {
//					break;
//				}
//				if (needCalculation) {
//					putTableMergeConditionToThreadLocalMap(rev.getVTabName(), tableVsPoolSet, tab);
//					Object obj = sqlMapClientTemplate.queryForObject(
//							rev.getCountStatementId(), param);
//					if (obj instanceof Integer) {
//						count = ((Integer) obj).longValue();
//					} else if (obj instanceof Long) {
//						count = ((Long) obj).longValue();
//					} else if (obj instanceof BigDecimal) {
//						count = ((BigDecimal) obj).longValue();
//					} else {
//						throw new TDLRunTimeException(
//								"count查询结果不是integer,bigDecimal或long,无法进行累加");
//					}
//
//					/*
//					 * 这里会有个问题，如果数据项增长很快的情况下，
//					 * 有可能出现虽然取数据时未未取到，但在下次count时却取到大于skip值的count值的情况
//					 * 更坏的情况是count值可能还大于max. 这种情况无法判断。 但预计不会经常发生。
//					 */
//					if (skip >= count) {
//						skip = skip - count;
//						max = max - count;
//					} else {
//						// 超过skip值或者甚至超过max值的话，max=range
//						max =range;
//						skip=0;
//						
//					}
////					param = addSkipMax2ParamMap(parameterObject, skip, max);
//				}
//
//			}
//
//		}
//	}
//
//	private void putTableMergeConditionToThreadLocalMap(String vTabName,
//			Set<String[]> tableVsPoolSet, TableNameObj tab) {
//		ThreadLocalMap.put(ThreadLocalString.TABLE_MERGE_SORT_VIRTUAL_TABLE_NAME, vTabName);
//		ThreadLocalMap.put(
//				ThreadLocalString.TABLE_MERGE_SORT_TABLENAME,
//				tab.tabName);
//		ThreadLocalMap.put(ThreadLocalString.TABLE_MERGE_SORT_POOL,
//		tableVsPoolSet.toArray()[0]);
//	}
//
//	/**
//	 * 排序表名
//	 * 
//	 * @param rev
//	 * @param poolMap
//	 * @return
//	 */
//	private List<TableNameObj> sortTables(MergeSortTablesRetVal rev,
//			Map<TableNameObj, Set<String[]>> poolMap) {
//		List<TableNameObj> tempSortList = new ArrayList<TableNameObj>(poolMap
//				.keySet());
//		if (rev.isAsc()) {
//			Collections.sort(tempSortList, new Comparator<TableNameObj>() {
//
//				public int compare(TableNameObj o1, TableNameObj o2) {
//
//					return o1.value - o2.value;
//				}
//
//			});
//		} else {
//			Collections.sort(tempSortList, new Comparator<TableNameObj>() {
//
//				public int compare(TableNameObj o1, TableNameObj o2) {
//
//					return o2.value - o1.value;
//				}
//
//			});
//		}
//		return tempSortList;
//	}
//
//	/**
//	 * 建立表名->读池的key -value关系
//	 * 
//	 * @param dbs
//	 * @param poolMap
//	 */
//	private void buildPoolMapBetweenTableNameVsReadPool(List<TargetDBs> dbs,
//			Map<TableNameObj, Set<String[]>> poolMap) {
//		long time=0;
//		if(log.isDebugEnabled()){
//			time=System.currentTimeMillis();
//		}
//		for (TargetDBs tdb : dbs) {
//			Set<TableNameObj> tabNames = tdb.getTableNames();
//			for (TableNameObj tabName : tabNames) {
//				if (poolMap.containsKey(tabName)) {
//					Set<String[]> temp = poolMap.get(tabName);
//					temp.add(tdb.getReadPool());
//					poolMap.put(tabName, temp);
//				} else {
//					Set<String[]> tempSet = new HashSet<String[]>();
//					// poolMap.put(str, tempSet);
//					tempSet.add(tdb.getReadPool());
//		
//					poolMap.put(tabName, tempSet);
//				}
//			}
//		}
//		if(log.isDebugEnabled()){
//			log.debug("buildPoolMapBetweenTableNameVsReadPoll,elapsed times is "+(System.currentTimeMillis()-time));
//		}
//	}

	/**
	 * 添加skip和max到参数内，写死了start和end,同时强制要求parameterObject必须为Map
	 * 
	 * @param parameterObject
	 * @param skip
	 * @param max
	 * @return
	 */
/*	private Map addSkipMax2ParamMap(Object parameterObject, long skip, long max) {
		Map param = null;
		if (parameterObject instanceof Map) {
			param = (Map) parameterObject;
			param.put("start", Long.valueOf(skip));
			param.put("end", Long.valueOf(max));
		} else {
			throw new RuntimeException("请以Map为参数输入，否则无法动态添加start和end进行分页");
		}
		return param;
	}*/

	public Object queryForObject(String statementID, Object parameterObject,
			RouteCondition rc)  {
		return queryForObject(statementID, parameterObject, false, rc);
	}

	public Object queryForObject(String statementID, Object param,
			boolean isExistsQuit, RouteCondition rc)  {

		ThreadLocalMap.put(ThreadLocalString.ROUTE_CONDITION, rc);
		ThreadLocalMap.put(ThreadLocalString.IS_EXIST_QUITE, isExistsQuit);
		return sqlMapClientTemplate.queryForObject(statementID, param);
	}

	public int update(String statementID, Object parameterObject,
			RouteCondition rc)  {
		ThreadLocalMap.put(ThreadLocalString.ROUTE_CONDITION, rc);
		return sqlMapClientTemplate.update(statementID, parameterObject);
	}

	public Object insert(String statementID, Object parameterObject) {
		return this.insert(statementID, parameterObject, null);
	}

	public List<Object> queryForList(String statementID, Object parameterObject) {
		 return this.queryForList(statementID, parameterObject, null);
	}

	public List<Object> queryForList(String statementID,
			Object parameterObject, boolean isExistQuit) {
		return queryForList(statementID, parameterObject, isExistQuit, null);
	}

	public List<Object> queryForMergeSortList(String statementID,
			Object parameterObject) {
		return queryForMergeSortList(statementID, parameterObject, null);
	}

	public List<Object> queryForMergeSortTables(String statementID,
			Object parameterObject) {
		return queryForMergeSortTables(statementID, parameterObject, null);
	}

	public Object queryForObject(String statementID, Object parameterObject) {
		 return queryForObject(statementID, parameterObject, null);
	}

	public Object queryForObject(String statementID, Object param,
			boolean isExistsQuit) {
		return queryForObject(statementID, param, isExistsQuit, null);
	}

	public int update(String statementID, Object parameterObject) {
		return update(statementID, parameterObject,null);
	}

}
