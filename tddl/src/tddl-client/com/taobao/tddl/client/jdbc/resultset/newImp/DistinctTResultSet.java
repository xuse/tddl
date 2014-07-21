package com.taobao.tddl.client.jdbc.resultset.newImp;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.client.jdbc.ConnectionManager;
import com.taobao.tddl.client.jdbc.TStatementImp;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.jdbc.resultset.helper.ComparatorRealizer;
import com.taobao.tddl.client.jdbc.sqlexecutor.RealSqlExecutor;
/**
 * 暂时不走这个ResultSet,多库多表可能有性能风险
 * 
 * @author junyu
 *
 */
public class DistinctTResultSet extends BaseTResultSet {
	// 只支持单列distinct
	private List<String> distinctColumns;
	// 已经检索数据,有内存溢出的风险
	private List<List<Object>> result = new ArrayList<List<Object>>(
			this.maxSize);
	//防止内存爆掉
	private int maxSize = 100000;
	// 比较器
	private Map<String, Comparator<Object>> compMap = null;
	// 判定结果集是否取尽
	private int rsIndex = 0;

	public DistinctTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,
			RealSqlExecutor realSqlExecutor) throws SQLException {
		super(tStatementImp, connectionManager, executionPlan, realSqlExecutor);
	}

	public DistinctTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,
			RealSqlExecutor realSqlExecutor, boolean init) throws SQLException {
		super(tStatementImp, connectionManager, executionPlan, realSqlExecutor,
				init);
	}

	@Override
	protected boolean internNext() throws SQLException {
		ResultSet rs = actualResultSets.get(rsIndex);
		super.currentResultSet = rs;
		if (rs.next()) {
			if (null == compMap) {
				compMap = new HashMap<String, Comparator<Object>>();
				for (String distinctColumn : distinctColumns) {
					Object obj = rs.getObject(distinctColumn);
					Class<?> sortType = obj.getClass();
					compMap.put(distinctColumn,
							ComparatorRealizer.getObjectComparator(sortType));
				}
			}

			boolean inPreSearch = true;
			List<Object> record = new ArrayList<Object>(distinctColumns.size());
			// 一个字段一个字段比较,根据DISTINCT定义,DISTINCT所有字段值相同,那么才相同
			for (int i = 0; i < distinctColumns.size(); i++) {
				Object obj = rs.getObject(distinctColumns.get(i));
				if (!this.contains(i, distinctColumns.get(i), obj)) {
					inPreSearch = false;
				}
				record.add(obj);
			}

			if (!inPreSearch) {
				if (result.size() < maxSize) {
					result.add(record);
				} else {
					throw new SQLException("[DISTINCT]查询出的数据条数多于限制值"+maxSize+",查询失败!");
				}
				return true;
			} else {
				return internNext();
			}
		} else {
			rsIndex++;
			if (actualResultSets.size() < (rsIndex + 1)) {
				return false;
			}

			return internNext();
		}
	}

	/**
	 * 判定当前检索的数据是否存在于已检索的数据,如果重复 则直接next取下一个.直到取尽
	 * 
	 * @param obj
	 * @return
	 */
	private boolean contains(int index, String distinctColumn, Object obj) {
		Comparator<Object> comp = compMap.get(distinctColumn);
		for (List<Object> re : result) {
			// 一发现有相同就直接返回true
			if (comp.compare(re.get(index), obj) == 0) {
				return true;
			}
		}
		return false;
	}

	public void setDistinctColumn(List<String> distinctColumns) {
		this.distinctColumns = distinctColumns;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
		this.result = new ArrayList<List<Object>>(this.maxSize);
	}
}
