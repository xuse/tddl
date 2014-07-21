//Copyright(c) Taobao.com
package com.taobao.tddl.rule.le;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.interact.bean.ComparativeMapChoicer;
import com.taobao.tddl.interact.bean.TargetDB;
import com.taobao.tddl.interact.rule.bean.SqlType;
import com.taobao.tddl.rule.le.exception.ResultCompareDiffException;
import com.taobao.tddl.rule.le.inter.TddlRuleInter;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a> 
 * @version 1.0
 * @since 1.6
 * @date 2011-11-15下午02:50:19
 */
public interface TddlRuleExtend extends TddlRuleInter {
	/**
	 * simple single rule api
	 * with TargetDB list results
	 * 
	 * @param vtab
	 * @param condition
	 * @return
	 */
	public List<TargetDB> routeWithTargetDbResult(String vtab, String conditionStr);
	
	/**
	 * simple single rule api
	 * with TargetDB list results
	 * and ComparativeMapChoicer rule condition
	 * 
	 * @param vtab
	 * @param condition
	 * @return
	 */
	public List<TargetDB> routeWithTargetDbResult(String vtab,ComparativeMapChoicer choicer);
	
	
	/**
	 * new,old versioned rule calculate and compare,but compared result
	 * no longer compare with the original db and tab where the data
	 * come
	 * 
	 * @param vtab
	 * @param conditionStr
	 * @return 
	 */
	public List<TargetDB> routeMultiVersionAndCompareT(SqlType sqlType,
			String vtab, String conditionStr)throws ResultCompareDiffException;
	
	/**
	 * new,old versioned rule calculate and compare,if rule result
	 * different,once more,new rule result compare with the data 
	 * original db and tab come from.if same,return the result
	 * 
	 * this api mainly use in data transfer
	 * 
	 * @param vtab
	 * @param conditionStr
	 * @return
	 */
	public List<TargetDB> routeMultiVersionAndCompareT(SqlType sqlType,
			String vtab, String conditionStr,String oriDb,String oriTable)throws ResultCompareDiffException;
 
	public Set<String> getTableShardColumn(String logicTable);
}
