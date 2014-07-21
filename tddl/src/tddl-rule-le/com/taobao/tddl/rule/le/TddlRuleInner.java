//Copyright(c) Taobao.com
package com.taobao.tddl.rule.le;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.interact.bean.ComparativeMapChoicer;
import com.taobao.tddl.interact.bean.MatcherResult;
import com.taobao.tddl.interact.rule.VirtualTable;
import com.taobao.tddl.interact.rule.VirtualTableRoot;
import com.taobao.tddl.interact.rule.VirtualTableRuleMatcher;
import com.taobao.tddl.interact.rule.bean.SqlType;
import com.taobao.tddl.rule.le.exception.ResultCompareDiffException;
import com.taobao.tddl.rule.le.extend.MatchResultCompare;
import com.taobao.tddl.rule.le.inter.TddlRuleTddl;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a> 
 * @version 1.0
 * @since 1.6
 * @date 2011-4-21下午01:39:37
 */
public class TddlRuleInner extends TddlRuleConfig implements TddlRuleTddl{
	private final VirtualTableRuleMatcher matcher = new VirtualTableRuleMatcher();
	
	/**
	 * 简单单套规则支持(TDDL使用)
	 * @param vtab
	 * @param condition
	 * @return
	 */
	public MatcherResult route(String vtab,ComparativeMapChoicer choicer,List<Object> args,boolean needSourceKey){
		VirtualTable rule = this.vtr.getVirtualTable(vtab);
		MatcherResult result = matcher.match(needSourceKey, choicer, args, rule);
		return result;
	}
	
	/**
	 * 多套规则支持(TDDL使用)
	 * @param vtab
	 * @param condition
	 * @return
	 */
	public Map<String, MatcherResult> routeMVer(
			String vtab,
			ComparativeMapChoicer choicer,List<Object> args,boolean needSourceKey) {
		if (this.vtr != null && this.vtrs.size() == 0) {
			throw new RuntimeException(
					"routeWithMulVersion method just support multy version rule,use route method instead or config with multy version style!");
		}

		Map<String, MatcherResult> results = new HashMap<String, MatcherResult>();
		for (Map.Entry<String, VirtualTableRoot> entry : this.vtrs.entrySet()) {
			VirtualTable rule = entry.getValue().getVirtualTable(vtab);
			MatcherResult result = matcher.match(needSourceKey, choicer, args, rule);
			results.put(entry.getKey(), result);
		}
		return results;
	}
	

	/**
	 * 指定一套规则计算
	 * @param vtab
	 * @param condition
	 * @return
	 */
	public MatcherResult route(String vtab,ComparativeMapChoicer choicer,List<Object> args,boolean needSourceKey,VirtualTableRoot specifyVtr){
		VirtualTable rule = specifyVtr.getVirtualTable(vtab);
		if(rule==null){
			StringBuilder sb=new StringBuilder(vtab);
			sb.append(" rule is null!check the rule context!");
			throw new RuntimeException(sb.toString());
		}
		MatcherResult result = matcher.match(needSourceKey, choicer, args, rule);
		return result;
	}
	
	/**
	 * 新旧规则计算并比较,不带目标库判定
	 * 
	 * @param vtab
	 * @param conditionStr
	 * @return
	 * @throws ResultCompareDiffException 
	 */
	public MatcherResult routeMVerAndCompare(SqlType sqlType,
			String vtab, ComparativeMapChoicer choicer,List<Object> args,boolean needSourceKey) throws ResultCompareDiffException {
		return routeMVerAndCompare(sqlType,vtab,choicer,args,needSourceKey,null,null);
	}
	
	/**
	 * 新旧规则计算并比较,带目标库判定
	 * 
	 * @param vtab
	 * @param conditionStr
	 * @return
	 * @throws ResultCompareDiffException 
	 */
	public MatcherResult routeMVerAndCompare(SqlType sqlType,
			String vtab,  ComparativeMapChoicer choicer,List<Object> args,boolean needSourceKey,String oriDb,String oriTable) throws ResultCompareDiffException {
		if (this.vtr != null && this.vtrs.size() == 0) {
			throw new RuntimeException(
					"routeWithMulVersion method just support multy version rule,use route method instead or config with multy version style!");
		}
		/*
		 * 这个逻辑做了修改。将使用规则版本的逻辑，全部交给 versionIndex 来决定
		 * 
		 * 
		// 如果只有单套规则,直接返回这套规则的路由结果
		if (this.vtrs.size() == 1) {
			return route(vtab, choicer,args,needSourceKey,this.vtrs.get(versionIndex.get(0)));
		}

		// 如果不止一套规则,那么计算两套规则,默认都返回新规则
		if (this.vtrs.size() != 2 || this.versionIndex.size() != 2) {
			throw new RuntimeException(
					"not support more than 2 copy rule compare");
		}*/
		
		if (this.versionIndex.size() == 1) {
			return route(vtab, choicer,args,needSourceKey,this.vtrs.get(versionIndex.get(0)));
		}
		
		if (this.versionIndex.size() != 2) {
			throw new RuntimeException(
					"not support more than 2 copy rule compare");
		}
		
		// 第一个排位的为旧规则
		MatcherResult oldResult =route(vtab,choicer,args,needSourceKey, this.vtrs.get(versionIndex.get(0)));
		
		if (sqlType.equals(SqlType.SELECT)
				|| sqlType.equals(SqlType.SELECT_FOR_UPDATE)) {
			return oldResult;
		} else {
			// 第二个排位的为新规则
			MatcherResult newResult =route(vtab,choicer,args,needSourceKey, this.vtrs.get(versionIndex.get(1)));

			boolean compareResult = MatchResultCompare.matchResultCompare(newResult,
					oldResult,oriDb,oriTable);
			
			if (compareResult) {
				return oldResult;
			} else {
				throw new ResultCompareDiffException("sql type is not-select,rule calculate result diff");
			}
		}
	}
}
