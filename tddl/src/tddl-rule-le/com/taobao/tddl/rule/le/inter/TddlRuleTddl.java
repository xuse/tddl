//Copyright(c) Taobao.com
package com.taobao.tddl.rule.le.inter;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.interact.bean.ComparativeMapChoicer;
import com.taobao.tddl.interact.bean.MatcherResult;
import com.taobao.tddl.interact.rule.VirtualTableRoot;
import com.taobao.tddl.interact.rule.bean.SqlType;
import com.taobao.tddl.rule.le.exception.ResultCompareDiffException;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a> 
 * @version 1.0
 * @since 1.6
 * @date 2011-5-4下午06:54:24
 */
public interface TddlRuleTddl {
	/**
	 * 简单单套规则支持(TDDL使用)
	 * @param vtab
	 * @param condition
	 * @return
	 */
	public MatcherResult route(String vtab,ComparativeMapChoicer choicer,List<Object> args,boolean needSourceKey);
	
	/**
	 * 多套规则支持(TDDL使用)
	 * @param vtab
	 * @param condition
	 * @return
	 */
	public Map<String, MatcherResult> routeMVer(
			String vtab,
			ComparativeMapChoicer choicer,List<Object> args,boolean needSourceKey);
	
	/**
	 * 指定一套规则计算
	 * @param vtab
	 * @param condition
	 * @return
	 */
	public MatcherResult route(String vtab,ComparativeMapChoicer choicer,List<Object> args,boolean needSourceKey,VirtualTableRoot specifyVtr);
	
	/**
	 * 新旧规则计算并比较,不带目标库判定
	 * 
	 * @param vtab
	 * @param conditionStr
	 * @return
	 */
	public MatcherResult routeMVerAndCompare(SqlType sqlType,
			String vtab, ComparativeMapChoicer choicer,List<Object> args,boolean needSourceKey)throws ResultCompareDiffException;
	
	/**
	 * 新旧规则计算并比较,带目标库判定
	 * 
	 * @param vtab
	 * @param conditionStr
	 * @return
	 */
	public MatcherResult routeMVerAndCompare(SqlType sqlType,
			String vtab,  ComparativeMapChoicer choicer,List<Object> args,boolean needSourceKey,String oriDb,String oriTable)throws ResultCompareDiffException; 
}
