package com.taobao.tddl.rule.ruleengine;

import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.exception.checked.TDLCheckedExcption;
import com.taobao.tddl.common.sequence.Config;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.TabRule;

public interface TableRuleProvider {

	/**
	 * @deprecated 只用于测试用了
	 * 获取表名列表 
	 * 目前不支持的情况如下：
	 * and关系只支持两个组合在一起。目前不支持的关系有这样4类：
	 * <p>1.多个and连续组成的条件</p>
	 * <p>如：假设一个column为id.
	 * id > x and id < y and id > z ……这样的连续id组成的条件队列</p>
	 * <p>return:会抛出异常</p>
	 * <p>2.不同的column组成的条件 </p>
	 * <p>return:本身都不支持。</p>
	 * <p>3.or有优先级的条件组合。</p>
	 * <p>如：假设一个column为id.
	 * id > x and ( id = y or id =z )</p>
	 * <p>return :抛出异常。</p>
	 * <p>4.and关系中是两个关系，但是是这样的关系：
	 * id > x and id=x.</p>
	 * <p>return:抛出异常：“and有两个，但至少有一个条件为等于”。</p>
	 * 
	 * 其余的条件都可以正确识别
	 * 
	 * @param row Comparable列
	 * @param position 列中参数对应的位置
	 * @param tab 载入的表名规则Bean.
	 * @param vTabName 虚拟表名
	 * @return
	 * @throws TDLCheckedExcption
	 * 
	 */
	@Deprecated 
	public Set<String> getTables(Comparable<?>[] row,
			Map<String, Integer> position, TabRule tab, String vTabName)
			throws TDLCheckedExcption;
	/**
	 * 获取表名列表 
	 * 目前不支持的情况如下：
	 * and关系只支持两个组合在一起。目前不支持的关系有这样4类：
	 * <p>1.多个and连续组成的条件</p>
	 * <p>如：假设一个column为id.
	 * id > x and id < y and id > z ……这样的连续id组成的条件队列</p>
	 * <p>return:会抛出异常</p>
	 * <p>2.不同的column组成的条件 </p>
	 * <p>return:本身都不支持。</p>
	 * <p>3.or有优先级的条件组合。</p>
	 * <p>如：假设一个column为id.
	 * id > x and ( id = y or id =z )</p>
	 * <p>return :抛出异常。</p>
	 * <p>4.and关系中是两个关系，但是是这样的关系：
	 * id > x and id=x.</p>
	 * <p>return:抛出异常：“and有两个，但至少有一个条件为等于”。</p>
	 * @param map 参数key value对
	 * @param tab 表规则
	 * @param vTabName 虚拟表名
	 * @param config pk配置文件
	 * @return
	 * @throws TDLCheckedExcption
	 */
	public Set<String> getTables(Map<String, Comparative> map, TabRule tab, String vTabName, Config config)
			throws TDLCheckedExcption;
}
