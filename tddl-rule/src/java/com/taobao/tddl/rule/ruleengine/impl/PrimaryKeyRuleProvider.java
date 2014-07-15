package com.taobao.tddl.rule.ruleengine.impl;

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.exception.checked.TDLCheckedExcption;
import com.taobao.tddl.common.exception.runtime.NotSupportException;
import com.taobao.tddl.common.sequence.Config;
import com.taobao.tddl.common.sequence.IDParse;
import com.taobao.tddl.common.sequence.IDParseFactory;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.TabRule;

/**
 * 用于处理pk字段分库。只用于pk字段中带有分库分表信息的时候
 * @author shenxun
 *
 */
public class PrimaryKeyRuleProvider extends CommonTableRuleProvider {
	private static final Log log = LogFactory.getLog(PrimaryKeyRuleProvider.class);
//	private static final IDParse<Long, Integer, Integer> idParse = new IDParseImp();
	private IDParseFactory factory=IDParseFactory.newInstance();
	public Set<String> getTables(Map<String, Comparative> map, TabRule tab, String tabName, Config config)
	throws TDLCheckedExcption {
		validTabRule(tab);
		Comparative comparable=null;
		comparable = map.get(tab.getPrimaryKey());
				
		return parseTableNameObj(tab, tabName, comparable,config);
	}

	protected Collection<String> getDefaultTabCollection(TabRule tab) {
		throw new NotSupportException("使用主键id的查询方式不支持除in以外的范围查询,或者没找到主键");
	}
	@SuppressWarnings("unchecked")
	protected Set<String> addAEqComparabToXXXFix(TabRule tab,
			String vTabName, Comparative comparative,int offset, Config config){
		Set<String> temp=new HashSet<String>();
		Comparable<?> comparable=comparative.getValue();
		long value =0;
		BigInteger bigInteger = null;
		if(comparable instanceof Long){
			value=(Long)comparable;
		}else if(comparable instanceof Integer){
			value=((Integer)comparable).longValue();
		} else if(comparable instanceof BigInteger) {
			bigInteger = (BigInteger)comparable;
		}else{
			throw new IllegalStateException("只支持int long");
		}
		Integer tableArg = null;
		Object detachIDObj;
		if (bigInteger == null) {
			IDParse<Long, Integer, Integer> idParse = (IDParse<Long, Integer, Integer>) factory.createIDParse(config);
			com.taobao.tddl.common.sequence.IDParse.DetachID<Long, Integer, Integer> detachID = idParse
					.parse(value);
			tableArg = detachID.getTableArg();
			detachIDObj = detachID;
		} else {
			IDParse<BigInteger, Integer, Integer> idParse = (IDParse<BigInteger, Integer, Integer>) factory.createIDParse(config);
			com.taobao.tddl.common.sequence.IDParse.DetachID<BigInteger, Integer, Integer> detachID = idParse
					.parse(bigInteger);
			tableArg = detachID.getTableArg();
			detachIDObj = detachID;
		}
		if (log.isDebugEnabled()) {
			StringBuilder buffer = new StringBuilder();
			buffer.append("detachId = ").append(detachIDObj);
			log.debug(buffer.toString());
		}
		if (tableArg != null) {
			String n = processOne(Integer.valueOf((tableArg)) + offset, tab,
					vTabName);
			if (n != null) {
				temp.add(n);
			}
		}
		return temp;
	}
	protected List<Object> getXxxfixlist(Comparative start, Comparative end,int offset, TabRule tab) {
		throw new NotSupportException("使用主键id的查询方式不支持除in以外的范围查询");
	}

}
