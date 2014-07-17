package com.taobao.tddl.rule.ruleengine.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.sequence.Config;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.TabRule;

public class ModTabProvider extends CommonTableRuleProvider {
	private static final Log log = LogFactory.getLog(ModTabProvider.class);

	public static final String FUNCTION_NAME_MOD = "mod";

	protected Set<String> addAEqComparabToXXXFix(TabRule tab,
			String vTabName, Comparative comparative,int offset, Config config){
		Set<String> temp=new HashSet<String>();
		String expression = tab.getExpFunction();
		if (expression.startsWith(FUNCTION_NAME_MOD)) {
			long value = Long.parseLong(comparative.getValue().toString());
			int modulus = Integer.parseInt(expression.substring(FUNCTION_NAME_MOD.length()));

			if (log.isDebugEnabled()) {
				StringBuilder buffer = new StringBuilder();
				buffer.append("expression = ").append(expression).append(", ");
				buffer.append("value = ").append(value).append(", ");
				buffer.append("modulus = ").append(modulus);

				log.debug(buffer.toString());
			}
			String n=processOne(Integer.valueOf((int) (value % modulus))+offset, tab, vTabName);
			if(n!=null){
				temp.add(n);
			}
		} else {
			throw new IllegalArgumentException("invalid mod expression: " + expression);
		}
		return temp;
	}
	protected List<Object> getXxxfixlist(Comparative start, Comparative end,int offset, TabRule tab) {
		List<Object> li=new ArrayList<Object>();
		long st = Long.parseLong(start.getValue().toString());
		long ed = Long.parseLong(end.getValue().toString());
		int startType = getType(start);
		int endType = getType(end);
		String expression = tab.getExpFunction();
		int modulus = Integer.parseInt(expression.substring(FUNCTION_NAME_MOD.length()));
		if(startType == LESS_GREAT){
			st++;
		}
		if(endType == LESS_OR_EQUAL_GREAT_OR_EQUAL){
			ed++;
		}
		for(long i=st;i<ed;i++){
			li.add(Integer.valueOf((int) (i % modulus)+offset));
		}
		return li;
	}

}
