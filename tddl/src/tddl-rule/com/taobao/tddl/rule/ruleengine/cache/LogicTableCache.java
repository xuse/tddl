//package com.taobao.tddl.rule.ruleengine.cache;
//
//import java.util.Set;
//
//import com.taobao.tddl.rule.ruleengine.entities.abstractentities.RuleChain;
//
///**
// * 逻辑表内一些需要cache的数据
// * @author shenxun
// *
// */
//public class LogicTableCache {
//	private  boolean isModifiable = true;
//	
//	public Set<RuleChain> getRuleChain() {
//		return ruleChain;
//	}
//	public void setRuleChain(Set<RuleChain> ruleChain) {
//		if(isModifiable)
//			this.ruleChain=ruleChain;
//		else
//			throw new IllegalArgumentException("不允许修改");
//	}
//	public boolean isModifiable() {
//		return isModifiable;
//	}
//	public void setModifiable(boolean isModifiable) {
//		this.isModifiable = isModifiable;
//	}
//	
//	
//	
//}
