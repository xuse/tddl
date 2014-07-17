package com.taobao.tddl.rule.bean;

import com.taobao.tddl.interact.rule.bean.ExtraParameterContext;
import com.taobao.tddl.rule.ruleengine.rule.AbstractMappingRule;
import com.taobao.tddl.rule.ruleengine.rule.ResultAndMappingKey;

@Deprecated
public class MappingRule extends AbstractMappingRule {

	@Override
	protected Object get(String targetKey, String sourceKey, Object sourceValue) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultAndMappingKey evalueateSimpleColumAndValue(String column,
			Object value, ExtraParameterContext extraParameterContext) {
		// TODO Auto-generated method stub
		return null;
	}
//	DatabaseBasedMapping mappingHandler ;
//	//private String mappingRuleBeanId ;
//	
//	public DatabaseBasedMapping getMappingHandler() {
//		return mappingHandler;
//	}
//
//	public void setMappingHandler(DatabaseBasedMapping mappingRule) {
//		this.mappingHandler = mappingRule;
//	}
//	
//	@Override
//	protected void initInternal() {
//		if(mappingHandler == null){
//			throw new IllegalArgumentException("mapping rule is null");
//		}
//		mappingHandler.initInternal();
//		super.initInternal();
//	}
//	/* (non-Javadoc)
//	 * @see com.taobao.tddl.rule.ruleengine.rule.AbstractMappingRule#get(java.lang.String, java.lang.String, java.lang.Object)
//	 */
//	protected Object get(String targetKey, String sourceKey, Object sourceValue) {
//		/*modified by shenxun:这里以前是依托于mappingRule里面的key的，但现在因为mappingRule和
//		 * DatabaseBasedMappingRule分开了，因此会出现MappingRule与TairBasedMappingRUle数量不同的问题。
//		 * 这样出现一个问题就是数据库中的列名和业务在不同表内输入的列名是不同的从sql里选出来的一列数据，需要映射到映射表内的另外一套数据中去。
//		 * 如auction_num_id 在auction_auctions ,image ,spu 中有三种不同的列名。
//		 * 但对应到数据库的列名却是唯一的。因此需要有一层映射而不能直接使用parameter里带有的列名。
//		 * 
//		 */
//		return mappingHandler.get(targetKey, sourceValue);
//	}
//
//	@Override
//	public ResultAndMappingKey evalueateSimpleColumAndValue(String column,
//			Object value, ExtraParameterContext extraParameterContext) {
//		return null;
//	};
	
}
