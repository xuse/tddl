package com.taobao.tddl.rule.ruleengine.rule;

public class RuleExecutionContext {
	//TODO:这个应该放到具体实现中去
	public static final String ITEM_NUM_TO_USER_NUM_NAMESPACE ="ITEMNUM2USERNUM";
	public static final String ITEM_CHAR_TO_USER_NUM_AND_ITEM_NUM_NAMESPACE ="ITEMNUM2USERNUM";
	public static  final  String ITEM_NUM_COLUMN="ITEM_NUM";
	public static final String USER_NUM_COLUMN="USER_NUM";
	private MapCache mapCache ;

	public MapCache getMapCache() {
		return mapCache;
	}

	public void setMapCache(MapCache mapCache) {
		this.mapCache = mapCache;
	}
	
}
