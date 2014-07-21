package com.taobao.tddl.sqlobjecttree;

import java.util.Map;


public class MaxWrapper extends PageWrapperCommon{

	public MaxWrapper(Object obj) {
		super(obj);
	}
	public String getSqlReturn(Number limitFrom, Number limitTo) {
		return limitTo.toString();
	}

	public void modifyParam(Number limitFrom, Number limitTo,Map<Integer,Object> modifiedMap) {
		Object obj=null;
		if(limitTo instanceof Long){
			obj=(Long)limitTo;
		}else if(limitTo instanceof Integer){
			obj=(Integer)limitTo;
		}else{
			throw new IllegalArgumentException("只支持int long的情况");
		}
		modifiedMap.put(index,obj);
	}
}
