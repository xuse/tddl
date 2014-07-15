//package com.taobao.tddl.sqlobjecttree.outputhandlerimpl;
//
//import java.util.Map;
//
//public class MaxRangeReplaceHandler extends RangePlaceHandler{
//
//	protected void modifyParam(int index ,Number skip,Number max,Map<Integer,Object> modifiedMap) {
//		Object obj=null;
//		if(max instanceof Long){
//			obj=(Long)max;
//		}else if(max instanceof Integer){
//			obj=(Integer)max;
//		}else{
//			throw new IllegalArgumentException("只支持int long的情况");
//		}
//		modifiedMap.put(index,obj);
//	}
//	protected String getSqlReturn(Number skip,Number max) {
//		return max.toString();
//	}
//
//}
