//package com.taobao.tddl.sqlobjecttree.outputhandlerimpl;
//
//import java.util.Map;
//
//public class SkipReplaceHandler extends RangePlaceHandler {
//
//	@Override
//	protected void modifyParam(int index, Number skip, Number max,
//			Map<Integer, Object> modifiedMap) {
//		Object obj=null;
//		if(skip instanceof Long){
//			obj=(Long)skip;
//		}else if(skip instanceof Integer){
//			obj=(Integer)skip;
//		}else{
//			throw new IllegalArgumentException("只支持int long的情况");
//		}
//		modifiedMap.put(index, obj);
//	}
//
//	@Override
//	protected String getSqlReturn(Number skip, Number max) {
//		if(skip instanceof Long){
//			return String.valueOf(((Long)skip).longValue());
//		}else if(skip instanceof Integer){
//			return String.valueOf(((Integer)skip).intValue());
//		}else{
//			throw new IllegalArgumentException("只支持int long的情况");
//		}
//	}
//
//}
