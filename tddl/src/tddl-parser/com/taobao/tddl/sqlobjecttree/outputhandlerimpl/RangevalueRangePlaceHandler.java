//package com.taobao.tddl.sqlobjecttree.outputhandlerimpl;
//
//import java.util.Map;
//
//import com.taobao.tddl.sqlobjecttree.PageWrapperCommon;
//
///**
// * 对应 limit m , n 中的n 需要替换的情况
// * @author shenxun
// *
// */
//public class RangevalueRangePlaceHandler extends RangePlaceHandler{
//
//
//	private long getSubLong(Number skip, Number max) {
//		return (max.longValue() - skip.longValue());
//	}
//
//	private int getSubInt(Number skip, Number max) {
//		return (max.intValue() - skip.intValue());
//	}
//	
//	/**
//	 * 换值，对应在绑定变量里出现m,n的情况
//	 * @param index
//	 * @param limitFrom
//	 * @param limitTo
//	 * @param modifiedMap
//	 */
//	protected void modifyParam(int index ,Number skip, Number max,Map<Integer,Object> modifiedMap) {
//		if (skip instanceof Long || max instanceof Long) {
//			modifiedMap.put(index, getSubLong(skip, max));
//		} else if (skip instanceof Integer && max instanceof Integer) {
//			modifiedMap.put(index, getSubInt(skip, max));
//		} else {
//			throw new IllegalArgumentException("只支持int long的情况");
//		}
//	}
//
//	protected String getSqlReturn(Number skip, Number max) {
//		if (skip instanceof Long || max instanceof Long) {
//			return String.valueOf(getSubLong(skip, max));
//		} else if (skip instanceof Integer && max instanceof Integer) {
//			return String.valueOf(getSubInt(skip, max));
//		} else {
//			throw new IllegalArgumentException("只支持int long的情况");
//		}
//
//	}
//
//}
