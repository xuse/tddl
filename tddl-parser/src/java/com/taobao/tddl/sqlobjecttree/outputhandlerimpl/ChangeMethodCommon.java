//package com.taobao.tddl.sqlobjecttree.outputhandlerimpl;
//
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import com.taobao.tddl.sqlobjecttree.IndexWrapper;
//import com.taobao.tddl.sqlobjecttree.MaxWrapper;
//import com.taobao.tddl.sqlobjecttree.OutputHandler;
//import com.taobao.tddl.sqlobjecttree.PageWrapperCommon;
//import com.taobao.tddl.sqlobjecttree.RangeWrapper;
//import com.taobao.tddl.sqlobjecttree.ReplacableWrapper;
//import com.taobao.tddl.sqlobjecttree.SkipWrapper;
//import com.taobao.tddl.sqlobjecttree.oracle.OraclePageWrapper;
//
//public abstract class ChangeMethodCommon implements OutputHandler{
//
//	public String handle(Map<String,String> table, List<Object> args,
//			List<Object> modifiableTableName, Set<String> originalTable, Number skip,
//			Number max,Map<Integer, Object> changeParam) {
//		if(allowChangePageNumber()){
//		PageWrapperCommon skipPage=null;
//		//oracle的max或者mysql的range都可以被注入到这里，因为max和range不会同时出现
//		//这部分代码的作用在于先找到最大的起始数值，和结束数值。
//		PageWrapperCommon maxOrMaxPage=null;
//		for(Object obj:modifiableTableName){
//			if(obj instanceof SkipWrapper){
//				if(skipPage==null){
//					skipPage=(SkipWrapper)obj;
//				}else if(skipPage.getVal(args)<((SkipWrapper)obj).getVal(args)){
//					//当前值大于snapshot中的值时，与mySelect,Select MyUpdate,Update……里面的参数一致。
//					skipPage=(SkipWrapper)obj;
//				}else{
//					//当前值小于等于snap中的值，什么也不做。
//				}
//			}else if(obj instanceof MaxWrapper||obj instanceof RangeWrapper){
//				if(maxOrMaxPage==null){
//					maxOrMaxPage=(PageWrapperCommon)obj;
//				}else if(maxOrMaxPage.getVal(args)<((PageWrapperCommon)obj).getVal(args)){
//					//当前值大于snapshot中的值时，与mySelect,Select MyUpdate,Update……里面的参数一致
//					maxOrMaxPage=(PageWrapperCommon)obj;
//				}else{
//					//当前值小于等于snap中的值，什么也不做。
//				}
//			}else{
//				//String 其他的情况
//			}
//		}
//		if(skipPage!=null){
//			skipPage.setCanBeChanged(true);
//		}
//		if(maxOrMaxPage!=null){
//			maxOrMaxPage.setCanBeChanged(true);
//		}
//		
//		}
//		//替换并输出
//		StringBuilder sb=new StringBuilder();
//		for(Object obj:modifiableTableName){
//			if(obj instanceof String){
//					//正常String
////					sb.append(getTable(table,originalTable));
//					sb.append(obj.toString());
//			}else if(obj instanceof PageWrapperCommon){
//				//处理分页
////				needAppendtableName=false;
//				PageWrapperCommon common=(PageWrapperCommon)obj;
//				sb.append(common.getSqlEle(skip, max, args,allowChangePageNumber(),changeParam));
//			}else if(obj instanceof ReplacableWrapper){
//				//处理index，表名
//				sb.append(getReplacedString(table,(ReplacableWrapper)obj));
//				
//			} else if(obj instanceof OraclePageWrapper) {
//				sb.append(((OraclePageWrapper)obj).getSqlEle(skip, max, args,allowChangePageNumber(),changeParam));
//			}else{
//				throw new IllegalStateException("should not be here");
//			}
//			
//		}
//		return sb.toString();
//	}
//
//	protected abstract String getReplacedString(Map<String,String> targetTableName,ReplacableWrapper replacedObj);
//	protected abstract boolean allowChangePageNumber();
//	
//	protected static final String mySQL_getReplacedString_changeTable_version(
//			String targetTableName, ReplacableWrapper replacedObj) {
//		switch (replacedObj.getType()) {
//		case INDEX:
//			return getNewIndex(targetTableName, replacedObj);
//		case TABLE:
//			return  targetTableName;
//		case VERSION_COL:
//			return ",sync_version=ifnull(sync_version,0) + 1 ";
//		default:
//			throw new IllegalArgumentException("should not be here");
//		}
//	}
//	protected static final String getReplacedString_changeTable(Map<String, String> targetTableName,
//			ReplacableWrapper replacedObj) {
//		switch (replacedObj.getType()) {
//		case INDEX:
//			return getNewIndex(targetTableName, replacedObj);
//		case TABLE:
//			return  targetTableName.get(replacedObj.getReplacedStr());
//		case VERSION_COL:
//			return "";
//		default:
//			throw new IllegalArgumentException("should not be here");
//		}
//	}
//	private static String getNewIndex(Map<String, String> targetTableName,
//			ReplacableWrapper replacedObj) {
//		IndexWrapper wa=(IndexWrapper)replacedObj;
//		String temp=wa.getReplacedStr().replace(wa.getOriginalTableName(), targetTableName.get(wa.getOriginalTableName()));
//		return temp;
//	}
//	protected static final  String ora_getReplacedString_changeTable_version(
//			String targetTableName, ReplacableWrapper replacedObj) {
//		switch (replacedObj.getType()) {
//		case INDEX:
//			return getNewIndex(targetTableName, replacedObj);
//		case TABLE:
//			return  targetTableName;
//		case VERSION_COL:
//			return ",sync_version=nvl(sync_version,0) + 1 ";
//		default:
//			throw new IllegalArgumentException("should not be here");
//		}
//	}
//
//
//}
