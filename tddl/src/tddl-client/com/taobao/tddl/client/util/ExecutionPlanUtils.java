package com.taobao.tddl.client.util;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.client.jdbc.RealSqlContext;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;

/**
 * 执行计划附加的工具类
 * @author JIECHEN
 *
 */
public class ExecutionPlanUtils {
	
	/**
	 * 是否在单库单表上执行
	 * @param executionPlan
	 * @return
	 */
	public static boolean isOneSqlPlan(ExecutionPlan executionPlan){
		Map<String, List<RealSqlContext>> sqlMap = getSqlMap(executionPlan);
		if(sqlMap.size() > 1) return false;
		boolean result = false;
		for(List<RealSqlContext> groupSqlContext : sqlMap.values()){
			result = groupSqlContext.size() == 1;
			break;
		}
		return result;
	}
	
	private static Map<String, List<RealSqlContext>> getSqlMap(ExecutionPlan executionPlan){
		if(executionPlan == null) throwUtilException();
		Map<String, List<RealSqlContext>> result = executionPlan.getSqlMap();
		if(result == null || result.size() ==0) throwUtilException();
		return result;
	}
	
	private static void throwUtilException(){
		throw new IllegalArgumentException("nothing in executionPlan, illegal argument.");
	}

}
