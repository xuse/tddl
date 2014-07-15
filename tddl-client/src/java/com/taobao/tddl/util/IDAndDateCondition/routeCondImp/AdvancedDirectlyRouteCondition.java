/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.taobao.tddl.util.IDAndDateCondition.routeCondImp;

import com.taobao.tddl.client.RouteCondition;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 用于直接执行sql
 * @author shenxun
 */
public class AdvancedDirectlyRouteCondition extends DirectlyRouteCondition implements RouteCondition{
	Map<String/*db index */, List<Map<String/*original table*/, String/*target table*/>>> shardTableMap = Collections.emptyMap();

	public Map<String, List<Map<String, String>>> getShardTableMap() {
		return shardTableMap;
	}
	public void setShardTableMap(
			Map<String, List<Map<String, String>>> directlyShardTableMap) {
		this.shardTableMap = directlyShardTableMap;
	}
	
//	这个方法不能重写父类里面的方法，否则将不支持dbId的设置，
//  直接导致无法使用AdvancedDirectlyRouteCondition
//  Edit by junyu 
//  public void setDBId(String dbId){
//		throw new IllegalArgumentException("advance not support this");
//	}
	public void addATable(String table) {
		throw new IllegalArgumentException("advance not support this");
	}
	@Override
	public void setTables(Set<String> tables) {
		throw new IllegalArgumentException("advance not support this");
	}


}
