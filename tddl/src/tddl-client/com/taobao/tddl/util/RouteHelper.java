/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.taobao.tddl.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.client.RouteCondition.ROUTE_TYPE;
import com.taobao.tddl.common.client.ThreadLocalString;
import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.interact.sqljep.ComparativeAND;
import com.taobao.tddl.interact.sqljep.ComparativeOR;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.AdvanceCondition;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.AdvancedDirectlyRouteCondition;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.DirectlyRouteCondition;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.SimpleCondition;

/**
 * 方便业务调用直接接口的方法
 * s
 * @author shenxun
 */
public class RouteHelper {
    public static final int EQ = Comparative.Equivalent;
    public static final int GT = Comparative.GreaterThan;
    public static final int LT = Comparative.LessThan;
    public static final int GTE = Comparative.GreaterThanOrEqual;
    public static final int LTE = Comparative.LessThanOrEqual;


    /**
     * 直接在某个库上,执行一条sql
     * 这时候TDDL只做两件事情，第一个是协助判断是否在事务状态。
     * 第二个事情是，进行表名的替换。
     *
     * @param dbIndex dbIndex列表
     * @param logicTable 逻辑表名
     * @param table 实际表名
     * @param routeType 决定这个hint是在连接关闭的时候清空，还是在执行时就清空
     */
    public static void executeByDBAndTab(
            String dbIndex,String logicTable,
            ROUTE_TYPE routeType,String... tables) {
        DirectlyRouteCondition condition = new DirectlyRouteCondition();
        if(tables == null){
        	throw new IllegalArgumentException("tables is null");
        }
        for(String table : tables){
        	 condition.addATable(table);
        }

        condition.setVirtualTableName(logicTable);
        condition.setDBId(dbIndex);
        condition.setRouteType(routeType);
        ThreadLocalMap.put(ThreadLocalString.DB_SELECTOR, condition);
    }
    /**
     * 直接在某个库上,执行一条sql
     * 这时候TDDL只做两件事情，第一个是协助判断是否在事务状态。
     * 第二个事情是，进行表名的替换。
     *
     * @param dbIndex dbIndex列表
     * @param logicTable 逻辑表名
     * @param table 实际表名
     */
    public static void executeByDBAndTab( String dbIndex,String logicTable,
            String... table){
    	executeByDBAndTab(dbIndex, logicTable,ROUTE_TYPE.FLUSH_ON_EXECUTE,table);
    }

    /**
     * 直接在某个库上,执行一条sql，并允许进行多组表名的替换，主要目标是join的sql
     *
     * tddl should do :
     * 1. 判断事务是否可执行。
     * 2. 将多组表名进行替换。
     * @param dbIndex dbIndex列表
     * @param tableMap 源表名->目标表名的map
     */
    public static void executeByDBAndTab(String dbIndex,
            Map<String/*original table*/,String/*advanced table*/> tableMap) {
    	executeByDBAndTab(dbIndex, tableMap, ROUTE_TYPE.FLUSH_ON_EXECUTE);
    }

    /**
     * 直接在某个库上,执行一条sql，并允许进行多组表名的替换，主要目标是join的sql
     *
     * @param dbIndex dbIndex列表
     * @param tableMap 源表名->目标表名的map
     * @param routeType 决定这个hint是在连接关闭的时候清空，还是在执行时就清空
     */
    public static void executeByDBAndTab(String dbIndex,
            Map<String/*original table*/,String/*advanced table*/> tableMap,ROUTE_TYPE routeType){
    	AdvancedDirectlyRouteCondition condition = new AdvancedDirectlyRouteCondition();
    	condition.setDBId(dbIndex);
    	Map<String, List<Map<String, String>>> directlyShardTableMap
    		= new HashMap<String, List<Map<String,String>>>(2);
    	List<Map<String,String>> tables2BReplaced = new ArrayList<Map<String,String>>(1);
    	tables2BReplaced.add(tableMap);
    	directlyShardTableMap.put(dbIndex, tables2BReplaced);
    	condition.setShardTableMap(directlyShardTableMap);
    	condition.setRouteType(routeType);
        ThreadLocalMap.put(ThreadLocalString.DB_SELECTOR, condition);
    }


    /**
     * 直接在某个库上,执行一条sql，并允许进行多组表名的替换，主要目标是join的sql
     *
     * @param dbIndex dbIndex列表
     * @param tableMap 源表名->目标表名的map
     * @param routeType 决定这个hint是在连接关闭的时候清空，还是在执行时就清空
     */
    public static void executeByDBAndTab(String dbIndex,
            List<Map<String/*original table*/,String/*advanced table*/>> tableMap,ROUTE_TYPE routeType){
    	AdvancedDirectlyRouteCondition condition = new AdvancedDirectlyRouteCondition();
    	condition.setDBId(dbIndex);

    	Map<String, List<Map<String, String>>> directlyShardTableMap
		= new HashMap<String, List<Map<String,String>>>(2);
    	directlyShardTableMap.put(dbIndex, tableMap);
    	condition.setShardTableMap(directlyShardTableMap);
    	condition.setRouteType(routeType);
        ThreadLocalMap.put(ThreadLocalString.DB_SELECTOR, condition);
    }

    /**
     * 直接在某个库上,执行一条sql，并允许进行多组表名的替换，主要目标是join的sql
     *
     * @param dbIndex dbIndex列表
     * @param tableMap 源表名->目标表名的map
     * @param routeType 决定这个hint是在连接关闭的时候清空，还是在执行时就清空
     */
    public static void executeByDBAndTab(String dbIndex,
            List<Map<String/*original table*/,String/*advanced table*/>> tableMap){
    	executeByDBAndTab(dbIndex, tableMap, ROUTE_TYPE.FLUSH_ON_EXECUTE);
    }
    /**
     * 直接在某个库上,执行一条sql，并允许进行多组表名的替换，主要目标是join的sql
     *
     * @param dbIndex dbIndex列表
     * @param tableMap 源表名->目标表名的map
     * @param routeType 决定这个hint是在连接关闭的时候清空，还是在执行时就清空
     */
    public static void executeByDBAndTab(Map<String, List<Map<String, String>>> tableMap,ROUTE_TYPE routeType){
    	AdvancedDirectlyRouteCondition condition = new AdvancedDirectlyRouteCondition();
    	condition.setShardTableMap(tableMap);
    	condition.setRouteType(routeType);
        ThreadLocalMap.put(ThreadLocalString.DB_SELECTOR, condition);
    }
    public static void executeByDBAndTab(Map<String, List<Map<String, String>>> tableMap){
    	executeByDBAndTab(tableMap, ROUTE_TYPE.FLUSH_ON_EXECUTE);
    }
    /**
     * 根据db index 执行一条sql. sql就是你通过Ibatis输入的sql.
     *
     * 只做一件事情，就是协助判断是否需要进行事务
     *
     * @param dbIndex dbIndex列表
     * @param routeType 决定这个hint是在连接关闭的时候清空，还是在执行时就清空
     */
    public static void executeByDB(String dbIndex,ROUTE_TYPE routeType){
    	DirectlyRouteCondition condition = new DirectlyRouteCondition();
    	condition.setDBId(dbIndex);
    	condition.setRouteType(routeType);
    	ThreadLocalMap.put(ThreadLocalString.DB_SELECTOR, condition);
    }

    /**
     * 根据db index 执行一条sql. sql就是你通过Ibatis输入的sql.
     *
     * 只做一件事情，就是协助判断是否需要进行事务
     *
     * @param dbIndex dbIndex列表
     */
    public static void executeByDB(String dbIndex){
    	executeByDB(dbIndex,ROUTE_TYPE.FLUSH_ON_EXECUTE);
    }

    /**
     * 选择一个规则。
     *
     * 默认情况下有读写分离的场景中，MASTER对应主库，SLAVE对应读库。
     *
     * @param selector db index key
     */
    public static void selectKey(String selector){
    	//方法是一个，如果在dsMap中出现，那么就用dsMap中的key所对应的ds
    	//如果没在dsMap,那么会查看规则map中是否有对应的规则。
    	executeByRule(selector,ROUTE_TYPE.FLUSH_ON_EXECUTE);
    }

    /**
     * 选择一个规则。
     *
     * @param ruleKey
     */
    public static void executeByRule(String ruleKey,ROUTE_TYPE routeType){
    	DirectlyRouteCondition condition = new DirectlyRouteCondition();
    	condition.setDBId(ruleKey);
    	condition.setRouteType(routeType);
    	ThreadLocalMap.put(ThreadLocalString.RULE_SELECTOR, condition);
    }

    /**
     * 选择一个规则。需要注意的是规则的key不能与数据库中的数据源重名。
     * 否则优先选择数据源
     *
     * 默认情况下有读写分离的场景中，MASTER对应主库，SLAVE对应读库。
     *
     * @param selector db index key
     * @param routeType 决定这个hint是在连接关闭的时候清空，还是在执行时就清空
     *
    */
    public static void selectKey(String selector,ROUTE_TYPE routeType){
    	executeByRule(selector,routeType);
    }

//    /**
//     * 执行一条sql,中间带有suffix.
//     * 类似@suffix@
//     * 或者@suffix,key@
//     *
//     * @param dbIndex
//     * @param suffix
//     */
//    public static void executeByDBAndTabSuffix(String dbIndex,
//    		String suffix) {
//    	AdvancedDirectlyRouteCondition condition = new AdvancedDirectlyRouteCondition();
//    	condition.setSuffixModel(true);
//    	condition.setSuffix(suffix);
//        ThreadLocalMap.put(ThreadLocalString.DB_SELECTOR, condition);
//    }

    /**
     * 根据条件选择数据库进行执行sql
     *
     * @param logicTable
     * @param key
     * @param comp
     */
    public static void executeByCondition(
            String logicTable, String key, Comparable<?> comp) {
       executeByCondition(logicTable, key, comp,ROUTE_TYPE.FLUSH_ON_EXECUTE);
    }

    public static void executeByCondition(
            String logicTable, String key, Comparable<?> comp,ROUTE_TYPE routeType){
    	 SimpleCondition simpleCondition = new SimpleCondition();
         simpleCondition.setVirtualTableName(logicTable);
         simpleCondition.put(key, comp);
         simpleCondition.setRouteType(routeType);
         ThreadLocalMap.put(ThreadLocalString.ROUTE_CONDITION, simpleCondition);
    }

    public static void executeWithParallel(boolean useParallel){
    	ThreadLocalMap.put(ThreadLocalString.PARALLEL_EXECUTE,useParallel);
    }

    public static void executeByCondition(
            String logicTable, String key, Comparable<?> comp,String ruleSelector) {
    	executeByCondition(logicTable, key, comp);
    	selectKey(ruleSelector);
    }

    public static void executeByCondition(
            String logicTable, String key, Comparable<?> comp,String ruleSelector
            ,ROUTE_TYPE routeType) {
    	executeByCondition(logicTable, key, comp,routeType);
    	selectKey(ruleSelector,routeType);
    }

    public static void executeByAdvancedCondition(
            String logicTable, Map<String, Comparable<?>> param
            ,ROUTE_TYPE routeType) {
    	AdvanceCondition condition = new AdvanceCondition();
        condition.setVirtualTableName(logicTable);
        for (Map.Entry<String, Comparable<?>> entry : param.entrySet()) {
            condition.put(entry.getKey(), entry.getValue());
        }
        condition.setRouteType(routeType);
        ThreadLocalMap.put(ThreadLocalString.ROUTE_CONDITION, condition);
    }

    public static void executeByAdvancedCondition(
            String logicTable, Map<String, Comparable<?>> param) {
        executeByAdvancedCondition(logicTable, param, ROUTE_TYPE.FLUSH_ON_EXECUTE);
    }

    public static void executeByAdvancedCondition(
            String logicTable, Map<String, Comparable<?>> param,String ruleSelector) {
    	executeByAdvancedCondition(logicTable, param);
    	selectKey(ruleSelector);
    }

    public static void executeByAdvancedCondition(
            String logicTable, Map<String, Comparable<?>> param,String ruleSelector
            ,ROUTE_TYPE routeType){
    	executeByAdvancedCondition(logicTable, param,routeType);
		selectKey(ruleSelector,routeType);
    }

    public static Comparative or(Comparative parent, Comparative target) {
        if (parent == null) {
            ComparativeOR or = new ComparativeOR();
            or.addComparative(target);
            return or;
        } else {
            if (parent instanceof ComparativeOR) {
                ((ComparativeOR) parent).addComparative(target);
                return parent;
            } else {
                ComparativeOR or = new ComparativeOR();
                or.addComparative(parent);
                or.addComparative(target);
                return or;
            }
        }
    }

    public static Comparative and(Comparative parent, Comparative target) {
        if (parent == null) {
            ComparativeAND and = new ComparativeAND();
            and.addComparative(target);
            return and;
        } else {
            if (parent instanceof ComparativeAND) {

                ComparativeAND and = ((ComparativeAND) parent);
                if (and.getList().size() == 1) {
                    and.addComparative(target);
                    return and;
                } else {
                    ComparativeAND andNew = new ComparativeAND();
                    andNew.addComparative(and);
                    andNew.addComparative(target);
                    return andNew;
                }

            } else {
                ComparativeAND and = new ComparativeAND();
                and.addComparative(parent);
                and.addComparative(target);
                return and;
            }
        }
    }
}
