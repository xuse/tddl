//Copyright(c) Taobao.com
package com.taobao.tddl.interact.rule.virtualnode;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.taobao.tddl.interact.rule.util.VirturalNodeUtil;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a> 
 * @version 1.0
 * @since 1.6
 * @date 2011-6-2下午03:12:39
 */
public class DBTableMap extends WrappedLogic implements VirtualNodeMap{
    private ConcurrentHashMap<String/*1,2,3... math like integer,long*/,String/*group_0*/> dbContext=new ConcurrentHashMap<String,String>();
    private Map<String,String> dbTableMap=new HashMap<String, String>();
    private String logicTable;
    
    private Map<String/*group*/, PartitionFunction> parFuncMap;
    
    private volatile boolean isInit=false;
    
    public synchronized void init(){
      	if(isInit){
    		return;
    	}
    	
    	isInit=true;
    	
    	this.initDbTableMap(this.dbTableMap);
    	
    	if(null!=dbTableMap&&dbTableMap.size()>0){
    		dbContext=(ConcurrentHashMap<String, String>) VirturalNodeUtil.extraReverseMap(dbTableMap);
    		addLogicTableAndSplitorToKey();
    	}else{
    		throw new IllegalArgumentException("no dbTableMap config at all");
    	}
    }
    
    public void addLogicTableAndSplitorToKey(){
        if(tableSlotKeyFormat!=null){
        	ConcurrentHashMap<String,String> reKeyMap=new ConcurrentHashMap<String,String>();
    		for(Map.Entry<String, String> entry:dbContext.entrySet()){
    			String newKey=super.wrapValue(entry.getKey());
    			reKeyMap.put(newKey, entry.getValue());
    		}
    		dbContext.clear();
    		dbContext=reKeyMap;
        }else if(this.logicTable!=null){
    		ConcurrentHashMap<String,String> reKeyMap=new ConcurrentHashMap<String,String>();
    		for(Map.Entry<String, String> entry:dbContext.entrySet()){
    			StringBuilder sb=new StringBuilder(this.logicTable);
    			sb.append(tableSplitor);
    			sb.append(entry.getKey());
    			reKeyMap.put(sb.toString(), entry.getValue());
    		}
    		dbContext.clear();
    		dbContext=reKeyMap;
    	}else{
    	    throw new RuntimeException("TableRule no tableSlotKeyFormat property and logicTable is null");
    	}
    }

	public String getValue(String tableSuffix){
		return dbContext.get(tableSuffix);
    }

	public void setDbTableMap(Map<String, String> dbTableMap) {
		this.dbTableMap = dbTableMap;
	}

	public void setLogicTable(String logicTable) {
		this.logicTable = logicTable;
	}

    public void setParFuncMap(Map<String, PartitionFunction> parFuncMap) {
        this.parFuncMap = parFuncMap;
    }
    
    private void initDbTableMap(Map<String, String> dbTableMap) {
        if(this.parFuncMap == null || this.parFuncMap.isEmpty()) {
            return;
        }
        
        for(String key: this.parFuncMap.keySet()) {
            PartitionFunction parFunc = this.parFuncMap.get(key);
            String value = this.buildDbTableMapValue(parFunc);
            dbTableMap.put(key, value);
        }
    }
	
    private String buildDbTableMapValue(PartitionFunction parFunc) {
        StringBuilder valueBuilder = new StringBuilder();
        int[] partitionCount = parFunc.getCount();
        int[] partitionLength = parFunc.getLength();
        int firstValue = parFunc.getFirstValue();
        
        for(int i = 0; i < partitionCount.length; i++) {
            for(int j = 0; j < partitionCount[i]; j++) {
                int key = firstValue + partitionLength[i];
                firstValue = key;
                valueBuilder.append(key);
                valueBuilder.append(",");
            }
        }
        
        valueBuilder.deleteCharAt(valueBuilder.length() - 1);
        
        return valueBuilder.toString();
    }
    
}
