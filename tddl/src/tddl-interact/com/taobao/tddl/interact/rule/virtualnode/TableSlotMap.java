//Copyright(c) Taobao.com
package com.taobao.tddl.interact.rule.virtualnode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.taobao.tddl.interact.monitor.TotalStatMonitor;
import com.taobao.tddl.interact.rule.util.VirturalNodeUtil;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a> 
 * @version 1.0
 * @since 1.6
 * @date 2011-6-2下午03:13:08
 */
public class TableSlotMap extends WrappedLogic implements VirtualNodeMap{
    private String logicTable;
	
    protected ConcurrentHashMap<String/*slot number*/,String/*table suffix*/> tableContext=new ConcurrentHashMap<String, String>(); 
    private Map<String/*table suffix*/,String/*slot string*/> tableSlotMap=new HashMap<String,String>();
	
    private PartitionFunction keyPartitionFunction;
    private PartitionFunction valuePartitionFunction;
    
    private volatile boolean isInit=false;
    
    public synchronized void init(){
    	if(isInit){
    		return;
    	}
    	
    	isInit=true;
    	
    	this.initTableSlotMap(tableSlotMap);
    	
    	if(null!=tableSlotMap&&tableSlotMap.size()>0){
    		tableContext=(ConcurrentHashMap<String, String>) VirturalNodeUtil.extraReverseMap(tableSlotMap);
    	}else{
    		throw new IllegalArgumentException("no tableSlotMap config at all");
    	}
    }
    
    public String getValue(String key){
    	String suffix=tableContext.get(key);
    	TotalStatMonitor.virtualSlotIncrement(buildLogKey(key));
    	if(super.tableSlotKeyFormat!=null){
    		return super.wrapValue(suffix);
    	}else if(logicTable!=null){
    		StringBuilder sb=new StringBuilder();
    		sb.append(logicTable);
    		sb.append(tableSplitor);
    		sb.append(suffix);
    		return sb.toString();
    	}else{
    		throw new RuntimeException("TableRule no tableSlotKeyFormat property and logicTable is null");
    	}
    }
    
    public String buildLogKey(String key){
    	if(logicTable!=null){
    	    StringBuilder sb=new StringBuilder(logicTable);
    	    sb.append("_slot_");
    	    sb.append(key);
    	    return sb.toString();
    	}else{
    		throw new RuntimeException("TableRule no logicTable at all,can not happen!!");
    	}
    }
    
	public void setTableSlotMap(Map<String, String> tableSlotMap) {
		this.tableSlotMap = tableSlotMap;
	}

	public void setLogicTable(String logicTable) {
		this.logicTable = logicTable;
	}

    public void setKeyPartitionFunction(PartitionFunction keyPartitionFunction) {
        this.keyPartitionFunction = keyPartitionFunction;
    }

    public void setValuePartitionFunction(PartitionFunction valuePartitionFunction) {
        this.valuePartitionFunction = valuePartitionFunction;
    }

    private void initTableSlotMap(Map<String, String> tableSlotMap) {
        if(this.keyPartitionFunction == null || this.valuePartitionFunction == null) {
            return;
        }
        
        List<String> keys = this.buildTableSlotMapKeys();
        List<String> values = this.buildTableSlotMapValues();
        
        for(int i = 0; i < keys.size(); i++) {
            tableSlotMap.put(keys.get(i), values.get(i));
        }
    }
    
    
    private List<String> buildTableSlotMapKeys() {
        List<String> result = new ArrayList<String>();
        int[] partitionCount = this.keyPartitionFunction.getCount();
        int[] partitionLength = this.keyPartitionFunction.getLength();
        int firstValue = this.keyPartitionFunction.getFirstValue();
        
        for(int i = 0; i < partitionCount.length; i++) {
            for(int j = 0; j < partitionCount[i]; j++) {
                int key = firstValue + partitionLength[i];
                firstValue = key;
                result.add(String.valueOf(key));
            }
        }
        
        return result;
    }
    
    
    private List<String> buildTableSlotMapValues() {
        List<String> result = new ArrayList<String>();
        int[] partitionCount = this.valuePartitionFunction.getCount();
        int[] partitionLength = this.valuePartitionFunction.getLength();
        int firstValue = this.valuePartitionFunction.getFirstValue();
        
        for(int i = 0; i < partitionCount.length; i++) {
            for(int j = 0; j < partitionCount[i]; j++) {
                int startValue = firstValue;
                int endValue = startValue + partitionLength[i] - 1;
                firstValue += partitionLength[i];
                result.add(String.valueOf(startValue) + "-" + String.valueOf(endValue));
            }
        }
        
        return result;
    }
	
}
