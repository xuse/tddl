//Copyright(c) Taobao.com
package com.taobao.tddl.rule.le;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.interact.bean.ComparativeMapChoicer;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.rule.le.util.ComparativeStringAnalyser;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a> 
 * @version 1.0
 * @since 1.6
 * @date 2011-3-29上午10:53:21
 */
public class SimpleComparativeMapChoicer implements ComparativeMapChoicer {
	private static final Log log=LogFactory.getLog(SimpleComparativeMapChoicer.class);
	private Map<String,Comparative> comparativeMap=new HashMap<String, Comparative>();
	
	public Map<String, Comparative> getColumnsMap(List<Object> arguments,
			Set<String> partnationSet) {
		return this.comparativeMap;
	}
	
	public void addComparatives(String conditionStr){
		if(conditionStr!=null){
			this.comparativeMap=ComparativeStringAnalyser.decodeComparativeString2Map(conditionStr);
		}else{
			log.info("未传入分库分表条件和值");
		}
	}

	public Comparative getColumnComparative(List<Object> arguments,
			String colName) {
		return this.comparativeMap.get(colName);
	}
}
