package com.taobao.tddl.sqlobjecttree;

import java.util.List;

/**
 * 
 * @author junyu
 *
 */
public class InExpressionObject {
    public final String columnName;
    public final String alias;
    public final List<Integer>  bindVarIndexs;
    //statement case,like: id in(1,2,3,4)
    public final List<Object> bindVarValues;
    public final String  inStr;
    
    public InExpressionObject(String columnName,String alias,List<Integer> bindVarIndexs,List<Object> bindVarValues,String inStr){
    	this.columnName=columnName;
    	this.alias=alias;
    	this.bindVarIndexs=bindVarIndexs;
    	this.bindVarValues=bindVarValues;
    	this.inStr=inStr;
    }
}
