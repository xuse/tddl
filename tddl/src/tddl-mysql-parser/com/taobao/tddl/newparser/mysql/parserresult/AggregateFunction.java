package com.taobao.tddl.newparser.mysql.parserresult;

import java.util.List;

import com.taobao.tddl.sqlobjecttree.GroupFunctionType;

public class AggregateFunction {
    public final GroupFunctionType type;
    public final boolean isDistinct;
    public final List<Object> args;
    public final String functionStr;
    
    public AggregateFunction(GroupFunctionType type,boolean isDistinct,List<Object> args,String functionStr){
    	this.type=type;
    	this.isDistinct=isDistinct;
    	this.args=args;
    	this.functionStr=functionStr;
    }
}
