package com.taobao.tddl.newparser.mysql.parserresult;

public class Table {
    public final String nameUpperCase;
    public final String schema;
    public final String alias;
    public Table(String nameUpperCase,String schema,String alias){
    	this.nameUpperCase=nameUpperCase;
    	this.schema=schema;
    	this.alias=alias;
    }
}
