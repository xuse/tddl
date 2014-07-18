package com.taobao.tddl.newparser.mysql.parserresult;

public class Column {
	/**
	 * 可能是alias，也可能是table
	 */
	public final String owner;
    public final String nameUpperCase;
    public final String alias;
    public Column(String nameUpperCase,String owner,String alias){
    	this.owner=owner;
    	this.nameUpperCase=nameUpperCase;
    	this.alias=alias;
    }
}
