package com.taobao.tddl.parser.hint;

import java.util.ArrayList;
import java.util.List;

public class HintTargetDB {
	final String dbID;
	public HintTargetDB(String dbID) {
		this.dbID=dbID;
	}
	final List<String>  tableNames=new ArrayList<String>();
	public void add(String tableName){
		tableNames.add(tableName);
	}
	public List<String> getTableName(){
		return tableNames;
	}
	
}
