package com.taobao.tddl.newparser.mysql.parserresult;

import java.util.ArrayList;
import java.util.List;

public class Join {
	private List<Table> joinTables = new ArrayList<Table>();
	private List<Column> joinColumns = new ArrayList<Column>();

	public void addJoinColumn(Column joinColumn) {
		this.joinColumns.add(joinColumn);
	}

	public List<Column> getJoinColumns() {
		return this.joinColumns;
	}

	public void addJoinTable(Table table){
		this.joinTables.add(table);
	}
	
	public List<Table> getJoinTables(){
		return this.joinTables;
	}
}
