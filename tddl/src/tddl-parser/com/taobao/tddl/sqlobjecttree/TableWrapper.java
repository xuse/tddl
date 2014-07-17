package com.taobao.tddl.sqlobjecttree;

public class TableWrapper implements ReplacableWrapper {
	private String oriTable;
	public String getReplacedStr() {
		return oriTable;
	}
	public void setOriTable(String oriTable) {
		this.oriTable = oriTable;
	}
	
}
