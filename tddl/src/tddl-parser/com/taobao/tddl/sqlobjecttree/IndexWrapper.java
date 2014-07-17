package com.taobao.tddl.sqlobjecttree;

import com.taobao.tddl.sqlobjecttree.outputhandlerimpl.ReplaceHandler;

public class IndexWrapper implements ReplacableWrapper{
	String originalTableName;
	public void visit(ReplaceHandler handler) {
		
	}
	public String getOriginalTableName() {
		return originalTableName;
	}
	public void setOriginalTableName(String originalTableName) {
		this.originalTableName = originalTableName;
	}
	public String getReplacedStr() {
		return originalTableName;
	}

}
