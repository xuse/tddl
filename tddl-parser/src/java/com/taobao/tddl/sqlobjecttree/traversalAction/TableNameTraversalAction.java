package com.taobao.tddl.sqlobjecttree.traversalAction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.taobao.tddl.sqlobjecttree.TableName;
import com.taobao.tddl.sqlobjecttree.common.TableNameImp;

public class TableNameTraversalAction implements TraversalSQLAction {
	private Set<String> snap=new HashSet<String>(1);
	public void actionProformed(TraversalSQLEvent event) {
		List<TableName> tbNames=event.getCurrStatement().getTbNames();
		for (TableName tbName : tbNames) {
			if(tbName instanceof TableNameImp){
				
				snap.addAll(tbName.getTableName());
			}
		}
	}
	public Set<String> getTableName(){
		return snap;
	}

}
