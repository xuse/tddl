package com.taobao.tddl.sqlobjecttree.oracle;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.Column;
import com.taobao.tddl.sqlobjecttree.SelectUpdate;

public class OracleForUpdate implements SelectUpdate{
	Column col=null;
	private final static int DEFAULT_WAITTIME=-100;
	int waitTime=DEFAULT_WAITTIME;
	
	/**
	 * 0的时候约定为no wait
	 * 为其他值的时候为wait time;
	 * @param waitTime
	 */
	public void setWait(int waitTime){
		this.waitTime=waitTime;
	}
	public void setOfColumn(Column col){
		this.col=col;
	}

	
	public void appendSQL(StringBuilder sb) {
		sb.append(" FOR UPDATE");
		if(col!=null){
			sb.append(" OF ");
			col.appendSQL(sb);
		}
		if(waitTime!=DEFAULT_WAITTIME){
			if(waitTime==0){
				sb.append(" NO WAIT");
			}else{
				sb.append(" WAIT " + waitTime);
			}
		}
		
	}

	public StringBuilder regTableModifiable(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		sb.append(" FOR UPDATE");
		if(col!=null){
			sb.append(" OF ");
			sb=col.regTableModifiable(oraTabName, list, sb);
		}
		if(waitTime!=DEFAULT_WAITTIME){
			if(waitTime==0){
				sb.append(" NOWAIT");
			}else{
				sb.append(" WAIT " + waitTime);
			}
		}
		return sb;
	}
	
}
