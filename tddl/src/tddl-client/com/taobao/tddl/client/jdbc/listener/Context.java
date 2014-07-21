package com.taobao.tddl.client.jdbc.listener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.taobao.tddl.client.jdbc.SqlExecuteEvent;
import com.taobao.tddl.sqlobjecttree.DMLCommon;

/*
 * @author guangxia
 * @since 1.0, 2010-4-12 下午02:20:52
 */
public class Context extends HashMap<Object, Object> {

	private static final long serialVersionUID = 1L;
	
	private String sql;
    private Object[] args;
    private DMLCommon parseResult;
    private List<SqlExecuteEvent> events;
    private int affectedRows;
        
	void setSql(String sql) {
		this.sql = sql;
	}
	public String getSql() {
		return sql;
	}
	void setArgs(Object[] args) {
		this.args = args;
	}
	public Object[] getArgs() {
		return args;
	}
	void setParseResult(DMLCommon parseResult) {
		this.parseResult = parseResult;
	}
	public DMLCommon getParseResult() {
		return parseResult;
	}
	public List<SqlExecuteEvent> getEvents() {
		if(events == null) {
			 events = new ArrayList<SqlExecuteEvent>();
		}
		return events;
	}
	public void setEvents(List<SqlExecuteEvent> events) {
		this.events = events;
	}
	public boolean isEventsEmpty() {
		if(events == null || events.isEmpty()) {
			return true;
		} else {
			return false;
		}
	}
	
	public void reset() {
		sql = null;
		args = null;
		parseResult = null;
		events = null;
		affectedRows = 0;
	}
	public void setAffectedRows(int affectedRows) {
		this.affectedRows = affectedRows;
	}
	public int getAffectedRows() {
		return affectedRows;
	}
    
}
