package com.taobao.tddl.client.controller;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.interact.bean.Field;
import com.taobao.tddl.interact.bean.ReverseOutput;
import com.taobao.tddl.interact.bean.TargetDB;

public interface DatabaseExecutionContext
{
	/** 返回targetDB 
	 * 
	 * @deprecated 兼容性方法，以后逐渐淘汰掉
	 * @return
	 */
	TargetDB getTargetDB();
	
	/**
	 * 获取表名和库名pair的list
	 * 
	 * List{对应一个sql列表}<Map<String{对应逻辑表}, String{对应真实表}>>
	 * @return
	 */
	List<Map<String/*logicTable*/, String/*targetTable*/>> getTableNames();
	
	/**
	 * 获取当前库的index
	 * @return
	 */
	String getDbIndex();
	
	/**
	 * 设定反向输出sql
	 * @param outputSQL
	 */
	void setOutputSQL(List<ReverseOutput> outputSQL);
	
	/**
	 * 获取反向输出sql
	 * @return
	 */
	public List<ReverseOutput> getOutputSQL();
	
	public Map<String, Field> getRealTableFieldMap();
}
