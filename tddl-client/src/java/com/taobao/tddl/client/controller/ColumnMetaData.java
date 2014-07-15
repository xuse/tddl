package com.taobao.tddl.client.controller;

import com.taobao.tddl.interact.sqljep.Comparative;

public class ColumnMetaData {
	/**
	 * 指定的列名字段
	 */
	public final String key;
	/**
	 * 该列名字段的对应Comparative
	 */
	public final  Comparative value;
	public ColumnMetaData(String key,Comparative value) {
		this.key=key;
		this.value=value;
	}
}
