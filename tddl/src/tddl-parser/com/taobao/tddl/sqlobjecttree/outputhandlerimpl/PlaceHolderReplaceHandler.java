package com.taobao.tddl.sqlobjecttree.outputhandlerimpl;

import java.util.Map;

import com.taobao.tddl.sqlobjecttree.ReplacableWrapper;

/**
 * 用于处理一些需要直接替换sql中文字的东西
 * 
 * @author shenxun
 *
 */
public abstract class PlaceHolderReplaceHandler implements ReplaceHandler{
	public abstract String getReplacedString(Map<String,String> targetTableName,ReplacableWrapper replacedObj);
}
