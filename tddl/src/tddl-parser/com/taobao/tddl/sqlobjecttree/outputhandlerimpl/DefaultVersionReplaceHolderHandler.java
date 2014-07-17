package com.taobao.tddl.sqlobjecttree.outputhandlerimpl;

import java.util.Map;

import com.taobao.tddl.sqlobjecttree.ReplacableWrapper;

/**
 * 默认情况下不加任何字段
 * 
 * @author shenxun
 *
 */
public class DefaultVersionReplaceHolderHandler extends
		PlaceHolderReplaceHandler {

	@Override
	public String getReplacedString(Map<String, String> targetTableName,
			ReplacableWrapper replacedObj) {
		return "";
	}

}
