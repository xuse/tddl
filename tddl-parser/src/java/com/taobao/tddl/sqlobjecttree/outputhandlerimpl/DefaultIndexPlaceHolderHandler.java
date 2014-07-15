package com.taobao.tddl.sqlobjecttree.outputhandlerimpl;

import java.util.Map;

import com.taobao.tddl.sqlobjecttree.ReplacableWrapper;

public class DefaultIndexPlaceHolderHandler extends PlaceHolderReplaceHandler{

	@Override
	public String getReplacedString(Map<String, String> targetTableName,
			ReplacableWrapper replacedObj) {
		
		return replacedObj.getReplacedStr();
	}

}
