package com.taobao.tddl.sqlobjecttree.outputhandlerimpl;

import java.util.Map;

import com.taobao.tddl.sqlobjecttree.ReplacableWrapper;

public class IndexPlaceHolderHandler extends PlaceHolderReplaceHandler {

	@Override
	public String getReplacedString(Map<String, String> targetTableName,
			ReplacableWrapper replacedObj) {
		String str = replacedObj.getReplacedStr();
		String toBeReplacedTable = targetTableName.get(str);
		if(toBeReplacedTable != null){
			return toBeReplacedTable;
		}else{
			return replacedObj.getReplacedStr();
		}
	}

}
