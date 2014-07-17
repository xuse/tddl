package com.taobao.tddl.sqlobjecttree.outputhandlerimpl;

import java.util.Map;

import com.taobao.tddl.sqlobjecttree.ReplacableWrapper;

public class MySQLVersionHolderReplaceHandler extends PlaceHolderReplaceHandler{

	@Override
	public String getReplacedString(Map<String, String> targetTableName,
			ReplacableWrapper replacedObj) {
		return ",sync_version=ifnull(sync_version,0) + 1 ";
	}

}
