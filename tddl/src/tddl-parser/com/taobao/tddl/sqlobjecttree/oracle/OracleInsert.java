package com.taobao.tddl.sqlobjecttree.oracle;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.sqlobjecttree.Insert;

public class OracleInsert extends Insert {


	@Override
	public StringBuilder regTableModifiable(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		super.appendInsert(sb);
		sb = HintUtils.appendHint(oraTabName, list, sb,hints);
		sb = super.appendInsertBody(oraTabName, list, sb);
		return sb;
	}
}

