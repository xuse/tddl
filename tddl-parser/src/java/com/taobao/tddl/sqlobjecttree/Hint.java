package com.taobao.tddl.sqlobjecttree;

import java.util.List;

import com.taobao.tddl.common.sqlobjecttree.SQLFragment;

public interface Hint extends SQLFragment{
	public List<String> getArguments();
}
