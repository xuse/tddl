package com.taobao.tddl.sqlobjecttree;

import com.taobao.tddl.common.sqlobjecttree.SQLFragment;

public interface Statement extends SQLFragment{
	boolean isDML();
}
