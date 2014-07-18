package com.taobao.tddl.common.sqlobjecttree;

import java.util.List;

public interface Value extends SQLFragment,Comparable{

	public Comparable<?> eval();
	public Comparable<?> getVal(List<Object> args);
}
