package com.taobao.tddl.jdbc.group;

public class GroupIndex {
	public final int index;
	public final boolean failRetry;

	public GroupIndex(int index, boolean failRetry) {
		this.index = index;
		this.failRetry = failRetry;
	}
}
