package com.taobao.tddl.parser.hint;

public class HintElement {
	public final Object key;
	public final Object value;
	public HintElement(Object key,Object value) {
		this.key=key;
		this.value=value;
	}
	@Override
	public String toString() {
		return "{"+key+"="+value+"}";
	}
}
