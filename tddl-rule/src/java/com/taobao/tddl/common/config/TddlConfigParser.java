package com.taobao.tddl.common.config;


public interface TddlConfigParser<T> {
	T parseCongfig(String txt);
}
