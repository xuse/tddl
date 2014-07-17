package com.taobao.tddl.common.util;

import org.springframework.context.ApplicationContext;


public class TDataSourceConfigHolder {
	private static ThreadLocal<ApplicationContext> applicationContextThreadLocal = new ThreadLocal<ApplicationContext>();

	public static ApplicationContext getApplicationContext() {
		return applicationContextThreadLocal.get();
	}

	public static void setApplicationContext(
			ApplicationContext applicationCOntext) {
		applicationContextThreadLocal.set(applicationCOntext);
	}


}
