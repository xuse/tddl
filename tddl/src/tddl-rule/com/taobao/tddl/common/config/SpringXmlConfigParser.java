package com.taobao.tddl.common.config;

import com.taobao.tddl.common.util.StringXmlApplicationContext;


public class SpringXmlConfigParser<T> implements TddlConfigParser<T> {

	/*private String beanId;
	public SpringXmlConfigParser(String beanId){
		this.beanId = beanId;
	}*/

	@SuppressWarnings("unchecked")
	public T parseCongfig(String txt) {
		//用String Resource类从txt初始化context
		StringXmlApplicationContext ctx = new StringXmlApplicationContext(txt);
		//Type  type = this.getClass().getGenericSuperclass();
		//System.out.println(type.getClass().getName());
		/*T a = null;
		class Temp extends SpringXmlConfigParser<T>{
			
		}
		Temp t = new Temp();
		t.get*/
		//ctx.getBeanNamesForType(a.getClass());		
		//TODO 解决泛型擦除问题，用T的短类名替代"root"
		if (ctx.containsBean("root")) {
			return (T) ctx.getBean("root");
		} else if (ctx.containsBean("vtabroot")) {
			return (T) ctx.getBean("vtabroot");
		} else {
			throw new IllegalStateException("No 'root' or 'vtabroot' bean in spring xml");
		}
	}
}
