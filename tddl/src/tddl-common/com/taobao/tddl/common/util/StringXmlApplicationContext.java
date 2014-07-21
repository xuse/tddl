package com.taobao.tddl.common.util;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

/**
 * 字符串形式的Spring ApplicationContext实现。
 * 支持动态订阅spring配置的处理
 * 
 * @author linxuan
 *
 */
public class StringXmlApplicationContext extends AbstractXmlApplicationContext {
	private Resource[] configResources;

	public StringXmlApplicationContext(String stringXml) {
		this(new String[] { stringXml }, null);
	}

	public StringXmlApplicationContext(String[] stringXmls) {
		this(stringXmls, null);
	}

	public StringXmlApplicationContext(String[] stringXmls, ApplicationContext parent) {
		super(parent);
		this.configResources = new Resource[stringXmls.length];
		for (int i = 0; i < stringXmls.length; i++) {
			this.configResources[i] = new ByteArrayResource(stringXmls[i].getBytes());
		}
		refresh();
	}

	protected Resource[] getConfigResources() {
		return this.configResources;
	}
	
	public ClassLoader getClassLoader() { 
       return this.getClass().getClassLoader();
    }
}
