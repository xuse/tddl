//Copyright(c) Taobao.com
package com.taobao.tddl.rule.le.config.impl;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.rule.le.config.ConfigDataHandler;
import com.taobao.tddl.rule.le.config.ConfigDataHandlerFactory;
import com.taobao.tddl.rule.le.config.ConfigDataListener;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-1-11����01:17:21
 */
@SuppressWarnings("rawtypes")
public class DefaultConfigDataHandlerFactory implements
		ConfigDataHandlerFactory {
	private static final Log log = LogFactory
			.getLog(DefaultConfigDataHandlerFactory.class);
	private static final String HANDLER_CLASS = "config.handler.constructor.name";
	private static final String DEFAULT_HANDLER_CLASS = "com.taobao.tddl.rule.le.config.diamond.DiamondConfigDataHandler";

	private static String propertyFile = "remote-config.properties";
	private static String handlerClassName;
	private static Class handlerClassObj;
	private static Constructor handlerConstructor;
	private static Properties prop;

	static {
		findSpecifiedConfigHandlerClass();
		createConstuctFromClassName();
	}

	private static void findSpecifiedConfigHandlerClass() {
		ClassLoader currentCL = getBaseClassLoader();
		InputStream resource;
		for (;;) {
			if (currentCL != null) {
				resource = currentCL.getResourceAsStream(propertyFile);
			} else {
				resource = ClassLoader.getSystemResourceAsStream(propertyFile);
				break;
			}

			if (null != resource) {
				break;
			} else {
				currentCL = currentCL.getParent();
			}
		}

		if (null != resource) {
			prop = new Properties();
			try {
				prop.load(resource);
				handlerClassName = prop.getProperty(HANDLER_CLASS);
				if (null == handlerClassName || "".equals(handlerClassName)) {
					handlerClassName = DEFAULT_HANDLER_CLASS;
				}
			} catch (IOException e) {
				log.error("properties can not load " + propertyFile);
			}
		} else {
			handlerClassName = DEFAULT_HANDLER_CLASS;
		}
	}

	@SuppressWarnings("unchecked")
	private static void createConstuctFromClassName() {
		ClassLoader currentCL = getBaseClassLoader();
		handlerClassObj = loadClass(handlerClassName, currentCL);
		if (null == handlerClassObj) {
			throw new IllegalArgumentException("can not get handler class:"
					+ handlerClassName);
		}

		try {
			handlerConstructor = handlerClassObj.getConstructor();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		}
	}

	private static Class loadClass(String className, ClassLoader currentCL) {
		log.info("Trying to load '" + className);
		try {
			Class clazz = currentCL.loadClass(handlerClassName);
			if (clazz != null) {
				return clazz;
			}
		} catch (ClassNotFoundException e) {
			log.error("can not load the class ");
		}

		return null;

	}

	private static boolean useTCCL = true;

	private static ClassLoader getBaseClassLoader() {
		ClassLoader thisClassLoader = DefaultConfigDataHandlerFactory.class
				.getClassLoader();
		if (useTCCL == false) {
			return thisClassLoader;
		}
		ClassLoader contextClassLoader = Thread.currentThread()
				.getContextClassLoader();
		ClassLoader baseClassLoader = getLowestClassLoader(contextClassLoader,
				thisClassLoader);
		return baseClassLoader;
	}

	private static ClassLoader getLowestClassLoader(ClassLoader c1,
			ClassLoader c2) {
		if (c1 == null)
			return c2;

		if (c2 == null)
			return c1;

		ClassLoader current;

		current = c1;
		while (current != null) {
			if (current == c2)
				return c1;
			current = current.getParent();
		}

		current = c2;
		while (current != null) {
			if (current == c1)
				return c2;
			current = current.getParent();
		}

		return null;
	}

	public static String objectId(Object o) {
		if (o == null) {
			return "null";
		} else {
			// �������System.identityHashCodeֻ���ڳ�ʼ��ʱ����һ�Σ�����
			// ����ܴ��ڵ�����Ӱ�첢����
			return o.getClass().getName() + "@" + System.identityHashCode(o);
		}
	}

	public ConfigDataHandler getConfigDataHandler(String dataId,String unitName) {
		return this.getConfigDataHandler(dataId, null,unitName);
	}

	public ConfigDataHandler getConfigDataHandler(String dataId,
			ConfigDataListener configDataListener,String unitName) {
		return this.getConfigDataHandlerC(dataId, configDataListener,
				new HashMap<String, String>(),unitName);
	}

	public ConfigDataHandler getConfigDataHandlerWithListenerList(
			String dataId, List<ConfigDataListener> configDataListenerList,String unitName) {
		return this.getConfigDataHandlerWithListenerListC(dataId,
				configDataListenerList, new HashMap<String, String>(),unitName);
	}

	public ConfigDataHandler getConfigDataHandlerC(String dataId,
			ConfigDataListener configDataListener, Map<String, String> config,String unitName) {
		return this.getConfigDataHandlerCE(dataId, configDataListener, null,
				config,unitName);
	}

	public ConfigDataHandler getConfigDataHandlerWithListenerListC(
			String dataId, List<ConfigDataListener> configDataListenerList,
			Map<String, String> config,String unitName) {
		return this.getConfigDataHandlerWithListenerListCE(dataId,
				configDataListenerList, null, config,unitName);
	}

	public ConfigDataHandler getConfigDataHandlerE(String dataId,
			ConfigDataListener configDataListener, Executor executor,String unitName) {
		return this.getConfigDataHandlerCE(dataId, configDataListener,
				executor, new HashMap<String, String>(),unitName);
	}

	public ConfigDataHandler getConfigDataHandlerWithListenerListE(
			String dataId, List<ConfigDataListener> configDataListenerList,
			Executor executor,String unitName) {
		return this
				.getConfigDataHandlerWithListenerListCE(dataId,
						configDataListenerList, executor,
						new HashMap<String, String>(),unitName);
	}

	public ConfigDataHandler getConfigDataHandlerCE(String dataId,
			ConfigDataListener configDataListener, Executor executor,
			Map<String, String> config,String unitName) {
		List<ConfigDataListener> configDataListenerList = new ArrayList<ConfigDataListener>();
		configDataListenerList.add(configDataListener);
		return this.getConfigDataHandlerWithListenerListCE(dataId,
				configDataListenerList, executor, config, unitName);
	}

	@SuppressWarnings("unchecked")
	public ConfigDataHandler getConfigDataHandlerWithListenerListCE(
			String dataId, List<ConfigDataListener> configDataListenerList,
			Executor executor, Map<String, String> config,String unitName) {
		try {
			ConfigDataHandler instance = (ConfigDataHandler) handlerConstructor
					.newInstance();
			Map<String, Object> configMap = new HashMap<String, Object>();
			if (config != null) {
				configMap.putAll(config);
			}
			
			if (prop != null) {
				configMap.putAll((Map) prop);
			}
			instance.init(dataId, configDataListenerList, configMap,unitName);
			return instance;
		} catch (IllegalArgumentException e) {
			log.error("illegal arguments!", e);
		} catch (InstantiationException e) {
			log.error("handler init error!", e);
		} catch (IllegalAccessException e) {
			log.error("securty limit,handler can not be init!", e);
		} catch (InvocationTargetException e) {
			log.error("constructor invode error!", e);
		}
		return null;
	}
}