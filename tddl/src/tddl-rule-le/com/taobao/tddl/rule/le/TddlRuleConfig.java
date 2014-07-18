//Copyright(c) Taobao.com
package com.taobao.tddl.rule.le;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.tddl.interact.bean.ComparativeMapChoicer;
import com.taobao.tddl.interact.monitor.TotalStatMonitor;
import com.taobao.tddl.interact.rule.VirtualTableRoot;
import com.taobao.tddl.rule.le.bean.RuleChangeListener;
import com.taobao.tddl.rule.le.config.ConfigDataHandler;
import com.taobao.tddl.rule.le.config.ConfigDataHandlerFactory;
import com.taobao.tddl.rule.le.config.ConfigDataListener;
import com.taobao.tddl.rule.le.config.impl.DefaultConfigDataHandlerFactory;
import com.taobao.tddl.rule.le.topology.AppTopology;
import com.taobao.tddl.rule.le.util.StringXmlApplicationContext;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-4-21下午01:15:22
 */
public class TddlRuleConfig {
	private static Log logger = LogFactory.getLog(TddlRuleConfig.class);
	private static final String NONE_RULE_VERSION = null;
	private static final String TDDL_RULE_LE_PREFIX = "com.taobao.tddl.rule.le.";
	private static final String TDDL_TOPOLOGY_PREFIX = "com.taobao.tddl.topology.";
	private static final String TDDL_RULE_LE_VERSIONS_FORMAT = "com.taobao.tddl.rule.le.{0}.versions";

	private String appName;
	private String unitName;
	private boolean needDbTabStat = false;
	private volatile ConfigDataHandlerFactory cdhf;

	// 本地规则
	private String appRuleFile;
	private String appRuleString;

	// 多套规则(动态推)
	private volatile ConfigDataHandler versionHandler;
	private volatile Map<String, ConfigDataHandler> ruleHandlers = new HashMap<String, ConfigDataHandler>();
	protected volatile Map<String, VirtualTableRoot> vtrs = new HashMap<String, VirtualTableRoot>();
	protected volatile Map<String, AppTopology> topologys = new HashMap<String, AppTopology>();
	protected volatile Map<String, String> ruleStrs = new HashMap<String, String>();
	private volatile List<RuleChangeListener> listeners = new ArrayList<RuleChangeListener>();

	/**
	 * key = 0(old),1(new),2,3,4... value= version
	 */
	protected volatile Map<Integer, String> versionIndex = new HashMap<Integer, String>();
	private volatile Map<String, AbstractXmlApplicationContext> oldCtxs = new HashMap<String, AbstractXmlApplicationContext>();

	// 单套规则(动态推+本地规则)
	protected VirtualTableRoot vtr = null;
	protected String ruleStr = null;
	private AbstractXmlApplicationContext oldCtx;

	protected volatile Map<String, Set<String>> shardColumnCache = new HashMap<String, Set<String>>();

	public static final TotalStatMonitor statMonitor = TotalStatMonitor
			.getInstance();

	private ClassLoader outerClassLoader = this.getClass().getClassLoader();

	protected VirtualTableRoot getVirtualTableRoot(){
		if(vtr != null)
			return vtr;
		
		if(vtrs.size() > 0) {
			//返回第一个规则
			for(VirtualTableRoot virtualTableRoot : vtrs.values())
				return virtualTableRoot;
		}
		throw new IllegalStateException("can not find VirtualTableRoot!");
	}

	public void init() {
		// 启动日志
		statMonitor.setAppName(appName);
		statMonitor.setNeedDbTabStat(needDbTabStat);
		statMonitor.start();

		if (appRuleFile != null) {
			String[] rulePaths = appRuleFile.split(";");
			if (rulePaths.length == 1 && !rulePaths[0].matches("^V[0-9]*#.+$")) {
				ApplicationContext ctx = new ClassPathXmlApplicationContext(
						appRuleFile) {
					@Override
					public ClassLoader getClassLoader() {
						if (outerClassLoader == null) {
							return super.getClassLoader();
						} else {
							return outerClassLoader;
						}
					}
				};
				vtr = (VirtualTableRoot) ctx.getBean("vtabroot");
			} else {
				for (int i = 0; i < rulePaths.length; i++) {
					if (rulePaths[i].matches("^V[0-9]*#.+$")) {
						continue;
					} else {
						throw new RuntimeException("rule file path \""
								+ rulePaths[i]
								+ " \" does not fit the pattern!");
					}
				}

				Map<Integer, String> tempIndexMap = new HashMap<Integer, String>();
				for (int i = 0; i < rulePaths.length; i++) {
					String rulePath = rulePaths[i];
					String[] temp = rulePath.split("#");

					ApplicationContext ctx = new ClassPathXmlApplicationContext(
							temp[1]) {
						@Override
						public ClassLoader getClassLoader() {
							if (outerClassLoader == null) {
								return super.getClassLoader();
							} else {
								return outerClassLoader;
							}
						}
					};
					tempIndexMap.put(i, temp[0]);
					vtrs.put(temp[0],
							(VirtualTableRoot) ctx.getBean("vtabroot"));
				}
				this.versionIndex = tempIndexMap;
			}
		} else if (appRuleString != null) {
			StringXmlApplicationContext ctx = new StringXmlApplicationContext(
					appRuleString, outerClassLoader);
			vtr = (VirtualTableRoot) ctx.getBean("vtabroot");
			ruleStr = appRuleString;
		} else if (appName != null) {
			// String versionsDataId = new MessageFormat(
			// TDDL_RULE_LE_VERSIONS_FORMAT)
			// .format(new Object[] { appName });
			String versionsDataId = TddlRuleConfig.getVersionsDataId(appName);
			cdhf = new DefaultConfigDataHandlerFactory();
			versionHandler = cdhf.getConfigDataHandler(versionsDataId,
					new VersionsConfigListener(),unitName);
			String versionData = versionHandler.getData(10 * 1000,
					ConfigDataHandler.FIRST_SERVER_STRATEGY);

			if (versionData == null) {
				// String dataId = TDDL_RULE_LE_PREFIX + appName;
				String dataId = TddlRuleConfig
						.getNonversionedRuledataId(versionData);
				if (!ruleDataSub(dataId, NONE_RULE_VERSION,
						new SingleRuleConfigListener())) {
					throw new RuntimeException(
							"subscribe the rule data or init rule error!check the error log!");
				}
			} else {
				String[] versions = versionData.split(",");
				int index = 0;
				Map<Integer, String> tempIndexMap = new HashMap<Integer, String>();
				for (String version : versions) {
					// String dataId = TDDL_RULE_LE_PREFIX + appName + "."
					// + version;
					String dataId = TddlRuleConfig.getVersionedRuleDataId(
							appName, version);
					if (!ruleDataSub(dataId, version,
							new SingleRuleConfigListener())) {
						throw new RuntimeException(
								"subscribe the rule data or init rule error!check the error log! the rule version is:"
										+ version);
					}
					topologyDataSub(appName, version);
					tempIndexMap.put(index, version);
					index++;
				}
				this.versionIndex = tempIndexMap;
				// 记下日志,方便分析
				TotalStatMonitor.recieveRuleLog(versionData);
			}
		}
	}
	
	/**
	 * remove listeners
	 */
	public void destory(){
		if(versionHandler != null){
			versionHandler.closeUnderManager();
		}
		for(ConfigDataHandler ruleListener : this.ruleHandlers.values()){
			ruleListener.closeUnderManager();
		}
	}

	public static String getVersionsDataId(String appName) {
		String versionsDataId = new MessageFormat(TDDL_RULE_LE_VERSIONS_FORMAT)
				.format(new Object[] { appName });
		return versionsDataId;
	}

	public static String getVersionedRuleDataId(String appName, String version) {
		return TDDL_RULE_LE_PREFIX + appName + "." + version;
	}
	
	public static String getVersionedTopologyDataId(String appName, String version) {
		return TDDL_TOPOLOGY_PREFIX + appName + "." + version;
	}

	public static String getNonversionedRuledataId(String appName) {
		return TDDL_RULE_LE_PREFIX + appName;
	}

	public String getOldRuleStr() {
		if (this.ruleStrs != null && this.ruleStrs.size() > 0) {
			String ruleStr = this.ruleStrs.get(versionIndex.get(0));
			return ruleStr;
		} else if (this.ruleStr != null) {
			return this.ruleStr;
		} else {
			throw new RuntimeException("规则对象为空!请检查diamond上是否存在动态规则!");
		}
	}

	private boolean ruleDataSub(String dataId, String version,
			ConfigDataListener listener) {
		ConfigDataHandler ruleHandler = cdhf.getConfigDataHandler(dataId,
				listener,unitName);

		String data = null;
		try {
			data = ruleHandler.getData(10 * 1000,
					ConfigDataHandler.FIRST_SERVER_STRATEGY);
		} catch (Exception e) {
			logger.error("get diamond data error!",e);
			ruleHandler.closeUnderManager();
			return false;
		}

		if (data == null) {
			logger.error("use diamond rule config,but recieve no config at all!");
			ruleHandler.closeUnderManager();
			return false;
		}

		if (ruleConfigInit(data, version)) {
			if (version != null) {
				this.ruleHandlers.put(version, ruleHandler);
			}
			return true;
		} else {
			ruleHandler.closeUnderManager();
			return false;
		}
	}
	
	
	private boolean topologyDataSub(String appName, String version) {
		String dataId = TddlRuleConfig.getVersionedTopologyDataId(appName, version);
		ConfigDataHandler ruleHandler = cdhf.getConfigDataHandler(dataId,unitName);
		String data = null;
		try {
			data = ruleHandler.getData(10 * 1000,
					ConfigDataHandler.FIRST_SERVER_STRATEGY);
		} catch (Exception e) {
			logger.error("get diamond data error!",e);
			ruleHandler.closeUnderManager();
			return false;
		}

		if (data == null) {
			logger.warn("use diamond rule config, but recieve no topoloy!");
			ruleHandler.closeUnderManager();
			return false;
		}

		if (topologyConfigInit(appName, version, data)) {
			return true;
		} else {
			ruleHandler.closeUnderManager();
			return false;
		}
	}

	private synchronized boolean topologyConfigInit(String appName, String version, String data) {
		try {
			AppTopology appTopology = AppTopology.loadInput(data);
			if(appTopology != null) {
				this.topologys.put(version, appTopology);
				return true;
			}
		} catch (Exception e) {
			logger.error("Load Topology Error. AppName >> " + appName + " ##  Version >> " + version + " ## Data >> " + data ,e);
		}
		return false;
	}
	
	private synchronized boolean ruleConfigInit(String data, String version) {
		StringXmlApplicationContext ctx = null;
		try {
			// this rule may be wrong rule,don't throw it but log it,
			// and will not change the vtr!
			ctx = new StringXmlApplicationContext(data, outerClassLoader);
		} catch (Exception e) {
			logger.error("init rule error,rule str is:" + data, e);
			return false;
		}

		VirtualTableRoot tempvtr = (VirtualTableRoot) ctx.getBean("vtabroot");
		if (version != null && tempvtr != null) {
			// 直接覆盖
			this.vtrs.put(version, tempvtr);
			this.ruleStrs.put(version, data);
			oldCtx = this.oldCtxs.get(version);
			// 销毁旧有容器
			if (oldCtx != null) {
				oldCtx.close();
			}
			this.oldCtxs.remove(version);
			this.oldCtxs.put(version, ctx);
		} else if (tempvtr != null) {
			this.vtr = tempvtr;
			this.ruleStr = data;
			if (oldCtx != null) {
				oldCtx.close();
			}
			oldCtx = (AbstractXmlApplicationContext) ctx;
		} else {
			// common not be here!
			logger.error("rule no vtabroot!!");
			return false;
		}
		return true;
	}

	protected ComparativeMapChoicer generateComparativeMapChoicer(
			String conditionStr) {
		SimpleComparativeMapChoicer mc = new SimpleComparativeMapChoicer();
		mc.addComparatives(conditionStr);
		return mc;
	}

	public void setAppRuleFile(String appRuleFile) {
		this.appRuleFile = appRuleFile;
	}

	public void setAppRuleString(String appRuleString) {
		this.appRuleString = appRuleString;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}
	
	public void setUnitName(String unitName) {
		this.unitName = unitName;
	}

	public void setNeedDbTabStat(boolean needDbTabStat) {
		this.needDbTabStat = needDbTabStat;
	}

	public void setOuterClassLoader(ClassLoader outerClassLoader) {
		this.outerClassLoader = outerClassLoader;
	}

	public void addRuleChangeListener(RuleChangeListener listener) {
		this.listeners.add(listener);
	}
	
	public AppTopology getAppTopology(){
		if (this.versionIndex.size() == 1) {
			return topologys.get(versionIndex.get(0));
		}
		throw new RuntimeException("This Method Not Support More Than 1 Version! AppName >> " + appName);
	}

	private class SingleRuleConfigListener implements ConfigDataListener {
		private ReentrantLock lock = new ReentrantLock();

		public void onDataRecieved(String dataId, String data) {
			try {
				lock.lock();
				if (data != null && !data.equals("")) {
					StringBuilder sb = new StringBuilder("recieve data!dataId:");
					sb.append(dataId);
					sb.append(" data:");
					sb.append(data);
					logger.info(sb.toString());

					String prefix = TDDL_RULE_LE_PREFIX + appName + ".";
					int i = dataId.indexOf(prefix);
					if (i < 0) {
						// non-versioned rule
						if (ruleConfigInit(data, null)) {
							for (RuleChangeListener listener : listeners) {
								try {
									// may be wrong,so try catch it ,not to
									// affect
									// other!
									listener.onRuleRecieve(data);
								} catch (Exception e) {
									logger.error("one listener error!", e);
								}
							}
						}
					} else {
						String version = dataId.substring(i + prefix.length());
						if (ruleConfigInit(data, version)) {
							for (RuleChangeListener listener : listeners) {
								try {
									// may be wrong,so try catch it ,not to
									// affect
									// other!
									listener.onRuleRecieve(getOldRuleStr());
								} catch (Exception e) {
									logger.error("one listener error!", e);
								}
							}
						}
					}
				}
			} finally {
				lock.unlock();
			}
		}
	}

	private class VersionsConfigListener implements ConfigDataListener {
		private ReentrantLock lock = new ReentrantLock();

		public void onDataRecieved(String dataId, String data) {
			try {
				lock.lock();
				if (data != null && !data.equals("")) {
					StringBuilder sb = new StringBuilder(
							"recieve versions data!dataId:");
					sb.append(dataId);
					sb.append(" data:");
					sb.append(data);
					logger.info(sb.toString());

					String[] versions = data.split(",");
					Map<String, String> checkMap = new HashMap<String, String>();
					// 添加新增的规则订阅
					int index = 0;
					Map<Integer, String> tempIndexMap = new HashMap<Integer, String>();
					for (String version : versions) {
						// FIXME:change the rule,may be wrong,this is a problem
						if (ruleHandlers.get(version) == null) {
							// String ruleDataId = TDDL_RULE_LE_PREFIX + appName
							// +
							// "."
							// + version;
							String ruleDataId = TddlRuleConfig
									.getVersionedRuleDataId(appName, version);
							if (!ruleDataSub(ruleDataId, version,
									new SingleRuleConfigListener())) {
								return;
							}
							topologyDataSub(appName, version);
						}
						checkMap.put(version, version);
						tempIndexMap.put(index, version);
						index++;
					}
					versionIndex = tempIndexMap;

					// 删除没有在version中存在的订阅
					List<String> needRemove = new ArrayList<String>();
					for (Map.Entry<String, ConfigDataHandler> handler : ruleHandlers
							.entrySet()) {
						if (checkMap.get(handler.getKey()) == null) {
							needRemove.add(handler.getKey());
						}
					}

					// 清理
					for (String version : needRemove) {
						ConfigDataHandler handler = ruleHandlers.get(version);
						handler.closeUnderManager();
						ruleHandlers.remove(version);
						vtrs.remove(version);
						ruleStrs.remove(version);
						oldCtxs.get(version).close();
						oldCtxs.remove(version);
					}
					// versionIndex = tempIndexMap;

					// 在versions data收到为null,或者为空,不调用,保护AppServer
					// 调用listener,但只返回位列第一个的VirtualTableRoot
					for (RuleChangeListener listener : listeners) {
						try {
							// may be wrong,so try catch it ,not to affect
							// other!
							listener.onRuleRecieve(getOldRuleStr());
						} catch (Exception e) {
							logger.error("one listener error!", e);
						}
					}

					shardColumnCache.clear();
				}

				// 记下日志,方便分析
				TotalStatMonitor.recieveRuleLog(data);
			} finally {
				lock.unlock();
			}
		}
	}
}
