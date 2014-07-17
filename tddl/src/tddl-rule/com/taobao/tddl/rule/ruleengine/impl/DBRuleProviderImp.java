//package com.taobao.tddl.rule.ruleengine.impl;
//
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.Map.Entry;
//import java.util.concurrent.ConcurrentHashMap;
//
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//import org.dom4j.DocumentException;
//
//import com.taobao.tddl.common.DBType;
//import com.taobao.tddl.common.exception.checked.ParseSQLJEPException;
//import com.taobao.tddl.common.exception.checked.TDLCheckedExcption;
//import com.taobao.tddl.common.exception.runtime.CantFindTargetVirtualNameException;
//import com.taobao.tddl.common.exception.runtime.TDLRunTimeException;
//import com.taobao.tddl.common.route.FuncRegister;
//import com.taobao.tddl.common.sequence.Config;
//import com.taobao.tddl.common.sequence.Config.ConfigException;
//import com.taobao.tddl.common.sqljep.ParseException;
//import com.taobao.tddl.common.sqljep.RowJEP;
//import com.taobao.tddl.common.sqljep.function.Comparative;
//import com.taobao.tddl.common.sqljep.function.ComparativeBaseList;
//import com.taobao.tddl.rule.ruleengine.DBRuleProvider;
//import com.taobao.tddl.rule.ruleengine.TableRuleProvider;
//import com.taobao.tddl.rule.ruleengine.TableRuleProviderRegister;
//import com.taobao.tddl.rule.ruleengine.entities.inputvalue.DBRule;
//import com.taobao.tddl.rule.ruleengine.entities.inputvalue.LogicTabMatrix;
//import com.taobao.tddl.rule.ruleengine.entities.inputvalue.TabRule;
//import com.taobao.tddl.rule.ruleengine.entities.retvalue.PartitionElement;
//import com.taobao.tddl.rule.ruleengine.entities.retvalue.TargetDB;
//import com.taobao.tddl.rule.ruleengine.entities.retvalue.TargetDBMetaData;
//import com.taobao.tddl.rule.ruleengine.util.LogUtils;
//import com.taobao.tddl.rule.ruleengine.util.StringUtils;
//import com.taobao.tddl.rule.ruleengine.xml.TDLXmlParser;
//
//public class DBRuleProviderImp implements DBRuleProvider {
//
//	/**
//	 * 因为parserExpression那一下太慢，必须做个cache
//	 */
//	private final Map<String/*
//							 * parameters:expression,或者primaryKey:PKExpression,可转化为positionMap
//							 */, RowJEP> rowJepCache = new ConcurrentHashMap<String, RowJEP>();
//
//	private final PaginationColumnRowJepType paginationColumnRowJepType = new PaginationColumnRowJepType();
//	private final PrimaryKeyRowJepType primaryKeyRowJepType = new PrimaryKeyRowJepType();
//	private final NoSpecficColumnRowJepType noSpecficColumnRowJepType = new NoSpecficColumnRowJepType();
//	private static final Logger log = Logger.getLogger(DBRuleProviderImp.class);
//	private static final Logger defaultWarningLog = Logger
//			.getLogger(LogUtils.TDDL_DEFAULT_WARN);
//
//	private Config.Factory IDConfigFactory = null;
//	/**
//	 * 缓存了当前的配置信息，还不支持动态刷新载入。
//	 */
//	final Map<String, LogicTabMatrix> tabMap = new ConcurrentHashMap<String, LogicTabMatrix>();
//	
//	private volatile boolean inited = false;
//	/**
//	 * url地址
//	 */
//	private volatile String url;
//	/**
//	 * Xml分析加载器
//	 */
//
//	private TDLXmlParser parser;
//
//	private volatile String IDGeneratorUrl;
//
//	public String getUrl() {
//		return url;
//	}
//
//	public void setUrl(String url) {
//		this.url = url;
//	}
//
//	public synchronized void setParser(TDLXmlParser parser) {
//		this.parser = parser;
//	}
//
//	public String getIDGeneratorUrl() {
//		return IDGeneratorUrl;
//	}
//
//	public void setIDGeneratorUrl(String generatorUrl) {
//		IDGeneratorUrl = generatorUrl;
//	}
//
//	// public Set<String> getSplitDBColumns(String virtualTabName,boolean isPK)
//	// {
//	// initTabmap();
//	// Set<String> set = new HashSet<String>();
//	// LogicTabMatrix logTabs = tabMap.get(virtualTabName);
//	// if (logTabs == null) {
//	// throw new CantFindTargetVirtualNameException(virtualTabName);
//	// }
//	// Map<String, DBRule> rules = logTabs.getAllRules();
//	// for (DBRule dbRule : rules.values()) {
//	// if(isPK){
//	// //pk只有一个在bean中保证
//	// set.add(dbRule.getPrimaryKey());
//	// }else{
//	// set.addAll(dbRule.getPosiMap().keySet());
//	// }
//	// }
//	// return set;
//	// }
//	public TargetDBMetaData getDBAndTabs(String virtualTabName,
//			Map<String, Comparative> colMap) throws TDLCheckedExcption {
//		initTabmap();
//		List<TargetDB> retDBs = new ArrayList<TargetDB>();
//		LogicTabMatrix logTabs = tabMap.get(virtualTabName.toLowerCase());
//
//		if (logTabs == null) {
//			throw new CantFindTargetVirtualNameException(virtualTabName);
//		}
//		if (colMap == null) {
//			throw new TDLRunTimeException("Map<String,Comparative> 为空");
//		}
//		if (log.isDebugEnabled()) {
//			log
//					.debug("method in getDBAndTabs virtualTabName:"
//							+ virtualTabName);
//			int i = 0;
//			for (Comparable<?> c : colMap.keySet()) {
//				log.debug(" comparable: " + i + c.toString());
//				i++;
//			}
//			for (Comparable<?> c : colMap.values()) {
//				log.debug(" comparable: " + i + c.toString());
//				i++;
//			}
//		}
//		return buildDBAndTabReturnSQL(virtualTabName, colMap, retDBs, logTabs);
//	}
//
//	/**
//	 * 通过Comparablemap 返回TargetDBs列表。是两套input的公用方法
//	 * 
//	 * @param virtualTabName
//	 * @param colListMap
//	 * @param retDBs
//	 * @param logTabs
//	 * @param skip
//	 * @param max
//	 * @param orderbyeles
//	 * @return
//	 * @throws TDLCheckedExcption
//	 * @throws ParseSQLJEPException
//	 */
//	private TargetDBMetaData buildDBAndTabReturnSQL(String virtualTabName,
//			Map<String, Comparative> colMap, List<TargetDB> retDBs,
//			LogicTabMatrix logTabs) throws TDLCheckedExcption,
//			ParseSQLJEPException {
//		if (virtualTabName == null) {
//			throw new IllegalArgumentException("virtualTableName is null");
//		}
//		if (colMap == null) {
//			throw new IllegalArgumentException("ComparativeMap is null");
//		}
//		boolean findAtLeastOneTab = false;
//
//		Collection<DBRule> depositedRules = logTabs.getDepositedRules()
//				.values();
//		// 针对逻辑表中的每一个项
//		for (DBRule adb : depositedRules) {
//			RowJEP jep = null;
//			Comparable<?>[] repo = null;
//
//			RowJepType rowJepHandler = getRowJepType(adb, colMap);
//
//			boolean isMatch = false;
//			try {
//
//				repo = rowJepHandler.getPositionMapAndComparableArray(adb,
//						colMap);
//				jep = rowJepHandler.getTargetRowJEP(virtualTabName, adb);
//				if (jep == null) {
//					isMatch = false;
//				} else {
//					if (IDConfigFactory != null) {
//						Config conf = IDConfigFactory
//								.newInstance(virtualTabName);
//						// if(conf==null){
//						// throw new
//						// IllegalArgumentException("目标虚拟表:"+virtualTabName
//						// +"不包含在配置文件中，请检查" +
//						// "语义id配置文件中是否有指定id的项");
//						// }
//						isMatch = ((Boolean) jep.getValue(repo, conf))
//								.booleanValue();
//					} else {
//						isMatch = ((Boolean) jep.getValue(repo)).booleanValue();
//					}
//
//				}
//			} catch (ParseException e) {
//				throw new ParseSQLJEPException(e);
//			}
//			// 符合条件的db
//			if (isMatch) {
//				TargetDB db = buildOneTargetDBs(virtualTabName, colMap,
//						logTabs, adb);
//				retDBs.add(db);
//				findAtLeastOneTab = true;
//				if (repo.length == 1) {
//					// 如果是一个分库字段，等于的关系，则找到就返回。
//					if ((repo[0] instanceof Comparative)
//							&& !(repo[0] instanceof ComparativeBaseList)) {
//						if (((Comparative) (repo[0])).getComparison() == Comparative.Equivalent) {
//							break;
//						}
//						// else{
//						// 其他情况不与处理，包含各种其他的比较关系
//						// }
//					}
//					// else{
//					// 其他情况不予处理，包含了And Or 这两类以及其他的一些非Comparative的情况
//					// }
//				}
//
//				// }else{
//				// 当前规则不符合输入参数，跳过
//				// }
//			}
//		}
//		if (!findAtLeastOneTab) {
//			if (defaultWarningLog.isEnabledFor(Level.DEBUG)) {
//				StringBuilder sb = new StringBuilder();
//				sb
//						.append(
//								"Can't find at least one DBRule by using spec arguments. Use default dbrule")
//						.append(StringUtils.NL);
//				sb.append("virtual table name:").append(virtualTabName).append(
//						StringUtils.NL);
//				sb.append("arguments:[").append(StringUtils.NL);
//				int index = 0;
//				sb.append("Layer:").append(index).append(":").append(
//						StringUtils.NL);
//
//				Set<Entry<String, Comparative>> entrySet = colMap.entrySet();
//
//				for (Entry<String, Comparative> ent : entrySet) {
//					sb.append("key:").append(ent.getKey()).append(
//							StringUtils.NL);
//					sb.append("value:").append(ent.getValue().toString())
//							.append(StringUtils.NL);
//				}
//
//				sb.append("]");
//				log.debug(sb.toString());
//			}
//			buildDefaultDBRuleSQL(virtualTabName, colMap, retDBs, logTabs);
//			// }else{
//			// 找到至少一个，跳过
//			// }
//		}
//		TargetDBMetaData meta =null;
//		if(logTabs.isAllowReverseOutput()){	
//			meta= new TargetDBMetaData(virtualTabName, retDBs,
//					logTabs.isNeedRowCopy(),true);
//			
//		}else{
//			meta= new TargetDBMetaData(virtualTabName, retDBs,
//					logTabs.isNeedRowCopy(),false);
//		}
//
//		return meta;
//	}
//
//	/**
//	 * 用于将KeyValue数组拼装成Comparable数组和position位置条件的Map
//	 * 
//	 * @param rule
//	 * @param colMap
//	 * @return
//	 */
//	private static Comparable<?>[] getPositionMapAndComparableArrays(
//			Map<String, Integer> posiMap, Map<String, Comparative> colMap) {
//		Comparable<?>[] comp = new Comparable<?>[posiMap.size()];
//		for (Entry<String, Integer> ent : posiMap.entrySet()) {
//			Comparative t = null;
//			t = colMap.get(ent.getKey());
//			if (t != null) {
//				if (comp[ent.getValue()] != null) {
//					throw new IllegalArgumentException("不支持分库字段出现在不同sql嵌套层级中");
//				} else {
//					comp[ent.getValue()] = t;
//				}
//			}
//
//		}
//		return comp;
//	}
//
//	/**
//	 * 检查where条件中是否包含的所有分库字段是否都有值，如果都有值则返回paginationColumnRowJepType
//	 * 如果没有，那么检查是否包含主键id以及主键条件表达式，有则返回primaryColumnRowJepType
//	 * 
//	 * @important 如果不修改以前的xml，会发生严重问题！！！ 将以前在xml中的表参数从库参数中去掉，会发生错误
//	 * 
//	 * @param posiMap
//	 * @param colMap
//	 * @return
//	 */
//	private RowJepType getRowJepType(DBRule dbRule,
//			Map<String, Comparative> colMap) {
//		boolean isPaginationColumn = true;
//		boolean isPrimaryColumn = true;
//		// 优先判断是否有分库字段进行分库。
//		Set<Entry<String, Integer>> entryset = dbRule.getPosiMap().entrySet();
//		if (entryset.size() == 0) {
//			isPaginationColumn = false;
//		} else {
//			for (Entry<String, Integer> ent : entryset) {
//				Comparative snap = null;
//				snap = colMap.get(ent.getKey());
//				if (snap == null) {
//					isPaginationColumn = false;
//				}
//			}
//		}
//		if (isPaginationColumn) {
//			return this.paginationColumnRowJepType;
//		}
//		// 然后判断是否可以根据id进行分库
//		entryset = dbRule.getPrimaryPosiMap().entrySet();
//		if (entryset.size() == 0) {
//			isPrimaryColumn = false;
//		} else {
//
//			Comparative snap = null;
//			for (Entry<String, Integer> ent : entryset) {
//				snap = colMap.get(ent.getKey());
//				if (snap == null) {
//					isPrimaryColumn = false;
//				}
//
//			}
//
//		}
//		if (isPrimaryColumn) {
//			return this.primaryKeyRowJepType;
//		} else {
//			// 不能根据parameters或者primaryKey找到指定的分库字段
//			return this.noSpecficColumnRowJepType;
//		}
//	}
//
//	/**
//	 * 组建默认DBRule，当没有一个Expression符合要求时调用此方法
//	 * 
//	 * @param vtabname
//	 * @param colMap
//	 * @param retDBs
//	 * @param logTabs
//	 * @param skip
//	 * @param max
//	 * @param orderbyeles
//	 * @throws TDLCheckedExcption
//	 */
//	private void buildDefaultDBRuleSQL(String vtabname,
//			Map<String, Comparative> colMap, List<TargetDB> retDBs,
//			LogicTabMatrix logTabs) throws TDLCheckedExcption {
//		List<DBRule> li = logTabs.getDefaultRules();
//		for (DBRule tab : li) {
//			TargetDB db = buildOneTargetDBs(vtabname, colMap, logTabs, tab);
//			retDBs.add(db);
//		}
//	}
//
//	/**
//	 * 支持通过注入url和parser的方式o 获取logixTabMarticMap.
//	 */
//	private void initTabmap() {
//		if (!inited) {
//			// 有可能spring在初始化的过程中导致parser!=null但还不是一个正确的Parser对象。
//			// 当然可能性很小
//			synchronized (this) {
//				if (!inited) {
//					if (tabMap.size() == 0) {
//						if (parser == null) {
//							throw new IllegalArgumentException(
//									"parser没有注入，也没有提供tabMap");
//						}
//						if (url == null || url.trim().equals("")) {
//							throw new IllegalArgumentException(
//									"url没有通过配置注入，也没有提供tabMap");
//						}
//
//						// 使用concurrentHashMap保证put的安全。
//						parser.initLogicTabMarticMap(url, tabMap);
//
//						if (IDGeneratorUrl != null) {
//							try {
//								IDConfigFactory = new Config.Factory(
//										IDGeneratorUrl);
//							} catch (DocumentException e) {
//								throw new TDLRunTimeException(e);
//							} catch (ConfigException e) {
//								throw new TDLRunTimeException(e);
//							}
//						}
//						inited = true;
//					}
//
//				}
//			}
//		}
//	}
//
//	/**
//	 * 组成一个TargetDBs.根据指定的条件
//	 * 
//	 * @param vtabName
//	 * @param comp
//	 * @param position
//	 * @param logTabs
//	 * @param tab
//	 * @param skip
//	 * @param max
//	 * @param orderbyeles
//	 * @return
//	 * @throws TDLCheckedExcption
//	 */
//	private TargetDB buildOneTargetDBs(String vtabName,
//			Map<String, Comparative> colMap, LogicTabMatrix logTabs, DBRule tab)
//			throws TDLCheckedExcption {
//		TargetDB db = new TargetDB();
//		TableRuleProvider provider = null;
//		Set<String> idSet = null;
//		if (tab.getDBSubTabRule() != null) {
//			String tabExpression = tab.getDBSubTabRule().getExpFunction();
//			// 首先尝试根据分库规则取表
//			provider = TableRuleProviderRegister
//					.getTableRuleProviderByKey(tabExpression);
//			if (provider != null) {
//				if (logTabs.getTableFactor() != null&&!logTabs.getTableFactor().equals("")) {
//					idSet = provider.getTables(colMap, tab.getDBSubTabRule(),
//							logTabs.getTableFactor(), null);
//				} else {
//					idSet = provider.getTables(colMap, tab.getDBSubTabRule(),
//							logTabs.getTableName(), null);
//				}
//
//			} else {
//				idSet = Collections.emptySet();
//			}
//			Comparative comp = null;
//			if (tab.getPrimaryKey() != null) {
//				comp = colMap.get(tab.getPrimaryKey());
//			}
//			// 如果没取到，并且可以找到pk主键。
//			if (idSet.size() == 0 && comp != null && IDConfigFactory != null) {
//
//				// 如果分库规则不能获取表,并且可以根据Primary key 找到对应的comparative
//				provider = TableRuleProviderRegister
//						.getTableRuleProviderByKey("primarykey");
//				if (logTabs.getTableFactor() != null&&!logTabs.getTableFactor().equals("")) {
//					idSet = provider.getTables(colMap, tab.getDBSubTabRule(),
//							logTabs.getTableFactor(), IDConfigFactory
//									.newInstance(vtabName));
//				} else {
//					idSet = provider.getTables(colMap, tab.getDBSubTabRule(),
//							logTabs.getTableName(), IDConfigFactory
//									.newInstance(vtabName));
//				}
//				
//
//			}
//
//			if (idSet.size() == 0 && comp == null) {
//				// 如果不能通过分库规则获取表，并且不能根据primary key 找到对应的comparative
//				// 因为前面有idSet为emptySet的设定，因此这里要从新建一个新的Set
//				idSet = new HashSet<String>();
//				idSet.addAll(tab.getDBSubTabRule().getDefaultTable());
//
//				if (defaultWarningLog.isEnabledFor(Level.DEBUG)) {
//					StringBuilder sb = new StringBuilder();
//					sb
//							.append(
//									"can't find specfic table,use default tables,virtual table:")
//							.append(vtabName).append(StringUtils.NL);
//					sb.append("pk:").append(
//							tab.getDBSubTabRule().getPrimaryKey()).append(
//							StringUtils.NL);
//					sb.append("exp:").append(
//							tab.getDBSubTabRule().getExpFunction()).append(
//							StringUtils.NL);
//					sb.append("param:").append(
//							tab.getDBSubTabRule().getParameter()).append(
//							StringUtils.NL);
//					sb.append("PKparam:").append(
//							tab.getDBSubTabRule().getPrimaryKey()).append(
//							StringUtils.NL);
//					sb.append("value map infomation:").append(StringUtils.NL);
//					Set<Entry<String, Comparative>> entrySet = colMap
//							.entrySet();
//					for (Entry<String, Comparative> ent : entrySet) {
//						sb.append("key:").append(ent.getKey()).append(
//								StringUtils.NL);
//						sb.append("value:").append(ent.getValue().toString())
//								.append(StringUtils.NL);
//					}
//					log.debug(sb.toString());
//				}
//			} else {
//				// idset不为0，表示可以取到值。因此什么都不做
//			}
//			db.setTableNames(idSet);
//		} else {
//			// 都没有那就用虚拟表名
//			Set<String> arList = new HashSet<String>();
//
//			String obj =logTabs.getTableName();
//			arList.add(obj);
//			db.setTableNames(arList);
//		}
//		db.setReadPool(tab.getReadPool());
//		db.setWritePool(tab.getWritePool());
//		return db;
//	}
//
//	/**
//	 * 因为parseExpression速度太慢 所以对分析后的Expression结果做了一个cache. 他会先根据key
//	 * 1:position位置字串。然后根据key 2: expression条件表达式，找到分析后的Expression
//	 * 
//	 * @param parameters
//	 * @param virtualTabName
//	 * @param expression
//	 * @return
//	 * @throws TDLCheckedExcption
//	 * @throws ParseException
//	 */
//	private RowJEP getTargetRowJEPs(String parameters, String virtualTabName,
//			String expression) throws TDLCheckedExcption, ParseException {
//		StringBuilder sb = new StringBuilder();
//		sb.append(parameters);
//		sb.append(":");
//		sb.append(expression);
//		RowJEP temp = rowJepCache.get(sb.toString());
//		log.debug("try to get rowJep object In rowJepCache,parameters is:"
//				+ parameters + "expression is :" + expression);
//		// 最坏的情况是多次执行if下面的代码，但由于类变量只有rowJEpCache一个，并且它的数据完整性
//		// 由concurrentHashMap保证，因此线程安全
//		if (temp == null) {
//			log.warn("cant find rowJep by para:" + parameters
//					+ ",we have to build this rule's cache");
//			long time = 0;
//			boolean debugEnable = log.isDebugEnabled();
//			if (debugEnable) {
//				time = System.currentTimeMillis();
//			}
//			// 全新的rowJepCache
//			LogicTabMatrix tabMatrix = tabMap.get(virtualTabName.toLowerCase());
//			Collection<DBRule> rules = tabMatrix.getDepositedRules().values();
//			buildOrRebuildMap(rules, rowJepCache);
//
//			if (debugEnable) {
//				log.debug("successfully build rowJepCache key1 is parameter:"
//						+ parameters + "elapsed time is "
//						+ (time - System.currentTimeMillis()));
//			}
//			temp = rowJepCache.get(sb.toString());
//		}
//		return temp;
//	}
//
//	/**
//	 * 载入或重载一个vtableName所对应的RowJEP解析
//	 * 
//	 * @param rules
//	 * @param map
//	 * @param IDmap
//	 * @throws ParseException
//	 */
//	private void buildOrRebuildMap(Collection<DBRule> rules,
//			Map<String, RowJEP> map) throws ParseException {
//		log.warn("build and parse sqljep expression once;");
//		String primaryKeyRowJepExpression = null;
//		for (DBRule rule : rules) {
//			if (!rule.getExpression().equals("")) {
//				String exp = rule.getExpression();
//				RowJEP jep = new RowJEP(exp);
//				jep.parseExpression(rule.getPosiMap(), null,
//						FuncRegister.ruleFunTab);
//
//				StringBuilder sb = new StringBuilder();
//				sb.append(rule.getParameters());
//				sb.append(":");
//				sb.append(exp);
//				map.put(sb.toString(), jep);
//			}
//			primaryKeyRowJepExpression = rule.getPrimaryKeyExp();
//			if (!primaryKeyRowJepExpression.equals("")) {
//				RowJEP temp = map.get(primaryKeyRowJepExpression);
//				if (temp == null) {
//					RowJEP jep = new RowJEP(primaryKeyRowJepExpression);
//					jep.parseExpression(rule.getPrimaryPosiMap(), null,
//							FuncRegister.ruleFunTab);
//					StringBuilder sb = new StringBuilder();
//					sb.append(rule.getPrimaryKey());
//					sb.append(":");
//					sb.append(primaryKeyRowJepExpression);
//					map.put(sb.toString(), jep);
//				}
//			}
//		}
//		log.warn("build sql successfully");
//
//	}
//
//	/**
//	 * 因为考虑到原则上只有init或者修改配置的时候才会调用，如果 有动态修改需要，则请小心谨慎处理 测试用方法
//	 * 
//	 * @param vTab
//	 * @param DBRule
//	 */
//	public synchronized void addDBRule(String tab, LogicTabMatrix DBRule) {
//		this.tabMap.put(tab.toLowerCase(), DBRule);
//
//	}
//
//	private interface RowJepType {
//		RowJEP getTargetRowJEP(String virtualTabName, DBRule dbRule)
//				throws TDLCheckedExcption, ParseException;
//
//		Comparable<?>[] getPositionMapAndComparableArray(DBRule dbRule,
//				Map<String, Comparative> colMap);
//
//	}
//
//	private class PaginationColumnRowJepType implements RowJepType {
//
//		public Comparable<?>[] getPositionMapAndComparableArray(DBRule dbRule,
//				Map<String, Comparative> colMap) {
//			return getPositionMapAndComparableArrays(dbRule.getPosiMap(),
//					colMap);
//		}
//
//		public RowJEP getTargetRowJEP(String virtualTabName, DBRule dbRule)
//				throws TDLCheckedExcption, ParseException {
//
//			return getTargetRowJEPs(dbRule.getParameters(), virtualTabName,
//					dbRule.getExpression());
//		}
//
//	}
//
//	private class PrimaryKeyRowJepType implements RowJepType {
//
//		public Comparable<?>[] getPositionMapAndComparableArray(DBRule dbRule,
//				Map<String, Comparative> colMap) {
//			return getPositionMapAndComparableArrays(
//					dbRule.getPrimaryPosiMap(), colMap);
//		}
//
//		public RowJEP getTargetRowJEP(String virtualTabName, DBRule dbRule)
//				throws TDLCheckedExcption, ParseException {
//			return getTargetRowJEPs(dbRule.getPrimaryKey(), virtualTabName,
//					dbRule.getPrimaryKeyExp());
//		}
//	}
//	
//	private static class NoSpecficColumnRowJepType implements RowJepType {
//
//		public Comparable<?>[] getPositionMapAndComparableArray(DBRule dbRule,
//				Map<String, Comparative> colMap) {
//			return new Comparable<?>[0];
//		}
//
//		public RowJEP getTargetRowJEP(String virtualTabName, DBRule dbRule)
//				throws TDLCheckedExcption, ParseException {
//			return null;
//		}
//	}
//
//	public DBType getDBType(){
//		initTabmap();
//		for(Entry<String,LogicTabMatrix> en:tabMap.entrySet()){
//			return en.getValue().getDBType();
//		}
//		return DBType.MYSQL;
//	}
//	/*
//	 * (non-Javadoc)
//	 * 
//	 * @see
//	 * com.taobao.tddl.rule.ruleengine.DBRuleProvider#getPatinationColumns()
//	 */
//	
//	@SuppressWarnings("deprecation")
//	public PartitionElement getPartitionColumns(String table) {
//		initTabmap();
//		PartitionElement ele = new PartitionElement();
//		if (table == null) {
//			throw new IllegalArgumentException("表名为空");
//		}
//		LogicTabMatrix matrix = tabMap.get(table.toLowerCase());
//		if (matrix == null) {
//			throw new IllegalArgumentException(table.toLowerCase()
//					+ ",未能在规则文件中找到对应的虚拟表名");
//		}
//		Map<String, DBRule> rules = matrix.getAllRules();
//
//		for (DBRule rule : rules.values()) {
//			Map<String, Integer> temp = rule.getPosiMap();
//			if (!temp.isEmpty()) {
//				ele.addAllDBFirstElement(temp.keySet());
//			}
//			temp = rule.getPrimaryPosiMap();
//			if (!temp.isEmpty()) {
//				ele.addAllPKFirstElement(temp.keySet());
//			}
//			TabRule tbRule = rule.getDBSubTabRule();
//			if (tbRule != null) {
//				String[] tempParamArr = tbRule.getAllParameter();
//				for (String one : tempParamArr) {
//					ele.addTabFirstElement(one);
//				}
//				String tempParam = tbRule.getPrimaryKey();
//				if (!tempParam.equals("")) {
//					ele.addPKFirstElement(tempParam);
//				}
//			}
//			// 只取第一个即可
//			return ele;
//		}
//		// if (rules != null && rules.size() > 0) {
//		//		
//		// }
//		return ele;
//	}
//
//	public TargetDBMetaData getDBAndTabs(String virtualTableName,
//			String databaseGroupID, Set<String> tables) throws TDLCheckedExcption {
//		initTabmap();
//		
//		if(virtualTableName == null){
//			throw new IllegalArgumentException("virtualtableName is null");
//		}else{
//			virtualTableName=virtualTableName.toLowerCase();
//		}
//		
//		if(databaseGroupID ==null){
//			throw new IllegalArgumentException("ruleID is null");
//		}else{
//			databaseGroupID=databaseGroupID.toLowerCase();
//		}
//
//		
//		LogicTabMatrix logicTable = tabMap.get(virtualTableName);
//		
//		if(logicTable == null){
//			throw new IllegalArgumentException(virtualTableName
//					+ ",未能在规则文件中找到对应的虚拟表名");
//		}
//		DBRule dbRule = logicTable.getAllRules().get(databaseGroupID);
//		if(dbRule == null){
//			throw new IllegalArgumentException(databaseGroupID+"，未能找到对应规则");
//		}
//		
//		List<TargetDB> targetDBs = new ArrayList<TargetDB>();
//		TargetDB db =new TargetDB();
//		db.setReadPool(dbRule.getReadPool());
//		db.setWritePool(dbRule.getWritePool());
//		//不做defaultTable的检查了，完全交给业务保证
//		db.setTableNames(tables);
//		targetDBs.add(db);
//		//不允许反向输出，也不允许行复制。
//		TargetDBMetaData meta = new TargetDBMetaData(virtualTableName,targetDBs,logicTable.isNeedRowCopy(),logicTable.isAllowReverseOutput());
//		return meta;
//	}
//
//	public TargetDBMetaData getDBAndTabs(String logicTableName,
//			Map<String, Comparative> colMap, int databaseRuleIndex,
//			int tableRuleIndex) throws TDLCheckedExcption {
//		throw new IllegalArgumentException("should not be here");
//	}
//
//}
