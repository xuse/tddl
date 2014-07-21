package com.taobao.tddl.common.config.beans;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.util.SimpleNamedMessageFormat;
import com.taobao.tddl.rule.bean.SimpleDateTableMapProvider;
import com.taobao.tddl.rule.bean.SimpleLogicTable;
import com.taobao.tddl.rule.bean.SimpleTableDatabaseMapProvider;
import com.taobao.tddl.rule.bean.SimpleTableMapProvider;
import com.taobao.tddl.rule.bean.SimpleTableTwoColumnsMapProvider;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.TabRule;

/**
 * 一个逻辑表怎样分库分表
 * 
 * @author linxuan
 * 
 */
public class TableRule extends SimpleLogicTable implements Cloneable {
	Log logger = LogFactory.getLog(TabRule.class);

	public static enum TABLE_GENERATION_TYPE {
		SIMPLE, DATE_SIMPLE, DATE_SIMPLE_AUTO
	}

	private String[] dbIndexes;
	private String dbIndexPrefix;
	private String dbIndexPattern;

	private int dbIndexCount;
	private Object[] dbRules; // string expression or mappingrule
	private Object[] tbRules; // string expression or mappingrule

	//private String[] uniqueKeys; //搬到行复制配置中
	//private boolean needRowCopy; //根据逻辑表在不在行复制配置中来设置

	private boolean allowReverseOutput;
	private boolean disableFullTableScan = true; // 是否关闭全表扫描

	/**
	 * 用来替换dbRules、tbRules中的占位符
	 * 优先用dbRuleParames，tbRuleParames替换，其为空时再用ruleParames替换
	 */
	private String[] ruleParames;
	private String[] dbRuleParames;
	private String[] tbRuleParames;
	
	/**
	 * 描述表后缀配置。格式： throughAllDB:[_0000-_0063]
	 * #如果3特殊的：throughAllDB:[_0000-_0063],3:[_00-_63] resetForEachDB:[_0-_4]
	 * twoColumnForEachDB: [_00-_99],[_00-_11] dbIndexForEachDB:[_00-_09]
	 */
	private SuffixManager suffixManager = new SuffixManager();

	TABLE_GENERATION_TYPE tableGenerationType = TABLE_GENERATION_TYPE.SIMPLE;
	private String timeStyle;
	private String calendarType;
	private String fromDateString;
	private String toDateString;
	private String groovyScript;
	//add by junyu:用户自定义包
	private List<String> extraPackages;

	/**
	 * 是否只根据日期进行分表。
	 * TODO:重构中应该只有一个这个字段出现。全部放入一套规则生成中即可。
	 */
	private boolean isOnlyDateSharding = false;

	/**
	 * 几年以后到今天？用于自动生成当前时间范围前后的表名，
	 * 这样就不需要写死from的时间了。
	 */
	private int yearsToNow = 2;

	/**
	 * 今天以后多少年？用于自动生成当前时间范围前后的表名，
	 * 这样就不需要写死to的时间了。
	 */
	private int yearsBehindNow = 2;

	public void init() {
		init(true);
	}

	@Override
	public void init(boolean invokeBySpring) {
		try {
			
			initDbIndexes();
			replaceWithParam(this.dbRules,
					dbRuleParames != null ? dbRuleParames : ruleParames);
			replaceWithParam(this.tbRules,
					tbRuleParames != null ? tbRuleParames : ruleParames);
			String tbSuffix = suffixManager.getTbSuffix();
			if (tbSuffix != null) {
				tbSuffix = replaceWithParam(tbSuffix, ruleParames);
				suffixManager.setTbSuffix(tbSuffix);
				suffixManager.parseTbSuffix(dbIndexes);
			} else {
				suffixManager.init(dbIndexes);
			}
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
		toLogicTable(this,invokeBySpring);
		super.init(invokeBySpring);
	}

	protected final void initDbIndexes() throws ParseException {
		if (dbIndexes != null) {
			return;
		}
		if (dbIndexPrefix == null && dbIndexPattern == null || dbIndexCount <= 0) {
			throw new IllegalArgumentException("dbIndexes没有配置");
		}
		dbIndexes = new String[dbIndexCount];
		if (dbIndexPrefix != null) {
			// 按dbIndexPrefix和dbIndexCount生成dbIndexes
			int suffixLen = Integer.valueOf(dbIndexCount).toString().length();
			for (int i = 0; i < dbIndexCount; i++) {
				String suffix = String.valueOf(i);
				while (suffix.length() < suffixLen) {
					suffix = "0" + suffix;
				}
				dbIndexes[i] = dbIndexPrefix + suffix;
			}
		} else if (dbIndexPattern != null) {
			MessageFormat mf = new MessageFormat(dbIndexPattern);
			for (int i = 0; i < dbIndexCount; i++) {
				dbIndexes[i] = mf.format(new Object[]{i});
			}
		}
	}

	protected static void replaceWithParam(Object[] rules, String[] params) {
		if (params == null || rules == null) {
			return;
		}
		for (int i = 0; i < rules.length; i++) {
			if (rules[i] instanceof String) {
				// rules[i] = new MessageFormat((String)
				// rules[i]).format(params);
				rules[i] = replaceWithParam((String) rules[i], params);
			} else if (rules[i] instanceof MappingRuleBean) {
				MappingRuleBean tmr = (MappingRuleBean) rules[i];
				// String finalParameter = new MessageFormat((String)
				// tmr.getParameter()).format(params);
				// String finalExpression = new MessageFormat((String)
				// tmr.getExpression()).format(params);
				String finalParameter = replaceWithParam(tmr.getParameter(),
						params);
				String finalExpression = replaceWithParam(tmr.getExpression(),
						params);
				tmr.setParameter(finalParameter);
				tmr.setExpression(finalExpression);
			}
		}
	}

	private static String replaceWithParam(String template, String[] params) {
		if (params == null || template == null) {
			return template;
		}
		if (params.length != 0 && params[0].indexOf(":") != -1) {
			// 只要params的第一个参数中含有冒号，就认为是NamedParam
			return replaceWithNamedParam(template, params);
		}
		return new MessageFormat(template).format(params);
	}

	private static String replaceWithNamedParam(String template, String[] params) {
		Map<String, String> args = new HashMap<String, String>();
		for (String param : params) {
			int index = param.indexOf(":");
			if (index == -1) {
				throw new IllegalArgumentException(
						"使用名字化的占位符替换失败！请检查配置。 params:" + Arrays.asList(params));
			}
			args.put(param.substring(0, index).trim(), param.substring(
					index + 1).trim());
		}
		return new SimpleNamedMessageFormat(template).format(args);
	}

	private String toCommaString(String[] strArray) {
		if (strArray == null)
			return null;
		StringBuilder sb = new StringBuilder();
		for (String str : strArray) {
			sb.append(",").append(str);
		}
		if (strArray.length > 0) {
			sb.deleteCharAt(0);
		}
		return sb.toString();
	}

	private void toLogicTable(TableRule tableRule,boolean invokeBySpring) {
		setAllowReverseOutput(tableRule.isAllowReverseOutput());
		setDatabases(tableRule.getDbIndexes());
		if (tableRule.getDbRuleArray() != null) {
			List<Object> dbRules = new ArrayList<Object>(tableRule
					.getDbRuleArray().length);
			for (Object obj : tableRule.getDbRuleArray()) {
				if (obj instanceof MappingRuleBean) {
					throw new IllegalArgumentException(
							"TDDL 在新版本中不再支持mapping rule");
//					 addMappingRule((MappingRuleBean) obj, dbRules,invokeBySpring);
				} else {
					dbRules.add((String) obj);
				}
			}
			setDatabaseRuleStringList(dbRules);
		}
		if (tableRule.getTbRuleArray() != null) {
			List<Object> tbRules = new ArrayList<Object>(tableRule
					.getTbRuleArray().length);
			for (Object obj : tableRule.getTbRuleArray()) {
				if (obj instanceof MappingRuleBean) {
//					throw new IllegalArgumentException(
//							"TDDL 在新版本中不再支持mapping rule");
					 addMappingRule((MappingRuleBean) obj, tbRules,invokeBySpring);
				} else {
					tbRules.add((String) obj);
				}
			}
			switch (tableRule.getTableGenerationType()) {

			case SIMPLE:
				// TODO:重构此处，将TableMapProvider和TableRule合并，最终产生一个Map就可以了。
				buildSimpleTableRule(tableRule, this, tbRules);
				break;
			case DATE_SIMPLE_AUTO:
				if (tableRule.getFromDateString() != null
						|| tableRule.getToDateString() != null) {
					throw new IllegalArgumentException(
							"自动日期生成时，不需要使用fromDateString和toDataString属性");
				}
				
				setTableRuleStringList(tbRules);
				setExtraPackagesStr(extraPackages);
				// 自动填写日期
				SimpleDateTableMapProvider provider1 = new SimpleDateTableMapProvider();
				fillDateMapProvider(tableRule, provider1);
				String timeStyle = provider1.getInputTimeStyle();
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
						timeStyle);
				// 获取当前时间
				Calendar calfrom = Calendar.getInstance();
				Calendar calto = (Calendar) calfrom.clone();
				// 往前推两年
				calfrom.add(Calendar.YEAR, -tableRule.getYearsToNow());
				Date from = calfrom.getTime();
				calto.add(Calendar.YEAR, tableRule.getYearsBehindNow());
				Date to = calto.getTime();
				String fromString = simpleDateFormat.format(from);
				provider1.setFromDateString(fromString);
				String toString = simpleDateFormat.format(to);
				provider1.setToDateString(toString);

				logger.warn("auto generate date: from =" + fromString
						+ " to = " + toString);

				logger.warn("provider is :" + provider1);
				setGlobalCustomTableMapProvider(provider1);
				break;
			case DATE_SIMPLE:
				setTableRuleStringList(tbRules);
				setExtraPackagesStr(extraPackages);
				SimpleDateTableMapProvider provider = new SimpleDateTableMapProvider();
				fillDateMapProvider(tableRule, provider);
				provider.setFromDateString(tableRule.getFromDateString());
				provider.setToDateString(tableRule.getToDateString());
				setGlobalCustomTableMapProvider(provider);
				logger.warn("provider is :" + provider);
				break;
			default:
				break;
			}

		}
		/*if (tableRule.getUniqueKeyArray() != null) {
			setUniqueKeys(Arrays.asList(tableRule.getUniqueKeyArray()));
		}*/
		//setNeedRowCopy(tableRule.isNeedRowCopy());//无用调用
		if (tableRule.isDisableFullTableScan()) {
			setDefaultListResultStragety(DEFAULT_LIST_RESULT_STRAGETY.NONE);
		} else {
			setDefaultListResultStragety(DEFAULT_LIST_RESULT_STRAGETY.FULL_TABLE_SCAN);
		}
	}

	private void fillDateMapProvider(TableRule tableRule,
			SimpleDateTableMapProvider provider) {
		SuffixManager suffixManager = tableRule.getSuffixManager();
		Suffix suf = suffixManager.getSuffix(0);
		provider.setFrom(suf.getTbSuffixFrom());
		provider.setTo(suf.getTbSuffixTo());
		provider.setWidth(suf.getTbSuffixWidth());
		provider.setPadding(suf.getTbSuffixPadding());
		provider.setTablesNumberForEachDatabases(suf.getTbNumForEachDb());
		provider.setCalendarTypeString(tableRule.getCalendarType());
		provider.setTimeStyle(tableRule.getTimeStyle());
		provider.setOnlyDateSharding(tableRule.isOnlyDateSharding());
		provider.setGroovyScript(tableRule.getGroovyScript());
	}

	private void buildSimpleTableRule(TableRule tableRule, SimpleLogicTable st,
			List<Object> tbRules) {
		// 如果是2列的情况就用2列的类，否则按以前的逻辑走
		st.setSimpleTableMapProvider(getTableMapProvider(tableRule));
		SuffixManager suffixManager = tableRule.getSuffixManager();
		Suffix suf = suffixManager.getSuffix(0);
		st.setTableRuleStringList(tbRules);
		st.setExtraPackagesStr(extraPackages);
		// 分表规则存在，才设置表后缀属性，设置了任何一个属性，就表示用simpleTableMapProvider
		st.setFrom(suf.getTbSuffixFrom());
		st.setTo(suf.getTbSuffixTo());
		st.setWidth(suf.getTbSuffixWidth());
		st.setPadding(suf.getTbSuffixPadding());
		st.setTablesNumberForEachDatabases(suf.getTbNumForEachDb());
	}
	
    // modify by junyu DatabaseBasedMapping移到了cache工程下
    // protected ThreadLocal<List<DatabaseBasedMapping>> mappingHandlers;

	@Deprecated
	private void addMappingRule(MappingRuleBean bean, List<Object> dbRules,boolean invokeBySpring) {
//		if(invokeBySpring)
//		{
//			logger.warn("invoke by spring skip to add MappingRule, we'll do it in TDataSource init method");
//			return;
//		}
//		ApplicationContext springApplicationContext = TDataSourceConfigHolder.getApplicationContext();
//		if(springApplicationContext == null)
//		{
//			throw new IllegalArgumentException("spring applicatoin is null");
//		}
//		if (!springApplicationContext.containsBean(bean.getMappingRuleBeanId())) {
//			// 如果父SpringContext中不包含MappingRuleBeanId的定义,则忽略这条映射规则
//			logger.warn("Discard a mapping rule because there is no definition for mappingRuleBeanId: "
//					+ bean.getMappingRuleBeanId());
//			return;
//		}
//		MappingRule mr = new MappingRule();
//		mr.setParameter(bean.getParameter());
//		mr.setExpression(bean.getExpression());
//		DatabaseBasedMapping mappingHandler = (TairBasedMapping) springApplicationContext
//				.getBean(bean.getMappingRuleBeanId());
//		if (mappingHandler.getRouteDatasource() == null) {
//			if (this.mappingHandlers == null) {
//				this.mappingHandlers = new ThreadLocal<List<DatabaseBasedMapping>>();
//			}
//			List<DatabaseBasedMapping> handlers = this.mappingHandlers.get();
//			if (handlers == null) {
//				handlers = new ArrayList<DatabaseBasedMapping>();
//				this.mappingHandlers.set(handlers);
//			}
//			handlers.add(mappingHandler);
//			// 只是简单的设为this的话，在事务中读路由表会走到主库，非事务的读才会走读库。所以用ThreadLocal做特殊处理
//			mappingHandler.setRouteDatasource((DataSource) this);
//			throw new IllegalArgumentException("不再支持这种情况，请直接使用mappingHandler.setRouteDatasource来指定需要使用的datasource");
//		}
//		mr.setMappingHandler(mappingHandler);
//		dbRules.add(mr);
//		return;
	}

	/**
	 * 
	 * @return
	 */
	public SimpleTableMapProvider getTableMapProvider(TableRule tableRule) {
		SimpleTableMapProvider simpleTableMapProvider = null;
		SuffixManager suffixManager = tableRule.getSuffixManager();
		Suffix suf = suffixManager.getSuffix(0);
		if (suf.getTbType().equals("twoColumnForEachDB")) {
			simpleTableMapProvider = new SimpleTableTwoColumnsMapProvider();
			SimpleTableTwoColumnsMapProvider twoColumns = (SimpleTableTwoColumnsMapProvider) simpleTableMapProvider;
			Suffix suf2 = suffixManager.getSuffix(1);
			twoColumns.setFrom2(suf2.getTbSuffixFrom());
			twoColumns.setTo2(suf2.getTbSuffixTo());
			twoColumns.setWidth2(suf2.getTbSuffixWidth());
			twoColumns.setPadding2(suf2.getTbSuffixPadding());
		} else if ("dbIndexForEachDB".equals(suf.getTbType())) {
			simpleTableMapProvider = new SimpleTableDatabaseMapProvider();
		} else {
			simpleTableMapProvider = new SimpleTableMapProvider();
		}
		return simpleTableMapProvider;
	}

	public void parseTbSuffix() throws ParseException {
		suffixManager.parseTbSuffix(dbIndexes);
	}

	/**
	 * 生成月份表名的东东结束
	 */
	public static class ParseException extends Exception {
		private static final long serialVersionUID = 1L;

		public ParseException() {
			super();
		}

		public ParseException(String msg) {
			super(msg);
		}
	}

	public void setDbIndexes(String dbIndexes) {
		this.dbIndexes = dbIndexes.split(",");
		for (int i = 0; i < this.dbIndexes.length; i++) {
			this.dbIndexes[i] = this.dbIndexes[i].trim();
		}
	}

	public String getDbIndexes() {
		return toCommaString(this.dbIndexes);
	}

	public void setDbIndexArray(String[] array) {
		this.dbIndexes = array;
	}

	public String[] getDbIndexArray() {
		return dbIndexes;
	}

	/*public String[] getUniqueKeyArray() {
		return uniqueKeys;
	}

	public void setUniqueKeys(String uniquekeys) {
		this.uniqueKeys = uniquekeys.split(",");
	}*/

	public Object[] getDbRuleArray() {
		return dbRules;
	}

	public void setDbRuleArray(Object[] dbRules) {
		this.dbRules = dbRules;
	}

	public void setDbRules(String dbRules) {
		if (this.dbRules == null) {
			// 优先级比dbRuleArray低
			this.dbRules = dbRules.split("\\|");
		}
	}

	public Object[] getTbRuleArray() {
		return tbRules;
	}

	public void setTbRuleArray(Object[] tbRules) {
		this.tbRules = tbRules;
	}

	public void setTbRules(String tbRules) {
		if (this.tbRules == null) {
			// 优先级比tbRuleArray低
			this.tbRules = tbRules.split("\\|");
		}
	}

	public void setRuleParames(String ruleParames) {
		if (ruleParames.indexOf('|') != -1) {
			// 优先用|线分隔,因为有些规则表达式中会有逗号
			this.ruleParames = ruleParames.split("\\|");
		} else {
			this.ruleParames = ruleParames.split(",");
		}
	}

	public void setRuleParameArray(String[] ruleParames) {
		this.ruleParames = ruleParames;
	}

	public void setDbRuleParames(String dbRuleParames) {
		this.dbRuleParames = dbRuleParames.split(",");
	}

	public void setDbRuleParameArray(String[] dbRuleParames) {
		this.dbRuleParames = dbRuleParames;
	}

	public void setTbRuleParames(String tbRuleParames) {
		this.tbRuleParames = tbRuleParames.split(",");
	}

	public void setTbRuleParameArray(String[] tbRuleParames) {
		this.tbRuleParames = tbRuleParames;
	}

	public void setTbSuffix(String tbSuffix) {
		this.suffixManager.setTbSuffix(tbSuffix);
	}

	public boolean isAllowReverseOutput() {
		return allowReverseOutput;
	}

	public void setAllowReverseOutput(boolean allowReverseOutput) {
		this.allowReverseOutput = allowReverseOutput;
	}

	/*public boolean isNeedRowCopy() {
		return needRowCopy;
	}

	public void setNeedRowCopy(boolean needRowCopy) {
		this.needRowCopy = needRowCopy;
	}*/

	public String getDbIndexPrefix() {
		return dbIndexPrefix;
	}

	public void setDbIndexPrefix(String dbIndexPrefix) {
		this.dbIndexPrefix = dbIndexPrefix;
	}

	public String getDbIndexPattern() {
		return dbIndexPattern;
	}

	public void setDbIndexPattern(String dbIndexPattern) {
		this.dbIndexPattern = dbIndexPattern;
	}

	public int getDbIndexCount() {
		return dbIndexCount;
	}

	public void setDbIndexCount(int dbIndexCount) {
		this.dbIndexCount = dbIndexCount;
	}

	public void setTbSuffixFrom(int tbSuffixFrom) {
		this.suffixManager.getSuffix(0).setTbSuffixFrom(tbSuffixFrom);
	}

	public void setTbSuffixTo(int tbSuffixTo) {
		this.suffixManager.getSuffix(0).setTbSuffixTo(tbSuffixTo);
	}

	public void setTbSuffixWidth(int tbSuffixWidth) {
		this.suffixManager.getSuffix(0).setTbSuffixWidth(tbSuffixWidth);
	}

	public void setTbSuffixPadding(String tbSuffixPadding) {
		this.suffixManager.getSuffix(0).setTbSuffixPadding(tbSuffixPadding);
	}

	public void setTbNumForEachDb(int tbNumForEachDb) {
		this.suffixManager.getSuffix(0).setTbNumForEachDb(tbNumForEachDb);
	}

	@Override
	public TableRule clone() throws CloneNotSupportedException {
		return (TableRule) super.clone();
	}

	public boolean isDisableFullTableScan() {
		return disableFullTableScan;
	}

	public void setDisableFullTableScan(boolean disableFullTableScan) {
		this.disableFullTableScan = disableFullTableScan;
	}

	/**
	 * 获取生成策略，以后TableRule进行小重构，将getSimpleTableRule实现后可废除
	 * 
	 * @return
	 */
	public TABLE_GENERATION_TYPE getTableGenerationType() {
		return tableGenerationType;
	}

	public void setTableGenerationType(TABLE_GENERATION_TYPE tableGenerationType) {
		this.tableGenerationType = tableGenerationType;
	}

	public String getTimeStyle() {
		return timeStyle;
	}

	public void setTimeStyle(String timeStyle) {
		this.timeStyle = timeStyle;
	}

	public String getCalendarType() {
		return calendarType;
	}

	public void setCalendarType(String calendarType) {
		this.calendarType = calendarType;
	}

	public String getFromDateString() {
		return fromDateString;
	}

	public void setFromDateString(String fromDateString) {
		this.fromDateString = fromDateString;
	}

	public String getToDateString() {
		return toDateString;
	}

	public void setToDateString(String toDateString) {
		this.toDateString = toDateString;
	}

	public boolean isOnlyDateSharding() {
		return isOnlyDateSharding;
	}

	public void setOnlyDateSharding(boolean isOnlyDateSharding) {
		this.isOnlyDateSharding = isOnlyDateSharding;
	}

	public int getYearsToNow() {
		return yearsToNow;
	}

	public void setYearsToNow(int yearsToNow) {
		this.yearsToNow = yearsToNow;
	}

	public int getYearsBehindNow() {
		return yearsBehindNow;
	}

	public void setYearsBehindNow(int yearsBehindNow) {
		this.yearsBehindNow = yearsBehindNow;
	}

	public void setGroovyScript(String groovyScript) {
		this.groovyScript = groovyScript;
	}

	public String getGroovyScript() {
		return groovyScript;
	}

	public SuffixManager getSuffixManager() {
		return suffixManager;
	}

	public void setExtraPackages(List<String> extraPackages) {
		this.extraPackages = extraPackages;
	}

	public static void main(String[] args) {
		String dbIndexPattern = "MYSQL_FEEL_{0}_GROUP";
		int dbIndexCount = 32;
		MessageFormat mf = new MessageFormat(dbIndexPattern);
		for (int i = 0; i < dbIndexCount; i++) {
			System.out.println("dbIndexes[i] ="+ mf.format(new Object[]{i}));
		}

		System.out.println("aaa|bbb".split("\\|")[0]);
		System.out.println("aaa|bbb".split("\\|")[1]);
	}
}
