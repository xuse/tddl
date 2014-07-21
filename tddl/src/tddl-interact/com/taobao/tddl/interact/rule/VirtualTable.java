package com.taobao.tddl.interact.rule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.interact.rule.bean.AdvancedParameter;
import com.taobao.tddl.interact.rule.bean.DBType;
import com.taobao.tddl.interact.rule.ruleimpl.EnumerativeRule;
import com.taobao.tddl.interact.rule.virtualnode.DBTableMap;
import com.taobao.tddl.interact.rule.virtualnode.TableSlotMap;

/**
 * 一个逻辑表怎样分库分表
 * 
 * @author linxuan
 * 
 */
public class VirtualTable implements VirtualTableRule<String, String> {
	private static Log logger = LogFactory.getLog(VirtualTable.class);
	/** =========================== **/
	/** =====原始的配置字符串======= **/
	/** =========================== **/
	protected String virtualTbName;
	protected String dbNamePattern; // item_{0000}_dbkey
	protected String tbNamePattern; // item_{0000}

	protected String[] dbRules; // string expression or mappingrule
	protected String[] tbRules; // string expression or mappingrule
	protected List<String> extraPackages;// add by junyu:用户自定义包

	/**
	 * 用来替换dbRules、tbRules中的占位符
	 * 优先用dbRuleParames，tbRuleParames替换，其为空时再用ruleParames替换
	 */
	protected String[] ruleParames;
	protected String[] dbRuleParames;
	protected String[] tbRuleParames;
	/** ============================= **/

	protected List<Rule<String>> dbShardRules;
	protected List<Rule<String>> tbShardRules;

	// add by junyu:支持自定义函数package
	protected static String extraPackagesStr;
	protected Map<String, Set<String>> actualTopology;

	protected Object outerContext;

	private DBType dbType = null; // Oracle|MySql
	protected boolean allowReverseOutput; // 是否允许反向输出
	protected boolean allowFullTableScan; // 是否允许全表扫描
	protected boolean disableFullTableScan = true; // 是否关闭全表扫描

	/**
	 * 虚拟节点映射
	 */
	protected TableSlotMap tableSlotMap;
	protected DBTableMap dbTableMap;
	protected String tableSlotKeyFormat;

	/**
	 * 是否需要行复制。若TDataSource挂了行复制，会在初始化时将该值设置为true
	 */
	protected boolean needRowCopy = false;

	/**
	 * 用在行复制中，若TDataSource挂了行复制，则初始化时会根据复制配置设置这个属性。
	 * 
	 * 唯一键是有优先级的。优先级高的，排在List中前面 每个级别的唯一键，支持多个，类似联合主键的概念。但是，后续其他关联处理，目前不考虑多个唯一键
	 * 所有List中的每个Set现在只能有一个元素。即每个级别一个唯一键
	 * 
	 * 当一个SQL到达时，按优先级遍历该list，第一个在sql中都包含的级别，其对应唯一键被返回作为解析后的唯一键（主键），用来在行复制中查询主库
	 */
	protected List<String> uniqueKeys;

	/**
	 * 是否是个广播表  AndOr需要用他来标记某个表是否需要进行复制。
	 */
	private boolean broadcast = false;
	
	/**
	 * 相同的join group 应该具有相同的切分规则
	 */
	private String joinGroup = null;
	
	public void init() {
		if (tbShardRules == null || tbShardRules.size() == 0) {
			if (this.tbNamePattern == null) {
				// 表规则没有，tbKeyPattern为空
				this.tbNamePattern = this.virtualTbName;
			}
		}

		initActualTopology();
	}

	protected void initVnodeMap() {
		if (tableSlotMap != null) {
			tableSlotMap.setTableSlotKeyFormat(tableSlotKeyFormat);
			tableSlotMap.setLogicTable(this.virtualTbName);
			tableSlotMap.init();
		}

		if (dbTableMap != null) {
			dbTableMap.setTableSlotKeyFormat(tableSlotKeyFormat);
			dbTableMap.setLogicTable(this.virtualTbName);
			dbTableMap.init();
		}
	}

	private static final String tableNameSepInSpring = ",";

	public void initActualTopology() {
		if (actualTopology != null) {
			// 将逗号分隔的表名转换为Set
			for (Map.Entry<String/* 库 */, Set<String/* 表 */>> e : this.actualTopology
					.entrySet()) {
				if (e.getValue().size() == 1) { // 如果只有一个元素，则怀疑为逗号分隔的
					Set<String> tables = new LinkedHashSet<String>();
					tables.addAll(Arrays.asList(e.getValue().iterator().next()
							.split(tableNameSepInSpring)));
					e.setValue(tables);
				}
			}
			showTopology(false);
			return; // 用户显式设置的优先
		}
		actualTopology = new TreeMap<String, Set<String>>();

		if ((dbShardRules == null || dbShardRules.size() == 0)
				&& (tbShardRules == null || tbShardRules.size() == 0)) {
			Set<String> tbs = new TreeSet<String>();
			tbs.add(this.tbNamePattern);
			actualTopology.put(this.dbNamePattern, tbs);
		} else if (dbShardRules == null || dbShardRules.size() == 0) { // 没有库规则
			Set<String> tbs = new TreeSet<String>();
			for (Rule<String> tbRule : tbShardRules) {
				tbs.addAll(vbvRule(tbRule, getEnumerates(tbRule)));
			}
			actualTopology.put(this.dbNamePattern, tbs);
		} else if (tbShardRules == null || tbShardRules.size() == 0) {// 没有表规则
			Set<String> tbs = new TreeSet<String>();
			tbs.add(this.tbNamePattern);
			for (Rule<String> dbRule : dbShardRules) {
				for (String dbIndex : vbvRule(dbRule, getEnumerates(dbRule))) {
					actualTopology.put(dbIndex, tbs);
				}
			}
		} else { // 库表规则都有
			for (Rule<String> dbRule : dbShardRules) {
				for (Rule<String> tbRule : tbShardRules) {
					if (this.tableSlotMap != null && this.dbTableMap != null) {
						valuebyvalue(this.actualTopology, dbRule, tbRule, true);
					} else {
						valuebyvalue(this.actualTopology, dbRule, tbRule, false);
					}
				}
			}
		}
		showTopology(true);
	}

	private static Set<String> vbvRule(Rule<String> rule, Samples samples) {
		Set<String> tbs = new TreeSet<String>();
		for (Map<String, Object> sample : samples) {
			tbs.add(rule.eval(sample, null));
		}
		return tbs;
	}

	private Set<String> vbvRule(Rule<String> rule,
			Map<String, Set<Object>> enumerates) {
		Set<String> tbs = new TreeSet<String>();
		for (Map<String, Object> sample : new Samples(enumerates)) {
			tbs.add(rule.eval(sample, null));
		}
		return tbs;
	}

	private static Map<String, Samples> vbvTrace(Rule<String> rule,
			Map<String, Set<Object>> enumerates) {
		Map<String, Samples> db2Samples = new TreeMap<String, Samples>();
		Samples dbSamples = new Samples(enumerates);
		for (Map<String, Object> sample : dbSamples) {
			String v = rule.eval(sample, null);
			Samples s = db2Samples.get(v);
			if (s == null) {
				s = new Samples(sample.keySet());
				db2Samples.put(v, s);
			}
			s.addSample(sample);
		}
		return db2Samples;
	}

	private static void valuebyvalue(Map<String, Set<String>> topology,
			Rule<String> dbRule, Rule<String> tbRule, boolean isVnode) {
		Map<String/* 列名 */, Set<Object>> dbEnumerates = getEnumerates(dbRule);
		Map<String/* 列名 */, Set<Object>> tbEnumerates = getEnumerates(tbRule);

		Set<AdvancedParameter> params = cast(tbRule.getRuleColumnSet());
		for (AdvancedParameter tbap : params) {
			if (dbEnumerates.containsKey(tbap.key)) {
				// 库表规则的公共列名，表枚举值要涵盖所有库枚举值跨越的范围= =!
				Set<Object> tbValuesBasedONdbValue = new HashSet<Object>();
				for (Object dbValue : dbEnumerates.get(tbap.key)) {
					tbValuesBasedONdbValue.addAll(tbap.enumerateRange(dbValue));
				}
				dbEnumerates.get(tbap.key).addAll(tbValuesBasedONdbValue);
			} else {
				dbEnumerates.put(tbap.key, tbEnumerates.get(tbap.key));
			}
		}

		// 有虚拟节点的话按照虚拟节点计算
		if (isVnode) {
			Samples tabSamples = new Samples(tbEnumerates);
			Set<String> tbs = new TreeSet<String>();
			for (Map<String, Object> sample : tabSamples) {
				String value = tbRule.eval(sample, null);
				tbs.add(value);
			}

			for (String table : tbs) {
				Map<String, Object> sample = new HashMap<String, Object>(1);
				sample.put(EnumerativeRule.REAL_TABLE_NAME_KEY, table);
				String db = dbRule.eval(sample, null);
				if (topology.get(db) == null) {
					Set<String> tabs = new HashSet<String>();
					tabs.add(table);
					topology.put(db, tabs);
				} else {
					topology.get(db).add(table);
				}
			}

			return;
		}

		// 没有虚拟节点按正常走
		Map<String, Samples> dbs = vbvTrace(dbRule, dbEnumerates);// 库计算结果，与得到结果的输入值集合
		for (Map.Entry<String/* 库值 */, Samples> e : dbs.entrySet()) {
			Set<String> tbs = topology.get(e.getKey());
			if (tbs == null) {
				tbs = vbvRule(tbRule, e.getValue());
				topology.put(e.getKey(), tbs);
			} else {
				tbs.addAll(vbvRule(tbRule, e.getValue()));
			}
		}
	}

	/**
	 * 根据#id,1,32|512_64#中第三段定义的遍历值范围，枚举规则中每个列的遍历值
	 */
	@SuppressWarnings("rawtypes")
	private static Map<String/* 列名 */, Set<Object>/* 列值 */> getEnumerates(
			Rule rule) {
		Set<AdvancedParameter> params = cast(rule.getRuleColumnSet());
		Map<String/* 列名 */, Set<Object>/* 列值 */> enumerates = new HashMap<String, Set<Object>>(
				params.size());
		for (AdvancedParameter ap : params) {
			enumerates.put(ap.key, ap.enumerateRange());
		}
		return enumerates;
	}

	private static final int showColsPerRow = 5;

	private void showTopology(boolean showMap) {
		int crossIndex, endIndex, maxcolsPerRow = showColsPerRow, maxtbnlen = 1, maxdbnlen = 1;
		for (Map.Entry<String, Set<String>> e : this.actualTopology.entrySet()) {
			int colsPerRow = colsPerRow(e.getValue(), showColsPerRow);
			if (colsPerRow > maxcolsPerRow) {
				maxcolsPerRow = colsPerRow;
			}
			if (e.getKey().length() > maxdbnlen) {
				maxdbnlen = e.getKey().length(); // dbIndex最大长度
			}
			for (String tbn : e.getValue()) {
				if (tbn.length() > maxtbnlen) {
					maxtbnlen = tbn.length(); // tableName最大长度
				}
			}
		}
		crossIndex = maxdbnlen + 1;
		endIndex = crossIndex + (maxtbnlen + 1) * maxcolsPerRow + 1;
		StringBuilder sb = new StringBuilder(
				"The topology of the virtual table " + this.virtualTbName);
		addLine(sb, crossIndex, endIndex);
		for (Map.Entry<String/* 库 */, Set<String/* 表 */>> e : this.actualTopology
				.entrySet()) {
			sb.append("\n|");
			sb.append(fillAfter(e.getKey(), maxdbnlen)).append("|");
			int i = 0, n = e.getValue().size();
			for (String tb : e.getValue()) {
				sb.append(fillAfter(tb, maxtbnlen)).append(",");
				i++;
				if (i % maxcolsPerRow == 0 && i < n) {
					sb.append("|\n|").append(fillAfter(" ", maxdbnlen))
							.append("|");// 折行后把库列输出
				}
			}
			if (i % maxcolsPerRow != 0) {
				int taillen = (maxcolsPerRow - (i % maxcolsPerRow))
						* (maxtbnlen + 1) + 1;
				sb.append(fillBefore("|", taillen));
			} else {
				sb.append("|");
			}

			addLine(sb, crossIndex, endIndex);
		}
		sb.append("\n");
		
		if(logger.isDebugEnabled()){
		    logger.debug(sb.toString());
		}

		if (!showMap) {
			return;
		}
		sb = new StringBuilder(
				"\nYou could add below segement as the actualTopology property to ");
		sb.append(this.virtualTbName + "'s TableRule bean in the rule file\n\n");
		sb.append("        <property name=\"actualTopology\">\n");
		sb.append("          <map>\n");
		for (Map.Entry<String/* 库 */, Set<String/* 表 */>> e : this.actualTopology
				.entrySet()) {
			sb.append("            <entry key=\"").append(e.getKey())
					.append("\" value=\"");
			for (String table : e.getValue()) {
				sb.append(table).append(tableNameSepInSpring);
			}
			if (sb.charAt(sb.length() - 1) == tableNameSepInSpring.charAt(0)) {
				sb.deleteCharAt(sb.length() - 1);
			}
			sb.append("\" />\n");
		}
		sb.append("          </map>\n");
		sb.append("        </property>\n");
		
		if(logger.isDebugEnabled()){
		    logger.debug(sb.toString());
		}
	}

	private int colsPerRow(Collection<String> c, int maxColPerRow) {
		int n = c.size();
		if (n <= maxColPerRow) {
			return n;
		}
		int maxfiti = maxColPerRow; // 不能整除的情况下，让最后一行空白最少的那个i
		int minblank = maxColPerRow; // 不能整除的情况下，最后一行的空白个数
		for (int i = maxColPerRow; i > 0; i--) {
			int mod = n % i;
			if (mod == 0) {
				if (n / i <= i) {
					return i; // 行数不能大于列数
				} else {
					break;
				}
			} else {
				if (i - mod < minblank) {
					minblank = i - mod;
					maxfiti = i;
				}
			}
		}

		return maxfiti;
	}

	private String fillAfter(String str, int len) {
		if (str.length() < len) {
			for (int i = 0, n = len - str.length(); i < n; i++) {
				str = str + " ";
			}
		}
		return str;
	}

	private String fillBefore(String str, int len) {
		if (str.length() < len) {
			for (int i = 0, n = len - str.length(); i < n; i++) {
				str = " " + str;
			}
		}
		return str;
	}

	private void addLine(StringBuilder sb, int crossIndex, int endIndex) {
		sb.append("\n+");
		for (int i = 1; i <= endIndex; i++) {
			if (i == crossIndex || i == endIndex) {
				sb.append("+");
			} else {
				sb.append("-");
			}
		}
	}

	public VirtualTable clone() throws CloneNotSupportedException {
		return (VirtualTable) super.clone();
	}

	@SuppressWarnings("unchecked")
	private static <T> T cast(Object obj) {
		return (T) obj;
	}

	public List<Rule<String>> getDbShardRules() {
		return dbShardRules;
	}

	public void setDbShardRules(List<Rule<String>> dbShardRules) {
		this.dbShardRules = dbShardRules;
	}

	public List<Rule<String>> getTbShardRules() {
		return tbShardRules;
	}

	public void setTbShardRules(List<Rule<String>> tbShardRules) {
		this.tbShardRules = tbShardRules;
	}

	public DBType getDbType() {
		return dbType;
	}

	public void setDbType(DBType dbType) {
		this.dbType = dbType;
	}

	public boolean isAllowReverseOutput() {
		return allowReverseOutput;
	}

	public boolean isNeedRowCopy() {
		return needRowCopy;
	}

	public List<String> getUniqueKeys() {
		return uniqueKeys;
	}

	public boolean isAllowFullTableScan() {
		return allowFullTableScan;
	}

	public Map<String, Set<String>> getActualTopology() {
		return actualTopology;
	}

	public String getVirtualTbName() {
		return virtualTbName;
	}

	public void setVirtualTbName(String virtualTbName) {
		this.virtualTbName = virtualTbName;
	}

	public void setExtraPackagesStr(List<String> extraPackages) {
		StringBuilder ep = new StringBuilder("");
		if (extraPackages != null) {
			int packNum = extraPackages.size();
			for (int i = 0; i < packNum; i++) {
				ep.append("import ");
				ep.append(extraPackages.get(i));
				ep.append(";");
			}
		}
		extraPackagesStr = ep.toString();
	}

	protected List<String> trimRuleString(List<String> ruleStrings) {
		List<String> result = new ArrayList<String>(ruleStrings.size());
		for (String ruleString : ruleStrings) {
			result.add(ruleString.trim());
		}
		return result;
	}

	public Object getOuterContext() {
		return outerContext;
	}

	public TableSlotMap getTableSlotMap() {
		return tableSlotMap;
	}

	public DBTableMap getDbTableMap() {
		return dbTableMap;
	}

	public String getDbNamePattern() {
		return dbNamePattern;
	}

	public String getTbNamePattern() {
		return tbNamePattern;
	}

	public String[] getDbRuleStrs() {
		return dbRules;
	}

	public String[] getTbRulesStrs() {
		return tbRules;
	}

	public boolean isBroadcast() {
		return broadcast;
	}

	public void setBroadcast(boolean broadcast) {
		this.broadcast = broadcast;
	}

	public String getJoinGroup() {
		return joinGroup;
	}

	public void setJoinGroup(String joinGroup) {
		this.joinGroup = joinGroup;
	}
	
	
}
