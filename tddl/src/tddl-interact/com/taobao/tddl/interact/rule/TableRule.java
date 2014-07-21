package com.taobao.tddl.interact.rule;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.interact.rule.ruleimpl.DbVirtualNodeRule;
import com.taobao.tddl.interact.rule.ruleimpl.GroovyRule;
import com.taobao.tddl.interact.rule.ruleimpl.TableVirtualNodeRule;
import com.taobao.tddl.interact.rule.ruleimpl.WrappedGroovyRule;
import com.taobao.tddl.interact.rule.util.SimpleNamedMessageFormat;
import com.taobao.tddl.interact.rule.virtualnode.DBTableMap;
import com.taobao.tddl.interact.rule.virtualnode.TableSlotMap;

/**
 * 一个逻辑表怎样分库分表
 *
 * @author linxuan
 *
 */
public class TableRule extends VirtualTable {
	Log logger = LogFactory.getLog(TableRule.class);
	public void init() {
		//不要随意调动位置
		super.setExtraPackagesStr(extraPackages);
		initDbIndexes();
		initVnodeMap();
		replaceWithParam(this.dbRules, dbRuleParames != null ? dbRuleParames : ruleParames);
		replaceWithParam(this.tbRules, tbRuleParames != null ? tbRuleParames : ruleParames);
		super.setDbShardRules(convertToRuleArray(dbRules, dbNamePattern,tableSlotMap,dbTableMap,false));
		super.setTbShardRules(convertToRuleArray(tbRules, tbNamePattern,tableSlotMap,dbTableMap,true));
		super.init();
	}

	protected final void initDbIndexes() {
		//TODO
	}

	protected static void replaceWithParam(Object[] rules, String[] params) {
		if (params == null || rules == null) {
			return;
		}
		for (int i = 0; i < rules.length; i++) {
			if (rules[i] instanceof String) {
				rules[i] = replaceWithParam((String) rules[i], params);
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
				throw new IllegalArgumentException("使用名字化的占位符替换失败！请检查配置。 params:" + Arrays.asList(params));
			}
			args.put(param.substring(0, index).trim(), param.substring(index + 1).trim());
		}
		return new SimpleNamedMessageFormat(template).format(args);
	}

	protected static List<Rule<String>> convertToRuleArray(Object[] rules,String keyPattern,TableSlotMap tableSlotMap,DBTableMap dbTableMap,boolean isTableRule) {
		List<Rule<String>> ruleList = new ArrayList<Rule<String>>(1);
		if (null == rules) {
			//按照现在需求不可能为tableRule
			if(tableSlotMap!=null&&dbTableMap!=null&&!isTableRule){
			    ruleList.add(new DbVirtualNodeRule(String.valueOf(""), dbTableMap,extraPackagesStr));
			    return ruleList;
			}else{
			    return null;
			}
		}

		for (Object rule : rules) {
			if (keyPattern != null && keyPattern.length() != 0) {
				ruleList.add(new WrappedGroovyRule(String.valueOf(rule), keyPattern,extraPackagesStr));
			} else {
				if(tableSlotMap!=null&&dbTableMap!=null&&isTableRule){
					ruleList.add(new TableVirtualNodeRule(String.valueOf(rule), tableSlotMap,extraPackagesStr));
				}else{
				    ruleList.add(new GroovyRule<String>(String.valueOf(rule),extraPackagesStr));
				}
			}
		}

		return ruleList;
	}

	public void setDbRuleArray(List<String> dbRules) {
		//若类型改为String[],spring会自动以逗号分隔，变态！
		dbRules = trimRuleString(dbRules);
		this.dbRules = dbRules.toArray(new String[dbRules.size()]);
	}

	public void setTbRuleArray(List<String> tbRules) {
		//若类型改为String[],spring会自动以逗号分隔，变态！
		tbRules = trimRuleString(tbRules);
		this.tbRules = tbRules.toArray(new String[tbRules.size()]);
	}

	public void setDbRules(String dbRules) {
		if (this.dbRules == null) {
			// 优先级比dbRuleArray低
			//this.dbRules = dbRules.split("\\|");
			this.dbRules = new String[] { dbRules.trim() }; //废掉|分隔符，没人用且容易造成混乱
		}
	}

	public void setTbRules(String tbRules) {
		if (this.tbRules == null) {
			// 优先级比tbRuleArray低
			//this.tbRules = tbRules.split("\\|");
			this.tbRules = new String[] { tbRules.trim() }; //废掉|分隔符，没人用且容易造成混乱
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

	public void setExtraPackages(List<String> extraPackages) {
		this.extraPackages = extraPackages;
	}
	
	public void setTableSlotKeyFormat(String tableSlotKeyFormat) {
		this.tableSlotKeyFormat = tableSlotKeyFormat;
	}
	
	public void setDbNamePattern(String dbKeyPattern) {
		this.dbNamePattern = dbKeyPattern;
	}
	
	public void setTbNamePattern(String tbKeyPattern) {
		this.tbNamePattern = tbKeyPattern;
	}
	
	public void setAllowReverseOutput(boolean allowReverseOutput) {
		this.allowReverseOutput = allowReverseOutput;
	}

	public boolean isDisableFullTableScan() {
		return disableFullTableScan;
	}

	public void setDisableFullTableScan(boolean disableFullTableScan) {
		this.disableFullTableScan = disableFullTableScan;
	}
	
	public void setNeedRowCopy(boolean needRowCopy) {
		this.needRowCopy = needRowCopy;
	}
	
	public void setUniqueKeys(List<String> uniqueKeys) {
		this.uniqueKeys = uniqueKeys;
	}
	
	public void setAllowFullTableScan(boolean allowFullTableScan) {
		this.allowFullTableScan = allowFullTableScan;
	}
	
	public void setActualTopology(Map<String, Set<String>> actualTopology) {
		this.actualTopology = actualTopology;
	}
	
	public void setOuterContext(Map<Object, Object> outerContext) {
		this.outerContext = outerContext;
	}
	
	public void setTableSlotMap(TableSlotMap tableSlotMap) {
		this.tableSlotMap = tableSlotMap;
	}

	public void setDbTableMap(DBTableMap dbTableMap) {
		this.dbTableMap = dbTableMap;
	}
}
