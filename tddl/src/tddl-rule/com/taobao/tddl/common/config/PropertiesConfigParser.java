package com.taobao.tddl.common.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.common.lang.io.ByteArrayInputStream;
import com.taobao.tddl.common.config.beans.AppRule;
import com.taobao.tddl.interact.rule.TableRule;
import com.taobao.tddl.interact.rule.VirtualTable;
import com.taobao.tddl.interact.rule.VirtualTableRoot;
import com.taobao.tddl.rule.bean.PropertyBaseTDDLRoot;

public class PropertiesConfigParser<T> implements TddlConfigParser<T> {
	Log log = LogFactory.getLog(PropertiesConfigParser.class);
	public static final String Prop_Key_244_tableRules = "tableRules";

	@SuppressWarnings("unchecked")
	public T parseCongfig(String txt) {
		Properties prop = new Properties();
		try {
			prop.load(new ByteArrayInputStream(txt.getBytes()));
		} catch (IOException e) {
			log.error("[parseCongfig]无法解析推送的配置：" + txt, e);
			return null;
		}

		if (prop.getProperty(PropertyBaseTDDLRoot.TABLE_RULES) != null) {
			//2.4.4之前的旧规则的key是table_rules
			AppRule appRule = new AppRule();
			PropertyBaseTDDLRoot root = new PropertyBaseTDDLRoot();
			root.init(prop);
			appRule.setDefaultTddlRoot(root);
			return (T) appRule;
		} else if (prop.getProperty(Prop_Key_244_tableRules) != null) {
			//2.4.4新规则的key是tableRules
			VirtualTableRoot vtr = parseVirtualTableRoot(prop);
			return (T) vtr;
		}

		log.error("[parseCongfig]No tableRules in properties：" + txt);
		return null;
	}

	public static VirtualTableRoot parseVirtualTableRoot(Properties prop) {
		Map<String, VirtualTable> virtualTableMap = new HashMap<String, VirtualTable>();
		for (String table : prop.getProperty(Prop_Key_244_tableRules).split(",")) {
			TableRule vt = new TableRule();
			table = table.trim();

			String dbNamePatternKey = table + ".dbNamePattern";
			if (prop.get(dbNamePatternKey) != null) {
				vt.setDbNamePattern(prop.getProperty(dbNamePatternKey).trim());
			}

			String tbNamePatternKey = table + ".tbNamePattern";
			if (prop.get(tbNamePatternKey) != null) {
				vt.setTbNamePattern(prop.getProperty(tbNamePatternKey).trim());
			}

			String dbRulesKey = table + ".dbRules";
			if (prop.get(dbRulesKey) != null) {
				vt.setDbRules(prop.getProperty(dbRulesKey).trim());
			}

			String tbRulesKey = table + ".tbRules";
			if (prop.get(tbRulesKey) != null) {
				vt.setTbRules(prop.getProperty(tbRulesKey).trim());
			}

			virtualTableMap.put(table, vt);
		}

		VirtualTableRoot vtr = new VirtualTableRoot();
		vtr.setCompleteDistinct("true".equalsIgnoreCase(getTrim(prop, "completeDistinct")));
		vtr.setDbType(getTrimUpperCase(prop, "dbType"));
		vtr.setDefaultDbIndex(getTrim(prop, "defaultDbIndex"));
		vtr.setNeedIdInGroup("true".equalsIgnoreCase(getTrim(prop, "needIdInGroup")));
		vtr.setTableRules(virtualTableMap);
		vtr.init();
		return vtr;
	}

	private static String getTrim(Properties prop, String key) {
		String value = prop.getProperty(key);
		return value == null ? null : value.trim();
	}

	private static String getTrimUpperCase(Properties prop, String key) {
		String value = prop.getProperty(key);
		return value == null ? null : value.trim().toUpperCase();
	}
}
