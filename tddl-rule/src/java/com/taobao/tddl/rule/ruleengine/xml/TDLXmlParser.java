package com.taobao.tddl.rule.ruleengine.xml;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.taobao.tddl.common.exception.runtime.CantfindConfigFileByPathException;
import com.taobao.tddl.common.exception.runtime.TDLRunTimeException;
import static com.taobao.tddl.common.util.StringUtils.trim;
import static com.taobao.tddl.common.util.StringUtils.validAndTrim;
import com.taobao.tddl.rule.ruleengine.TableRuleType;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.DBRule;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.LogicTabMatrix;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.TabRule;

public class TDLXmlParser {
	private static final Log log = LogFactory.getLog(TDLXmlParser.class);
	/**
	 * 根据输入的path地址获取含有所有dbrule和tableRule的Map. 只在初始化的时候调用一次.
	 * 
	 * 
	 * @param path
	 *            路径名系统内采用 getClass().getResourceAsStream(path);的方式获取对应的stream资源
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public synchronized void initLogicTabMarticMap(String path,
			Map<String, LogicTabMatrix> map) {
			Document doc = getDocument(path);
			Element ele = doc.getRootElement();
			String dbType= trim(ele.attributeValue("dbType"));
			Iterator dbTabItr = ele.elementIterator("table");
			int i = 1;
			while (dbTabItr.hasNext()) {
				Element aVtab = (Element) dbTabItr.next();
				LogicTabMatrix aVtabMatrix = new LogicTabMatrix();
				// globalTableRule,如果有则储存起来
				Element globeRule = aVtab.element("globalTableRule");
				if (globeRule != null) {
					TabRule globTabRule = this.getTabRule(globeRule);
					aVtabMatrix.setGlobalTableRule(globTabRule);
				}
				String logicName = validAndTrim(aVtab
						.attributeValue("logicName"), "无法找到第" + i
						+ "个虚拟表的logicName");
				//martrix里面的TableName是返还给前端用于表名替换的，因此要保持原样
				aVtabMatrix.setTableName(logicName);
				String needRowCopy = trim(aVtab.attributeValue("rowCopy"));
				if(needRowCopy != null&&!needRowCopy.equals("")){
					aVtabMatrix.setNeedRowCopy(Boolean.valueOf(needRowCopy));
				}
				String reverseOutput = trim(aVtab.attributeValue("reverseOutput"));
				if(reverseOutput!=null&&!reverseOutput.equals("")){
					aVtabMatrix.setAllowReverseOutput(Boolean.valueOf(reverseOutput));
				}
				aVtabMatrix.setDBType(dbType);
				String tabFactor = trim(aVtab.elementText("tableFactor"));
				aVtabMatrix.setTableFactor(tabFactor);
				Element dbRules = (Element) aVtab.element("dbRules");
				Iterator rulesItr = dbRules.elementIterator("dbRule");
				Map<String,DBRule> ruleMap = getRuleList(rulesItr,aVtabMatrix);
				//当一个Rule对象有expression的时候放在depositedRule和allRule
				//如果没有expressionString串，则只放在allRule待选
				Map<String,DBRule> depositedRules = getDepositedRule(ruleMap);
				aVtabMatrix.setAllRules(ruleMap);
				aVtabMatrix.setDepositedRules(depositedRules);
				List<DBRule> defaultRuleList = getDefaultRuleList(aVtab,
						aVtabMatrix);
				aVtabMatrix.setDefaultRules(defaultRuleList);

			
				map.put(logicName.toLowerCase(), aVtabMatrix);
				i++;
		}
	}

	private Map<String,DBRule> getDepositedRule(Map<String,DBRule> ruleMap){
		Map<String,DBRule> retMap=new HashMap<String, DBRule>(ruleMap.size());
		for(Entry<String, DBRule> ent:ruleMap.entrySet()){
			if(!ent.getValue().getExpression().equals("")){
				if(ent.getValue().getParameters().equals("")){
					throw new IllegalArgumentException("在depositedRule中必须输入parameters参数。");
				}
				retMap.put(ent.getKey(), ent.getValue());
			}
			if(!ent.getValue().getPrimaryKeyExp().equals("")){
				if(ent.getValue().getPrimaryKey().equals("")){
					throw new IllegalArgumentException("在depositedRule中必须输入primary key参数。");
				}
				retMap.put(ent.getKey(), ent.getValue());
			}
		}
		return retMap;
	}
	/**
	 * 根据DefaultPool字段（允许用“,”分隔)，获取DBRule中含有指定写库的DBRule
	 * 
	 * @param aVtab
	 * @param aVtabMatrix
	 * @param tempSet
	 * @return
	 */
	private List<DBRule> getDefaultRuleList(Element aVtab,
			LogicTabMatrix aVtabMatrix) {
		List<DBRule> defaultList = new ArrayList<DBRule>();
		String defaultPools = trim(aVtab.attributeValue("defaultWritePool"));
		if (defaultPools != null) {
			String[] defaultPoolsStrArray = defaultPools.split(",");
			Map<String, DBRule> map=aVtabMatrix.getAllRules();
			for (String str : defaultPoolsStrArray) {
				DBRule dbrule=map.get(str.trim());
				if(dbrule!=null){
				defaultList.add(dbrule);
				}else{
					throw new IllegalArgumentException("defaultRule中id为:"+str+" 的字段不能找到" +
							"一个对应的规则，请确认该id对应一个dbRule的id参数");
				}
			}
		}
		return defaultList;
	}

	/**
	 * 解析每一个rule
	 * 
	 * @param rulesItr
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Map<String,DBRule> getRuleList(Iterator rulesItr,LogicTabMatrix aVtabMatrix) {
		Map<String,DBRule> rules = new HashMap<String,DBRule>();
	
		while (rulesItr.hasNext()) {
			
			Element ruleEle = (Element) rulesItr.next();
			String id=validAndTrim(ruleEle.attributeValue("id"),
					"必须指定Rule的id");
			DBRule rule = new DBRule();
			String exp=trim(ruleEle.elementText("expression"));
			rule.setExpression(exp);
			String primaryKey=trim(ruleEle.elementText("primaryKey"));
			rule.setPrimaryKey(primaryKey);
			String primaryKeyExp=trim(ruleEle.elementText("primaryKeyExp"));
			rule.setPrimaryKeyExp(primaryKeyExp);
			String parameters=trim(ruleEle.elementText("parameters"));
			rule.setParameters(parameters);
			String readPoolStr = ruleEle.elementText("readPools");
			String writePoolStr = ruleEle.elementText("writePools");
			if (readPoolStr == null || writePoolStr == null
					|| readPoolStr.trim().equals("")
					|| writePoolStr.trim().equals("")) {
				throw new TDLRunTimeException("readPool和writePool必须同时指名"
						+ "readPool可以和writePool同名，但writePool不能为多个，也不能为虚拟池");
			}
			String[] readPools = readPoolStr.trim().split(",");
			String[] writePools = writePoolStr.trim().split(",");

			rule.setReadPool(readPools);
			rule.setWritePool(writePools);
			Element subTableRuleElement = ruleEle.element("tableRule");
			if (subTableRuleElement != null) {
				TabRule tabRule = getTabRule(subTableRuleElement);
				
				tabRule.setPrimaryKey(primaryKey);
				rule.setDBSubTabRule(tabRule);
				log.debug("id:"+id+"的DBRule有subTableRule,因此使用subTableRule");
			}
			//add by shenxun 现在的逻辑是直接替换subRuleEle,以后不会再
			//使用globalRule这个项目了，它只在载入的时候临时起作用
			else{
				TabRule tabRule=aVtabMatrix.getGlobalTableRule();
				if(tabRule!=null){
					tabRule.setPrimaryKey(primaryKey);
				}
				rule.setDBSubTabRule(tabRule);
				log.debug("id:"+id+"的DBRule没有subTableRule,因此使用globalTableRule");
			}
			rules.put(id,rule);
		}
		return rules;
	}

	/**
	 * 获取表规则。
	 * 
	 * @param ele
	 * @return
	 */
	private TabRule getTabRule(Element ele) {
		TabRule tabRule = new TabRule();
		String parameters = trim(ele.elementText("parameters"));
		tabRule.setParameter(parameters);
		String expression = trim(ele.elementText("expression"));
		tabRule.setExpFunction(expression);
		String type = trim(ele.elementText("type"));
		if (type != null && !type.equals("")) {
			tabRule.setTableType(TableRuleType.valueOf(type.toUpperCase()));
		}
		String padding=trim(ele.elementText("padding"));
		tabRule.setPadding(padding);
		String width=trim(ele.elementText("width"));
		tabRule.setWidth(width);
		tabRule.setDefaultTable(trim(ele.elementText("defaultTableRange")));
		String offset = ele.elementText("offset");
		if (offset != null && !offset.trim().equals("")) {
			tabRule.setOffset(Integer.valueOf(offset.trim()).intValue());
		}
		return tabRule;
	}

	/**
	 * 根据路径获取dom4j
	 * 
	 * @param path
	 * @return
	 */
	private Document getDocument(String path) {
		Document doc = null;
		InputStream in = null;
		if (path == null) {
			throw new CantfindConfigFileByPathException(null);
		}
		path = path.trim();
		if (path.equals("")) {
			throw new CantfindConfigFileByPathException(path);
		}
		try {
			in = TDLXmlParser.class.getResourceAsStream(path);
			if (in == null) {
				try {
					in=new FileInputStream(new File(path));
				} catch (FileNotFoundException e) {
					throw new TDLRunTimeException("指定的mapping文件不正确，所指定路径为：" + path
							+ "" + "若使用/filename,则会从path的class根目录上去找资源,一般使用这个即可"
							+ "若使用filename,则会从类的相对目录去寻找资源,如果不能找到，也会尝试使用File的方式来找",e);
			
				}
			}
			Reader read = new InputStreamReader(in);
			SAXReader reader = new SAXReader();
			doc = reader.read(read);
		} catch (DocumentException e) {
			throw new TDLRunTimeException("解析映射文件时发生错误,请检查映射文件", e);
		}
		return doc;
	}
}
