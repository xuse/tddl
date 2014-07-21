package com.taobao.tddl.rule.bean;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.interact.bean.Field;
import com.taobao.tddl.interact.bean.TargetDB;
import com.taobao.tddl.interact.rule.bean.ExtraParameterContext;
import com.taobao.tddl.interact.rule.bean.SamplingField;
import com.taobao.tddl.rule.ruleengine.cartesianproductcalculator.CartesianProductCalculator;
import com.taobao.tddl.rule.ruleengine.entities.abstractentities.ListSharedElement;
import com.taobao.tddl.rule.ruleengine.entities.abstractentities.OneToManyEntry;
import com.taobao.tddl.rule.ruleengine.entities.abstractentities.RuleChain;
import com.taobao.tddl.rule.ruleengine.entities.abstractentities.SharedElement;
import com.taobao.tddl.rule.ruleengine.entities.abstractentities.TablePropertiesSetter;
import com.taobao.tddl.rule.ruleengine.entities.convientobjectmaker.TableMapProvider;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.CalculationContextInternal;
import com.taobao.tddl.rule.ruleengine.rule.ListAbstractResultRule;
import com.taobao.tddl.rule.ruleengine.rule.ResultAndMappingKey;
import com.taobao.tddl.rule.ruleengine.util.RuleUtils;

/**
 * 一个数据库的抽象
 * 
 * @author shenxun
 * 
 */
public class Database extends ListSharedElement implements
		TablePropertiesSetter {
	Log log = LogFactory.getLog(ListSharedElement.class);
	// implements TableContainer,TableListResultRuleContainer
	private String dataSourceKey;

	/**
	 * 逻辑表名
	 */
	private String logicTableName;
	/**
	 * 表名List，会在init时变成ruleChain
	 */
	private List<ListAbstractResultRule> tableRuleList;
	/**
	 * 1对多指定过来的Entry
	 */
	private OneToManyEntry oneToManyEntry;

	private TableMapProvider tableMapProvider;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.taobao.tddl.rule.ruleengine.entities.abstractentities.ListSharedElement
	 * #init() 这里主要是先初始化整个subTable，然后才能做相应的动作。
	 */
	public void init() {
		init(true);
	}

	public void init(boolean invokeBySpring) {
		initTableRuleChain();
		initLogicTableName();
		initTableMapProvider();
		initDefaultListResultStragety();

		// 如果有tableRuleProvider 那么初始化子表，tableMapProvier在transmit方法被调用时先做掉了
		if (tableMapProvider != null) {
			if (invokeBySpring) {
				log.warn("请删除"
						+ "在tddl规则中，bean class 为com.taobao.tddl.common.config.beans.TableRule"
						+ " 的规则的bean 中的init-method方法。 " + "目前可兼容，但最好删除");
			} else {
				Map<String, SharedElement> beConstructedMap = getTableMapByTableMapProvider();
				putAutoConstructedMapIntoCurrentTagMap(beConstructedMap);
			}
		}

		super.init(invokeBySpring);
	}

	private void initDefaultListResultStragety() {
		if (defaultListResultStragety == null) {
			defaultListResultStragety = oneToManyEntry
					.getDefaultListResultStragety();
		}
	}

	/*
	*//**
	 * 此方法已经未使用，替换的方法下面，为了单元测试兼容
	 * 
	 * 计算表规则的方法 1. 如果本Database本身没有规则，既对应的库没有分表规则,
	 * 或者虽然有规则，但是当前的sql条件中没有匹配到这些规则，分下面两种情况处理： a
	 * 如果Database有且只有一个表，那么无论默认规则为何都应该取出该表。这种情况主要出现在单库（单,多）表，或（单，多）库 单表的情况下。 b
	 * 如果Database有多个表，那么应该使用默认选库策略（DEFAULT_LIST_RESULT_STRAGETY）。
	 * 这种情况将策略设为DEFAULT_LIST_RESULT_STRAGETY.NONE比较安全;
	 * 
	 * 2. 如果给定了规则
	 * 2.1）给了规则但Database层没有传递定位到本database时计算获得的列名->描点集合的map.那么表示前序数据和当前数据无关
	 * ，直接使用当前数据+sql中匹配的值即可。 2.2）传递了定位到本database时的列名->描点的集合
	 * 2.2.1）传递的描点集合中的key包含了从sql中获取的key，那么表示database和table的数据是会相互产生影响的。
	 * 这个时候优先使用database中传入的 列名->结果集描点。
	 * 2.2.2）传递的描点集合中的key不包含从sql中获取的key,那么表示database和table之间的数据无关
	 * ，直接使用table的数据进行计算。
	 * 
	 * @param targetDB
	 *            目标表对象，在这个方法内被包装
	 * @param sourceTrace
	 *            计算出当前数据库的源变量追踪。
	 * @param map
	 *            规则和规则对应的上下文。
	 */
	/*
	 * public void calculateTable(TargetDB targetDB, Field sourceTrace,
	 * Map<RuleChain, CalculationContextInternal> map) {
	 * CalculationContextInternal calculationContext =
	 * map.get(this.listResultRule); Map<String,Field> resultSet = null;
	 * 
	 * if (calculationContext == null) { // 表示没有指定规则，包含当前规则为null和没有匹配到数据，用默认 if
	 * (subSharedElement != null && subSharedElement.size() == 1) { // 1）
	 * resultSet = builSingleTable(); } else { //2) resultSet =
	 * buildDefaultTable(); } } else if (sourceTrace ==
	 * null||sourceTrace.sourceKeys.isEmpty()) {
	 * //2.1）Database层没有传递定位到本database时计算获得的列名->描点集合的map
	 * 
	 * ListAbstractResultRule rule =
	 * calculationContext.ruleChain.getRuleByIndex(calculationContext.index);
	 * Map<String, Set<Object>> argsFromSQL = getEnumeratedSqlArgsMap(
	 * calculationContext, rule); resultSet = rule.evalElement(argsFromSQL); }
	 * else { //2.2）传递了定位到本database时的列名->描点的集合 ListAbstractResultRule rule =
	 * calculationContext.ruleChain.getRuleByIndex(calculationContext.index);
	 * Map<String, Set<Object>> argsFromSQL = getEnumeratedSqlArgsMap(
	 * calculationContext, rule);
	 * //有关系的数据应该优先。putAll会用传入的sourceKeys（对本database有效的描点集）覆盖全部sql信息的计算结果
	 * Map<String 列名 , Set<Object> 得到该结果的描点值名 > sourceKeys =
	 * sourceTrace.sourceKeys; for(Entry<String, Set<Object>>
	 * entry:sourceKeys.entrySet()){
	 * if(argsFromSQL.containsKey(entry.getKey())){
	 * argsFromSQL.put(entry.getKey(), entry.getValue());
	 * log.debug("put entry: "+entry +" to args"); } // else{ //
	 * //从分库来的列中没有分表所需要的数据的时候，不放进去 // } } resultSet =
	 * rule.evalElement(argsFromSQL); }
	 * 
	 * buildTableNameSet(targetDB, resultSet); }
	 */
	/**
	 * 计算表规则的方法 1. 如果本Database本身没有规则，既对应的库没有分表规则,
	 * 或者虽然有规则，但是当前的sql条件中没有匹配到这些规则，分下面两种情况处理： a
	 * 如果Database有且只有一个表，那么无论默认规则为何都应该取出该表。这种情况主要出现在单库（单,多）表，或（单，多）库 单表的情况下。 b
	 * 如果Database有多个表，那么应该使用默认选库策略（DEFAULT_LIST_RESULT_STRAGETY）。
	 * 这种情况将策略设为DEFAULT_LIST_RESULT_STRAGETY.NONE比较安全;
	 * 
	 * 2. 如果给定了规则
	 * 2.1）给了规则但Database层没有传递定位到本database时计算获得的列名->描点集合的map.那么表示前序数据和当前数据无关
	 * ，直接使用当前数据+sql中匹配的值即可。 2.2）传递了定位到本database时的列名->描点的集合
	 * 2.2.1）传递的描点集合中的key包含了从sql中获取的key，那么表示database和table的数据是会相互产生影响的。
	 * 这个时候优先使用database中传入的 列名->结果集描点。
	 * 2.2.2）传递的描点集合中的key不包含从sql中获取的key,那么表示database和table之间的数据无关
	 * ，直接使用table的数据进行计算。
	 * 
	 * @param targetDB
	 *            目标表对象，在这个方法内被包装
	 * @param sourceTrace
	 *            计算出当前数据库的源变量追踪。
	 * @param map
	 *            规则和规则对应的上下文。
	 */
	public void calculateTable(TargetDB targetDB, Field sourceTrace,
			Map<RuleChain, CalculationContextInternal> map,
			ExtraParameterContext extraParameterContext) {
		CalculationContextInternal calculationContext = map
				.get(this.listResultRule);
		Map<String, Field> resultSet = null;

		if (calculationContext == null) {
			// 表示没有指定规则，包含当前规则为null和没有匹配到数据，用默认
			if (subSharedElement != null && subSharedElement.size() == 1) {
				// 1）
				resultSet = builSingleTable();
			} else {
				// 2)
				resultSet = buildDefaultTable();
			}
		} else if (sourceTrace == null || sourceTrace.sourceKeys.isEmpty()) {
			// 2.1）Database层没有传递定位到本database时计算获得的列名->描点集合的map

			// ListAbstractResultRule rule =
			// calculationContext.ruleChain.getRuleByIndex(calculationContext.index);
			ListAbstractResultRule rule = calculationContext.rule;
			Map<String, Set<Object>> argsFromSQL = getEnumeratedSqlArgsMap(
					calculationContext, rule);
			resultSet = rule.evalElement(argsFromSQL, extraParameterContext);
		} else {
			// 2.2）传递了定位到本database时的列名->描点的集合
			// ListAbstractResultRule rule =
			// calculationContext.ruleChain.getRuleByIndex(calculationContext.index);
			ListAbstractResultRule rule = calculationContext.rule;
			Map<String, Set<Object>> argsFromSQL = getEnumeratedSqlArgsMap(
					calculationContext, rule);
			// 有关系的数据应该优先。putAll会用传入的sourceKeys（对本database有效的描点集）覆盖全部sql信息的计算结果
			Map<String/* 列名 */, Set<Object>/* 得到该结果的描点值名 */> sourceKeys = sourceTrace.sourceKeys;
			for (Entry<String, Set<Object>> entry : sourceKeys.entrySet()) {
				if (argsFromSQL.containsKey(entry.getKey())) {
					argsFromSQL.put(entry.getKey(), entry.getValue());
					log.debug("put entry: " + entry + " to args");
				}
				// else{
				// //从分库来的列中没有分表所需要的数据的时候，不放进去
				// }
			}
			resultSet = rule.evalElement(argsFromSQL, extraParameterContext);
		}
		buildTableNameSet(targetDB, resultSet);
	}

	// //////////////////////////////////////////////////////////////////////////////

	public void calculateTableWithNoDuplicateKey(TargetDB targetDB,
			Map<String/* 列名 */, Object/* 参数值 */> sourceMap,
			ExtraParameterContext extraParameterContext, RuleContext ruleContext) {
		CalculationContextInternal calculationContext = ruleContext.calContextMap
				.get(this.listResultRule);
		Map<String, Field> resultSet = null;
		if (calculationContext == null) {
			if (ruleContext.firstTableCalculate) {
				firstWithOutCalculateContext(ruleContext);
			}
			resultSet = ruleContext.tabSourceWithNoRule;
		} else {
			if (ruleContext.firstTableCalculate) {
				firstWithCalculateContext(sourceMap, calculationContext,
						ruleContext);
				//如果抛NPE,说明ruleContext.dbAndTabWithSameColumn未初始化,
				//这个在开发的时候就应该规避
				if (ruleContext.dbAndTabWithSameColumn.size() == 0) {
					resultSet = evalElement(calculationContext,
							ruleContext.tabArgsMap, extraParameterContext,
							ruleContext);
					ruleContext.tabResultSet=resultSet;
				}
			}
            
			//当库表的分库分表键有交集的时候,我们需要替换掉交集的key,每次计算表
			if (ruleContext.dbAndTabWithSameColumn.size() != 0) {
				// db计算后的每一个sourceKey都可能不相同,所以需要替换
				for (String key : ruleContext.dbAndTabWithSameColumn) {
					Set<Object> set = new HashSet<Object>(1);
					set.add(sourceMap.get(key));
					ruleContext.tabArgsMap.put(key, set);
				}
				
				resultSet = evalElement(calculationContext,
						ruleContext.tabArgsMap, extraParameterContext,
						ruleContext);
			}else{
				//当库表的分库分表键没有交集的时候,对于表的计算,我们只需要计算一次
				resultSet=ruleContext.tabResultSet;
			}
		}

		buildOrAddTable(targetDB, resultSet);
	}

	/**
	 * 1.枚举表参数放到ruleContext中,一次sql操作中,这个表参数是不会有变化的
	 * 2.计算参数交集,将交集列放到ruleContext中,一次sql操作中,这个交集是不会有变化的
	 * 
	 * 以上2个操作在一次sql操作中只执行一次.
	 * 
	 * @param sourceMap
	 * @param calculationContext
	 * @param ruleContext
	 */
	private void firstWithCalculateContext(
			Map<String/* 列名 */, Object/* 参数值 */> sourceMap,
			CalculationContextInternal calculationContext,
			RuleContext ruleContext) {
		Map<String, Set<Object>> argsFromSQL = getEnumeratedSqlArgsMap(
				calculationContext, calculationContext.rule);
		ruleContext.tabArgsMap = argsFromSQL;

		ruleContext.dbAndTabWithSameColumn = new ArrayList<String>(2);
		for (String key : sourceMap.keySet()) {
			if (argsFromSQL.containsKey(key)) {
				ruleContext.dbAndTabWithSameColumn.add(key);
			}
		}
		ruleContext.firstTableCalculate = false;
	}

	/**
	 * 没有表规则的情况下,第一次计算需要得到结果map,并设置到ruleContext中 之后将不必直接使用ruleContext中的map即可
	 * 以上这个操作在一次sql操作中只执行一次.
	 * 
	 * @param ruleContext
	 */
	private void firstWithOutCalculateContext(RuleContext ruleContext) {
		if (subSharedElement != null && subSharedElement.size() == 1) {
			ruleContext.tabSourceWithNoRule = builSingleTable();
		} else {
			ruleContext.tabSourceWithNoRule = buildDefaultTable();
		}
		ruleContext.firstTableCalculate = false;
	}

	/**
	 * 计算某一组参数的表后缀,并且试图将结果放入目标targetDB,如果前面一个 计算结果已经放入了,那么只加sourceKey
	 * 
	 * @param calculationContext
	 * @param enumeratedMap
	 * @param extraParameterContext
	 * @param ruleContext
	 * @return
	 */
	private Map<String/* 结果的值 */, Field> evalElement(
			CalculationContextInternal calculationContext,
			Map<String, Set<Object>> enumeratedMap,
			ExtraParameterContext extraParameterContext, RuleContext ruleContext) {
		Map<String/* 结果的值 */, Field> map;
		if (enumeratedMap.size() == 1) {
			String column = null;
			Set<Object> enumeratedValues = null;
			for (Entry<String, Set<Object>> entry : enumeratedMap.entrySet()) {
				column = entry.getKey();
				enumeratedValues = entry.getValue();
			}

			// 返回值最多也就是与函数的x的个数相对应
			map = new HashMap<String, Field>(enumeratedValues.size());
			for (Object value : enumeratedValues) {
				evalSimple(column, value, calculationContext,
						extraParameterContext, map, ruleContext);
			}
		} else {
			CartesianProductCalculator cartiesianProductCalculator = new CartesianProductCalculator(
					enumeratedMap);
			map = new HashMap<String, Field>(16);
			for (SamplingField samplingField : cartiesianProductCalculator) {
				eval(samplingField, calculationContext, extraParameterContext,
						map, ruleContext);
			}
		}
		return map;
	}

	/**
	 * 最终计算一组参数,返回sourceKey
	 * 
	 * @param samplingField
	 * @param calculationContext
	 * @param extraParameterContext
	 * @param map
	 * @param ruleContext
	 */
	private void eval(SamplingField samplingField,
			CalculationContextInternal calculationContext,
			ExtraParameterContext extraParameterContext,
			Map<String, Field> map, RuleContext ruleContext) {
		ResultAndMappingKey returnAndMappingKey = calculationContext.rule
				.evalueateSamplingField(samplingField, extraParameterContext);
		if (returnAndMappingKey != null) {
			String[] targets = StringUtils.split(returnAndMappingKey.result,
					"\\|");
			List<String> columns = samplingField.getColumns();
			List<Object> values = samplingField.getEnumFields();
			for (String target : targets) {
				Field colMap = map.get(target);
				if (colMap == null) {
					colMap = new Field(columns.size());
					map.put(target, colMap);
				}

				// 只有在需要sourceKey的情况下才去做
				if (ruleContext.needSourceKey) {
					int index = 0;
					for (String column : columns) {
						Object value = values.get(index);
						Set<Object> set = colMap.sourceKeys.get(column);
						if (set == null) {
							set = new HashSet<Object>(1);
							colMap.sourceKeys.put(column, set);
						}
						set.add(value);
						index++;
					}
				}
			}
		}
	}

	/**
	 * 针对单column,单value的规则计算
	 * 
	 * @param column
	 * @param value
	 * @param calculationContext
	 * @param extraParameterContext
	 * @param map
	 * @param ruleContext
	 */
	private void evalSimple(String column, Object value,
			CalculationContextInternal calculationContext,
			ExtraParameterContext extraParameterContext,
			Map<String, Field> map, RuleContext ruleContext) {
		ResultAndMappingKey returnAndMappingKey = calculationContext.rule
				.evalueateSimpleColumAndValue(column, value,
						extraParameterContext);
		if (returnAndMappingKey != null) {
			String[] targets = StringUtils.split(returnAndMappingKey.result,
					"\\|");
			for (String target : targets) {
				Field colMap = map.get(target);
				if (colMap == null) {
					colMap = new Field(1);
					map.put(target, colMap);
				}

				// 只有在需要sourceKey的情况下才去做
				if (ruleContext.needSourceKey) {
					Set<Object> set = colMap.sourceKeys.get(column);
					if (set == null) {
						set = new HashSet<Object>(1);
						colMap.sourceKeys.put(column, set);
					}
					set.add(value);
				}
			}
		}
	}

	/**
	 * 试图将table加入目标targeDB
	 * 
	 * @param targetDB
	 * @param resultSet
	 */
	private void buildOrAddTable(TargetDB targetDB, Map<String, Field> resultSet) {
		for (Entry<String, Field> entry : resultSet.entrySet()) {
			Table table = (Table) subSharedElement.get(entry.getKey());
			if (table == null) {
				throw new IllegalArgumentException(
						"cant find table by target index :" + entry
								+ " current sub tables is " + subSharedElement);
			}
			targetDB.addOneTableWithSameTable(table.getTableName(),
					entry.getValue());
		}
	}

	// ////////////////////////////////////////////////////////////////////
	private void buildTableNameSet(TargetDB targetDB,
			Map<String, Field> resultSet) {
		for (Entry<String, Field> entry : resultSet.entrySet()) {
			Table table = (Table) subSharedElement.get(entry.getKey());
			if (table == null) {
				throw new IllegalArgumentException(
						"cant find table by target index :" + entry
								+ " current sub tables is " + subSharedElement);
			}
			targetDB.addOneTable(table.getTableName(), entry.getValue());
		}
	}

	private Map<String, Set<Object>> getEnumeratedSqlArgsMap(
			CalculationContextInternal calculationContext,
			ListAbstractResultRule rule) {
		if (rule == null) {
			throw new IllegalStateException("should not be here");
		}
		// 强转 需要用到里面的方法
		Map<String, Set<Object>> argsFromSQL = RuleUtils.getSamplingField(
				calculationContext.sqlArgs, rule.getParameters());
		return argsFromSQL;
	}

	private Map<String, Field> buildDefaultTable() {
		Map<String, Field> resultMap;
		resultMap = new HashMap<String, Field>();
		for (String defaultIndex : defaultListResult) {
			resultMap.put(defaultIndex, Field.EMPTY_FIELD);
		}
		return resultMap;
	}

	private Map<String, Field> builSingleTable() {
		Map<String, Field> resultMap = new HashMap<String, Field>(2);
		for (String key : subSharedElement.keySet()) {
			resultMap.put(key, Field.EMPTY_FIELD);
		}
		return resultMap;
	}

	/**
	 * 如果当前节点没有tableMapProvider，那么使用1对多赋给的tableMapProvider.
	 */
	void initTableMapProvider() {
		if (this.tableMapProvider == null) {
			this.tableMapProvider = oneToManyEntry.getTableMapProvider();
		}
	}

	/**
	 * 如果当前节点没有logicTableName,那么使用1对多赋予的logictable.
	 */
	void initLogicTableName() {
		String logicTable = oneToManyEntry.getLogicTableName();
		if (logicTableName == null || logicTableName.length() == 0) {
			this.logicTableName = logicTable;
		}
	}

	/**
	 * 初始化tableRuleChain,如果当前节点的ListRule不为空,RuleChain为空则使用listRule新建一个ruleChain.
	 * 如果listRule为空，RuleChain也为空，那么使用1对多赋予的ruleChain. 如果ruleChain不为空，则初始化之
	 */
	void initTableRuleChain() {
		RuleChain ruleChain = oneToManyEntry.getTableRuleChain();
		// 如果tableRuleList 不为空，并且ruleChain == 空。用tableRuleList
		// 兼容老实现
		if (this.tableRuleList != null) {
			if (listResultRule != null) {
				throw new IllegalArgumentException(
						"有tableRuleList但又指定了ruleChain");
			} else {
				listResultRule = OneToManyEntry.getRuleChain(tableRuleList);
			}
		}
		// 如果老实现未指定给当前database特殊规则，则使用传递后的规则
		if (listResultRule == null) {
			listResultRule = ruleChain;
		}
		// 当前database已经有规则的情况下，初始化当前规则。因为rulechain不会重复初始化，所以只会初始化一次。
		if (ruleChain != null) {
			listResultRule.init();
		} else {
			log.warn("rule chain size is 0");
		}
	}

	protected Map<String, SharedElement> getTableMapByTableMapProvider() {
		TableMapProvider provider = getTableMapProvider();
		provider.setParentID(this.getId());
		provider.setLogicTable(getLogicTableName());
		Map<String, SharedElement> beConstructedMap = provider.getTablesMap();
		return beConstructedMap;
	}

	/**
	 * 将利用便捷方法生成的子元素map设置到当前节点的子元素map引用中。
	 * 
	 * 如果当前子节点的子元素map引用为空则直接设置
	 * 
	 * 如果当前子节点不为空,则表示业务通过spring的方式set了一个现有的map进来
	 * 
	 * 这个map的优先级要比自动生成的map的优先级要高。
	 * 
	 * @param beingConstructedMap
	 */
	protected void putAutoConstructedMapIntoCurrentTagMap(
			Map<String, SharedElement> beingConstructedMap) {
		if (this.subSharedElement == null) {
			subSharedElement = beingConstructedMap;
		} else {
			if (beingConstructedMap != null) {
				// 有自定义的table，同时又有统一规则，那么两个合并，自定义规则覆盖通用统一规则。
				beingConstructedMap.putAll(subSharedElement);
				subSharedElement = beingConstructedMap;
			}
			// else
			// 没有自动生成规则，只有自定义规则，那么什么事情都不做
		}
	}

	/**
	 * 1个databse对应一个tables
	 * 
	 * @param tablesMap
	 */
	public void setTables(Map<String, SharedElement> tablesMap) {
		super.subSharedElement = tablesMap;
	}

	@SuppressWarnings("unchecked")
	public Map<String, SharedElement> getTables() {
		return (Map<String, SharedElement>) super.subSharedElement;
	}

	private Map<String, SharedElement> getTablesMapByStringList(
			List<String> tablesString) {

		List<Table> tables = null;
		tables = new ArrayList<Table>(tablesString.size());

		for (String tabName : tablesString) {
			Table tab = new Table();
			tab.setTableName(tabName);
			tables.add(tab);
		}
		Map<String, SharedElement> returnMap = RuleUtils
				.getSharedElemenetMapBySharedElementList(tables);
		return returnMap;
	}

	/**
	 * 允许业务直接通过string->string的方式来指定数据
	 * 
	 * @param tablesMapString
	 */
	protected void setTablesMapString(
			Map<String/* 表的index */, String/* 表的实际表名 */> tablesMapString) {
		Map<String, SharedElement> beingConstructedMap = new HashMap<String, SharedElement>(
				tablesMapString.size());

		for (Entry<String, String> entry : tablesMapString.entrySet()) {
			Table table = new Table();
			table.setTableName(entry.getValue());
			beingConstructedMap.put(entry.getKey(), table);
		}
		putAutoConstructedMapIntoCurrentTagMap(beingConstructedMap);
	}

	@SuppressWarnings("unchecked")
	public void setTablesMapSimple(Object obj) {
		if (obj instanceof Map) {
			setTablesMapString((Map<String/* 表的index */, String/* 表的实际表名 */>) obj);
		} else if (obj instanceof List) {
			setTablesList((List) obj);
		}
	}

	/**
	 * 允许业务使用table1,table2,table3的方式来指定表名，key=数组下标。 同时也允许业务使用list.add("table1");
	 * list.add("table2");... 的方式来指定表。Key=数组下标
	 * 
	 * @param tablesString
	 */
	protected void setTablesList(List<String> tablesString) {
		// 没有做tablesStringlist的not null检查，因为基于spring
		if (tablesString.size() == 1) {
			String[] tokens = tablesString.get(0).split(",");
			tablesString = new ArrayList<String>();
			tablesString.addAll(Arrays.asList(tokens));
			putAutoConstructedMapIntoCurrentTagMap(getTablesMapByStringList(tablesString));
		} else {
			putAutoConstructedMapIntoCurrentTagMap(getTablesMapByStringList(tablesString));
		}
	}

	public String getDataSourceKey() {
		return dataSourceKey;
	}

	public void setDataSourceKey(String dataSourceKey) {
		this.dataSourceKey = dataSourceKey;
	}

	@Override
	public String toString() {
		return "Database [dataSourceKey=" + dataSourceKey
				+ ", defaultListResult=" + defaultListResult
				+ ", defaultListResultStragety=" + defaultListResultStragety
				+ ", listResultRule=" + listResultRule + ", subSharedElement="
				+ subSharedElement + "]";
	}

	public void setLogicTableName(String logicTable) {
		this.logicTableName = logicTable;
	}

	public void setTableMapProvider(TableMapProvider tableMapProvider) {
		this.tableMapProvider = tableMapProvider;
	}

	public void setTableRule(List<ListAbstractResultRule> tableRule) {
		this.tableRuleList = tableRule;
	}

	public String getLogicTableName() {
		return logicTableName;
	}

	public TableMapProvider getTableMapProvider() {
		return tableMapProvider;
	}

	public RuleChain getRuleChain() {
		return super.listResultRule;
	}

	public void setTableRuleChain(RuleChain ruleChain) {
		super.listResultRule = ruleChain;
	}

	@Override
	public void put(OneToManyEntry oneToManyEntry) {
		this.oneToManyEntry = oneToManyEntry;
	}
}
