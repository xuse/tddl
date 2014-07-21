package com.taobao.tddl.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import com.alibaba.common.lang.StringUtil;
import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.client.RouteCondition.ROUTE_TYPE;
import com.taobao.tddl.common.client.ThreadLocalString;
import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.common.util.TStringUtil;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.interact.sqljep.ComparativeAND;
import com.taobao.tddl.interact.sqljep.ComparativeBaseList;
import com.taobao.tddl.interact.sqljep.ComparativeOR;
import com.taobao.tddl.util.HintParser.RouteMethod;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.AdvanceCondition;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.AdvancedDirectlyRouteCondition;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.DirectlyRouteCondition;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.SimpleCondition;

/**
 * Description: 本工具类主要提供将TDDL使用的THREAD_LOCAL的对象编码成String,和将编码的String转换
 * 成对应的THREAD_LOCAL的对象，方便DPS对THREAD_LOCAL的值进行网络传递。
 * 
 * @author: qihao
 * @version: 1.0 Filename: DBProxyThreadLocalHepler.java Create at: Nov 8, 2010
 *           10:39:15 AM
 * 
 *           Copyright: Copyright (c)2010 Company: TaoBao
 * 
 *           Modification History: Date Author Version Description
 *           ------------------------------------------------------------------
 *           Nov 8, 2010 qihao 1.0 1.0 Version
 */
public class DBProxyThreadLocalHepler {
	/**
	 * THREAD_LOCAL的key对应的类型常量
	 */
	private static final int INTEGER_TYPE = 1;
	private static final int BOOLEAN_TYPE = 2;
	private static final int ROUTE_CONDITION_TYPE = 3;
	/**
	 * RouteCondition 的实际类型常量
	 */
	private static final int ADVANCED_DIRECTLY_CLASS_TYPE = 1;
	private static final int DIRECTLY_CLASS_TYPE = 2;
	private static final int ADVANCE_CONDITION_CLASS_TYPE = 3;
	private static final int SIMPLE_CONDITION_CLASS_TYPE = 4;
	/**
	 * ROUTE_TYPE 的常量(DBProxy的hint)
	 */
	private static final int ROUTE_TYPE_FLUSH_ON_CLOSECONNECTION = 1;
	private static final int ROUTE_TYPE_FLUSH_ON_EXECUTE = 2;

	/**
	 * ROUTE_TYPE 的常量(用户的Sql hint)
	 */
//	private static final String CONNECTION = "connection";
//	private static final String EXECUTE = "execute";

	/**
	 * 将threadLocal里的所有值dump成Map<String, String>
	 * 注意如果生命周期是FLUSH_ON_EXECUTE的话，本次dump 后会清除掉threadLocal中对应的值
	 * 
	 * @return
	 */
	public static Map<String, String> dumpThreadLocal2StrMap() {
		ThreadLocalKey[] keyEnums = ThreadLocalKey.values();
		Map<String, String> threadLocals = new HashMap<String, String>(
				keyEnums.length);
		for (ThreadLocalKey keyEnum : keyEnums) {
			String threadLocalKey = keyEnum.getKey();
			String strData = encodeThreadLocal(threadLocalKey);
			if (StringUtil.isNotBlank(strData)) {
				threadLocals.put(threadLocalKey, strData);
				Object object = ThreadLocalMap.get(threadLocalKey);
				// 如果RouteCondition的ROUTE_TYPE 是FLUSH_ON_EXECUTE
				// dump完成后就清除threadLocal中对应的key
				if (object instanceof RouteCondition) {
					RouteCondition routeCondition = (RouteCondition) object;
					if (ROUTE_TYPE.FLUSH_ON_EXECUTE == routeCondition
							.getRouteType()) {
						ThreadLocalMap.put(threadLocalKey, null);
					}
				}
			}
		}
		return threadLocals;
	}

	/**
	 * 该方法供连接关闭时调用，主要是清除生命周是FLUSH_ON_CLOSECONNECTION 的ThreadLocal对象。
	 */
	public static void cleanOnCloseConnectionThreadLocal() {
		ThreadLocalKey[] keyEnums = ThreadLocalKey.values();
		for (ThreadLocalKey keyEnum : keyEnums) {
			String threadLocalKey = keyEnum.getKey();
			Object object = ThreadLocalMap.get(threadLocalKey);
			if (null != object) {
				// 如果是RouteCondition类型的需要判断下ROUTE_TYPE
				// 如果ROUTE_TYPE是FLUSH_ON_CLOSECONNECTION 清除掉
				if (object instanceof RouteCondition) {
					RouteCondition routeCondition = (RouteCondition) object;
					if (ROUTE_TYPE.FLUSH_ON_CLOSECONNECTION == routeCondition
							.getRouteType()) {
						ThreadLocalMap.put(threadLocalKey, null);
					}
				} else {
					// 不是RouteCondition类型的清除掉
					ThreadLocalMap.put(threadLocalKey, null);
				}
			}
		}
	}

	/**
	 * 向ThreadLocal中设置值，主要是不想将ThreadLocalMap 这个类暴漏到外部系统中去。
	 * 
	 * @param key
	 * @param value
	 */
	public static void setThreadLocal(String key, Object value) {
		ThreadLocalMap.put(key, value);
	}

	/**
	 * 将指定的ThreadLocal的key对应的对象，编码成字符串
	 * 
	 * @param keyThreadLocal的key
	 * @return
	 */
	public static String encodeThreadLocal(String key) {
		String strValue = null;
		ThreadLocalKey enumKey = ThreadLocalKey.getThreadLocalKey(key);
		Object objValue = ThreadLocalMap.get(key);
		// 如果获取不到对应的枚举对象，说明key是不被支持的，或者value是null
		if (null == enumKey || null == objValue) {
			return strValue;
		}
		// 根据key的类型对应关系，将value转换成String
		switch (enumKey.getType()) {
		case INTEGER_TYPE:
			strValue = Integer.toString((Integer) objValue);
			break;
		case BOOLEAN_TYPE:
			strValue = Boolean.toString((Boolean) objValue);
			break;
		case ROUTE_CONDITION_TYPE:
			strValue = encodeRouteCondition(objValue);
		}
		return strValue;
	}

	/**
	 * 根据指定的ThreadLocal的key，和编码过的字符串解码成 编码前的对象
	 * 
	 * @param key
	 *            ThreadLocal的key
	 * @param strData
	 *            对应的经过编码的字符串数据
	 * @return
	 */
	public static Object decodeThreadLocal(String key, String strData) {
		Object object = null;
		ThreadLocalKey enumKey = ThreadLocalKey.getThreadLocalKey(key);
		// 如果获取不到对应的枚举对象，说明key是不被支持的,或者是不合法的字符串
		if (null == enumKey || StringUtil.isBlank(strData)) {
			return object;
		}
		// 根据key的类型对应关系，将value转换成String
		switch (enumKey.getType()) {
		case INTEGER_TYPE:
			object = Integer.valueOf(strData);
			break;
		case BOOLEAN_TYPE:
			object = Boolean.valueOf(strData);
			break;
		case ROUTE_CONDITION_TYPE:
			object = decodeRouteCondition(strData);
		}
		return object;
	}

	/*
	 * 将routeCondition 对象编码成String
	 * 
	 * @param routeCondition
	 * 
	 * @return
	 */
	private static String encodeRouteCondition(Object routeCondition) {
		String strCondition = null;
		JSONObject jsonObject = new JSONObject();
		try {
			if (routeCondition instanceof DirectlyRouteCondition) {
				// 设置公共部分属性
				DirectlyRouteCondition directlyRouteCondition = (DirectlyRouteCondition) routeCondition;
				jsonObject.put("dbId", directlyRouteCondition.getDbRuleID());
				if (ROUTE_TYPE.FLUSH_ON_CLOSECONNECTION.toString().equals(
						directlyRouteCondition.getRouteType().toString())) {
					jsonObject.put("routeType",
							ROUTE_TYPE_FLUSH_ON_CLOSECONNECTION);
				} else if (ROUTE_TYPE.FLUSH_ON_EXECUTE.toString().equals(
						directlyRouteCondition.getRouteType().toString())) {
					jsonObject.put("routeType", ROUTE_TYPE_FLUSH_ON_EXECUTE);
				}
				jsonObject.put("virtualTableName",
						directlyRouteCondition.getVirtualTableName());
				// 设置个别属性
				if (routeCondition.getClass() == DirectlyRouteCondition.class) {
					// 设置RouteCondition的class类型为DirectlyRouteCondition
					jsonObject.put("classType", DIRECTLY_CLASS_TYPE);
					Set<String> tableSet = directlyRouteCondition.getTables();
					if (null != tableSet && !tableSet.isEmpty()) {
						JSONArray jsonTables = new JSONArray();
						for (String table : tableSet) {
							jsonTables.add(table);
						}
						jsonObject.put("tables", jsonTables);
					}
				} else if (routeCondition.getClass() == AdvancedDirectlyRouteCondition.class) {
					// 设置RouteCondition的class类型为AdvancedDirectlyRouteCondition
					jsonObject.put("classType", ADVANCED_DIRECTLY_CLASS_TYPE);
					AdvancedDirectlyRouteCondition advancedDirectlyRouteCondition = (AdvancedDirectlyRouteCondition) routeCondition;
					Map<String, List<Map<String, String>>> shardTableMap = advancedDirectlyRouteCondition
							.getShardTableMap();
					JSONObject jsonShardTableMap = new JSONObject();
					for (Map.Entry<String, List<Map<String, String>>> entry : shardTableMap
							.entrySet()) {
						jsonShardTableMap.put(entry.getKey(), entry.getValue());
					}
					jsonObject.put("shardTableMap", jsonShardTableMap);
				} else {
					throw new RuntimeException(
							"encodeRouteCondition Error not support RouteCondition type: "
									+ routeCondition.getClass().getName());
				}
				strCondition = jsonObject.toString();
			} else if (routeCondition instanceof SimpleCondition) {
				// 处理公共部分属性
				SimpleCondition simpleCondition = (SimpleCondition) routeCondition;
				if (ROUTE_TYPE.FLUSH_ON_CLOSECONNECTION.toString().equals(
						simpleCondition.getRouteType().toString())) {
					jsonObject.put("routeType",
							ROUTE_TYPE_FLUSH_ON_CLOSECONNECTION);
				} else if (ROUTE_TYPE.FLUSH_ON_EXECUTE.toString().equals(
						simpleCondition.getRouteType().toString())) {
					jsonObject.put("routeType", ROUTE_TYPE_FLUSH_ON_EXECUTE);
				}
				jsonObject.put("virtualTableName",
						simpleCondition.getVirtualTableName());
				// 处理parameters
				Map<String, Comparative> parameters = simpleCondition
						.getParameters();
				if (null != parameters && !parameters.isEmpty()) {
					JSONObject jsonParameters = new JSONObject();
					for (Map.Entry<String, Comparative> entry : parameters
							.entrySet()) {
						String paramKey = entry.getKey();
						Comparative praramValue = entry.getValue();
						// 如果参数合法，对Comparative进行编码成String
						if (StringUtil.isNotBlank(paramKey)
								&& null != praramValue) {
							String encdeComparativeStr = encodeComparative(praramValue);
							if (StringUtil.isNotBlank(encdeComparativeStr)) {
								jsonParameters.put(paramKey,
										encdeComparativeStr);
							}
						}
					}
					jsonObject.put("parameters", jsonParameters);
				}
				// 设置classType
				if (routeCondition.getClass() == SimpleCondition.class) {
					jsonObject.put("classType", SIMPLE_CONDITION_CLASS_TYPE);
				} else if (routeCondition.getClass() == AdvanceCondition.class) {
					jsonObject.put("classType", ADVANCE_CONDITION_CLASS_TYPE);
				} else {
					throw new RuntimeException(
							"encodeRouteCondition Error not support RouteCondition type: "
									+ routeCondition.getClass().getName());
				}
				strCondition = jsonObject.toString();
			} else {
				throw new RuntimeException(
						"encodeRouteCondition Error not support RouteCondition type: "
								+ routeCondition.getClass().getName());
			}
		} catch (JSONException e) {
			throw new RuntimeException("encodeRouteCondition Error !", e);
		}
		return strCondition;
	}

	/*
	 * 将comparative转换编码成字符串，无论是单一关系运算， 还是多关系运算
	 * [操作符(and/or)]~关系运算符编号1;值类型1:值1,关系运算符编号2;值类型2:值2........关系运算符编号n;值类型n:值n
	 * 
	 * 注意：1.多关系运算 只支持一层，不支持嵌套，即关系运算值里不支持再次存在多关系的comparative
	 * 例如：a>(b<c)这样是不支持的，只支持a>b and <c and <......这样的格式
	 * 2.关系运算符的值目前只支持Integer,Long,String,Date类型 and~1;i:5,2;l:4
	 * 
	 * @param comparative
	 * 
	 * @return
	 */
	private static String encodeComparative(Comparative comparative) {
		StringBuilder sb = new StringBuilder();
		if (comparative instanceof ComparativeBaseList) {
			// 可能是ComparativeOR或者comparativeAND
			ComparativeBaseList comparativeBaseList = (ComparativeBaseList) comparative;
			List<Comparative> comparativeList = comparativeBaseList.getList();
			if (null != comparativeList && !comparativeList.isEmpty()) {
				if (comparativeBaseList instanceof ComparativeAND) {
					sb.append("and").append("~");
				} else if (comparativeBaseList instanceof ComparativeOR) {
					sb.append("or").append("~");
				} else {
					throw new RuntimeException(
							"encodeComparative not support ComparativeBaseList!");
				}
				for (Comparative comp : comparativeList) {
					// 这里如果是第二层还是ComparativeBaseList类型的直接抛出异常不支持
					if (comp instanceof ComparativeBaseList) {
						throw new RuntimeException(
								"encodeComparative not support second ComparativeBaseList!");
					}
					String strValue = encodeComparativeValue(comp);
					if (null != strValue) {
						sb.append(strValue).append(",");
					}
				}
				// 如果是多个comparative，去掉最后多余的,号
				String strData = sb.toString();
				if (StringUtil.isNotBlank(strData)
						&& strData.lastIndexOf(",") != -1) {
					return StringUtil.substringBeforeLast(strData, ",");
				}
			}
		} else {
			String strValue = encodeComparativeValue(comparative);
			if (null != strValue) {
				sb.append(strValue);
			}
		}
		return sb.length() == 0 ? null : sb.toString();
	}

	/*
	 * 将单一关系运算的Comparative 编码成字符串 格式：关系运算符编号;值类型:值 例如 >=1 是2;i:4
	 * 
	 * @param comp Comparative对象
	 * 
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	private static String encodeComparativeValue(Comparative comp) {
		StringBuilder sb = new StringBuilder();
		Comparable compValue = comp.getValue();
		if (null != compValue) {
			if (compValue instanceof Integer) {
				sb.append(comp.getComparison()).append(";").append("i:")
						.append(((Integer) compValue).intValue());
			} else if (compValue instanceof Long) {
				sb.append(comp.getComparison()).append(";").append("l:")
						.append(((Long) compValue).longValue());
			} else if (compValue instanceof String) {
				sb.append(comp.getComparison()).append(";").append("s:")
						.append(((String) compValue));
			} else if (compValue instanceof Date) {
				sb.append(comp.getComparison()).append(";").append("d:")
						.append(((Date) compValue).getTime());
			} else {
				throw new RuntimeException(
						"encodeComparative not support ComparativeValue!");
			}
		}
		return sb.length() == 0 ? null : sb.toString();
	}

	/*
	 * 将编码成字符串的RouteCondition 解码还原成RouteCondition对象
	 * 
	 * @param strData
	 * 
	 * @return
	 */
	public static RouteCondition decodeRouteCondition(String strData) {
		RouteCondition routeCondition = null;
		try {
			JSONObject jsonObject = JSONObject.fromObject(strData);
			int classType = jsonObject.getInt("classType");
			// 说明是AdvancedDirectlyRouteCondition 或者DirectlyRouteCondition
			if (classType == DIRECTLY_CLASS_TYPE
					|| classType == ADVANCED_DIRECTLY_CLASS_TYPE) {
				routeCondition = decodeNoComparativeRouteCondition(jsonObject);
			} else if (classType == SIMPLE_CONDITION_CLASS_TYPE
					|| classType == ADVANCE_CONDITION_CLASS_TYPE) {
				routeCondition = decodeComparativeRouteCondition(jsonObject);
			}
		} catch (JSONException e) {
			throw new RuntimeException("decodeRouteCondition Error !", e);
		}
		return routeCondition;
	}

	/*
	 * 将json字符串解码成带Comparative的RouteCondition
	 * 
	 * @param jsonObject
	 * 
	 * @return
	 * 
	 * @throws JSONException
	 */
	private static RouteCondition decodeComparativeRouteCondition(
			JSONObject jsonObject) throws JSONException {
		int classType = jsonObject.getInt("classType");
		SimpleCondition comparativeCondition = null;
		if (classType == SIMPLE_CONDITION_CLASS_TYPE) {
			comparativeCondition = new SimpleCondition();
		} else if (classType == ADVANCE_CONDITION_CLASS_TYPE) {
			comparativeCondition = new AdvanceCondition();
		} else {
			throw new RuntimeException(
					"not Sport RouteCondition Type Error ! classType: "
							+ classType);
		}

		decodeVirtualTableName(comparativeCondition, jsonObject);
		decodeRouteType(comparativeCondition, jsonObject);
		decodeParameter(comparativeCondition, jsonObject);
		return comparativeCondition;
	}

	/**
	 * 给用户hint解析使用的ComparativeRouteCondition解码
	 * 
	 * @param jsonObject
	 * @return
	 * @throws JSONException
	 */
	public static RouteCondition decodeComparativeRouteCondition4Outer(
			JSONObject jsonObject) throws JSONException {
		SimpleCondition comparativeCondition = new SimpleCondition();
		decodeParameterForOuter(comparativeCondition, jsonObject);
		decodeVirtualTableName(comparativeCondition, jsonObject);
		decodeSpecifyInfo(comparativeCondition, jsonObject);
		return comparativeCondition;
	}

	private static void decodeSpecifyInfo(SimpleCondition condition,
			JSONObject jsonObject) throws JSONException {
		if (jsonContainsKey(jsonObject, "skip")) {
			Integer skip = Integer.valueOf(jsonObject.getString("skip"));
			if (skip != null) {
				condition.setSkip(skip);
			}
		}

		if (jsonContainsKey(jsonObject, "max")) {
			Integer max = Integer.valueOf(jsonObject.getString("max"));
			if (max != null) {
				condition.setMax(max);
			}
		}
	}

	/**
	 * 需要在其他解析开始之前使用，否则有可能中途改变类型 导致属性丢失
	 * 
	 * @param condition
	 * @param jsonObject
	 * @throws JSONException
	 */
	@SuppressWarnings("unchecked")
	private static void decodeParameter(SimpleCondition condition,
			JSONObject jsonObject) throws JSONException {
		if (jsonContainsKey(jsonObject, "parameters")) {
			// 处理 ComparativeMap
			JSONObject jsonParameters = jsonObject.getJSONObject("parameters");
			if (null != jsonParameters && jsonParameters.size() != 0) {
				Iterator<String> keys = jsonParameters.keys();
				while (keys.hasNext()) {
					String key = keys.next();
					String value = jsonParameters.getString(key);
					if (StringUtil.isNotBlank(value)) {
						// and~1;i:5,2;l:4
						if (StringUtil.contains(value, "~")) {
							// 说明是ComparativeBaseList
							String compStr = StringUtil.substringAfter(value,
									"~");
							String opStr = StringUtil.substringBefore(value,
									"~");
							if (StringUtil.isNotBlank(compStr)
									&& StringUtil.isNotBlank(opStr)) {
								ComparativeBaseList comparativeBaseList = null;
								if ("or".endsWith(opStr)) {
									comparativeBaseList = new ComparativeOR();
								} else if ("and".endsWith(opStr)) {
									comparativeBaseList = new ComparativeAND();
								} else {
									throw new RuntimeException(
											"decodeComparative not support ComparativeBaseList key: "
													+ key + " str:" + value);
								}
								String[] compValues = StringUtil.split(compStr,
										",");
								for (String compValue : compValues) {
									Comparative comparative = decodeComparative(compValue);
									if (null != comparative) {
										comparativeBaseList
												.addComparative(comparative);
									}
								}
								condition.put(key, comparativeBaseList);
							}
						} else {
							// 说明只是Comparative
							Comparative comparative = decodeComparative(value);
							if (null != comparative) {
								condition.put(key, comparative);
							}
						}
					}
				}
			}
		}
	}

	/**
	 * 需要在其他解析开始之前使用，否则有可能中途改变类型 导致属性丢失
	 * 
	 * @param condition
	 * @param jsonObject
	 * @throws JSONException
	 */
	public static void decodeParameterForOuter(SimpleCondition condition,
			JSONObject jsonObject) throws JSONException {
		// modified by jiechen.qzm 确保一定有parameters这个参数
		String parametersString = jsonContainsKeyAndValueNotBlank(jsonObject,
				"parameters");
		if (parametersString == null) {
			throw new RuntimeException(
					"hint contains no property 'parameters'.");
		}

		// 处理 ComparativeMap
		JSONArray jsonParameters = JSONArray.fromObject(parametersString);
		if (jsonParameters.size() != 0) {
			for (int i = 0; i < jsonParameters.size(); i++) {
				String value = jsonParameters.getString(i).toLowerCase();
				if (StringUtil.isNotBlank(value)) {
					// pk=1;i and pk>=2;i
					boolean containsAnd = StringUtil.contains(value, " and ");
					boolean containsOr = StringUtil.contains(value, " or ");
					if (containsAnd || containsOr) {
						ComparativeBaseList comparativeBaseList = null;
						String op;
						if (containsOr) {
							comparativeBaseList = new ComparativeOR();
							op = " or ";
						} else if (containsAnd) {
							comparativeBaseList = new ComparativeAND();
							op = " and ";
						} else {
							throw new RuntimeException(
									"decodeComparative not support ComparativeBaseList value:"
											+ value);
						}
						String[] compValues = TStringUtil.twoPartSplit(value,
								op);
						String key = null;
						for (String compValue : compValues) {
							Comparative comparative = decodeComparativeForOuter(compValue);
							if (null != comparative) {
								comparativeBaseList.addComparative(comparative);
							}
							String temp = getComparativeKey(compValue).trim();
							if (null == key) {
								key = temp;
							} else if (!temp.equals(key)) {
								throw new RuntimeException(
										"decodeComparative not support ComparativeBaseList value:"
												+ value);
							}
						}
						condition.put(key, comparativeBaseList);
					} else {
						// 说明只是Comparative
						String key = getComparativeKey(value);
						Comparative comparative = decodeComparativeForOuter(value);
						if (null != comparative) {
							condition.put(key, comparative);
						}
					}
				}
			}
		}
	}

	public static String getComparativeKey(String compValue) {
		int value = Comparative.getComparisonByCompleteString(compValue);
		String splitor = Comparative.getComparisonName(value);
		int index = compValue.indexOf(splitor);
		return StringUtil.substring(compValue, 0, index);
	}

	// public static void main(String[] args) {
	// try {
	// DBProxyThreadLocalHepler db = new DBProxyThreadLocalHepler();
	// JSONObject jsonObject = new
	// JSONObject("{parameters:[\"pk=1;i and pk>=2;i\",\"id<2;i or id<>3;i\",\"gmt=2011-11-11;d\"]}");
	// SimpleCondition comparativeCondition = new SimpleCondition();
	// db.decodeParameterForOuter(comparativeCondition,jsonObject);
	// } catch (JSONException e) {
	// e.printStackTrace();
	// }
	// }

	/*
	 * 将格式：关系运算符编号;值类型:值 例如 >=1 是2;i:4 的字符串解码成Comparative对象
	 * 
	 * @param compValue
	 * 
	 * @return
	 */
	private static Comparative decodeComparative(String compValue) {
		// 1;i:5
		Comparative comparative = null;
		String strFuction = StringUtil.substringBefore(compValue, ";");
		String[] data = StringUtil.split(
				StringUtil.substringAfter(compValue, ";"), ":");
		if (StringUtil.isNumeric(strFuction) && null != data
				&& data.length == 2) {
			int fuction = Integer.valueOf(StringUtil.substringBefore(compValue,
					";"));
			String dataType = data[0];
			String dataValue = data[1];
			if ("i".equals(dataType)) {
				comparative = new Comparative(fuction,
						Integer.valueOf(dataValue));
			} else if ("l".equals(dataType)) {
				comparative = new Comparative(fuction, Long.valueOf(dataValue));
			} else if ("s".equals(dataType)) {
				comparative = new Comparative(fuction, dataValue);
			} else if ("d".equals(dataType)) {
				comparative = new Comparative(fuction, new Date(
						Long.valueOf(dataValue)));
			} else {
				throw new RuntimeException(
						"decodeComparative Error notSupport Comparative valueType value: "
								+ compValue);
			}
		} else {
			throw new RuntimeException("decodeComparative Error datsStr: "
					+ compValue);
		}
		return comparative;
	}

	private static Comparative decodeComparativeForOuter(String compValue) {
		Comparative comparative = null;
		int value = Comparative.getComparisonByCompleteString(compValue);
		String splitor = Comparative.getComparisonName(value);
		int size = splitor.length();
		int index = compValue.indexOf(splitor);
		String[] valueAndType = StringUtil.split(
				StringUtil.substring(compValue, index + size), ";");

		if (null != valueAndType && valueAndType.length == 2) {
			if ("i".equals(valueAndType[1].trim())) {
				comparative = new Comparative(value,
						Integer.valueOf(valueAndType[0]));
			} else if ("l".equals(valueAndType[1].trim())) {
				comparative = new Comparative(value,
						Long.valueOf(valueAndType[0]));
			} else if ("s".equals(valueAndType[1].trim())) {
				//去除前后单引号
				String v=valueAndType[0].substring(1,valueAndType[0].length()-1);
				comparative = new Comparative(value, v);
			} else if ("d".equals(valueAndType[1].trim())) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				String v=valueAndType[0].substring(1,valueAndType[0].length()-1);
				try {
					comparative = new Comparative(value,
							sdf.parse(v));
				} catch (ParseException e) {
					throw new RuntimeException(
							"only support 'yyyy-MM-dd',now date string is:"
									+ v);
				}
			} else if ("int".equals(valueAndType[1].trim())) {
				comparative = new Comparative(value,
						Integer.valueOf(valueAndType[0]));
			} else if ("long".equals(valueAndType[1].trim())) {
				comparative = new Comparative(value,
						Long.valueOf(valueAndType[0]));
			} else if ("string".equals(valueAndType[1].trim())) {
				String v=valueAndType[0].substring(1,valueAndType[0].length()-1);
				comparative = new Comparative(value,v);
			} else if ("date".equals(valueAndType[1].trim())) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				String v=valueAndType[0].substring(1,valueAndType[0].length()-1);
				try {
					comparative = new Comparative(value,
							sdf.parse(v));
				} catch (ParseException e) {
					throw new RuntimeException(
							"only support 'yyyy-MM-dd',now date string is:"
									+ v);
				}
			} else {
				throw new RuntimeException(
						"decodeComparative Error notSupport Comparative valueType value: "
								+ compValue);
			}
		} else {
			throw new RuntimeException(
					"decodeComparative Error notSupport Comparative valueType value: "
							+ compValue);
		}

		return comparative;
	}

	/*
	 * 将没Comparative对象的RouteCondition编码成String字符串的数据解码 还原成RouteCondition对象
	 * modified by junyu 抽出逻辑复用
	 * 
	 * @param jsonObject
	 * 
	 * @return
	 * 
	 * @throws JSONException
	 */
	private static RouteCondition decodeNoComparativeRouteCondition(
			JSONObject jsonObject) throws JSONException {
		RouteCondition routeCondition;
		/*
		 * 先定义一个DirectlyRouteCondition 主要用于设置
		 * AdvancedDirectlyRouteCondition和DirectlyRouteCondition 的公共属性
		 */
		DirectlyRouteCondition directlyRouteCondition = null;
		int classType = jsonObject.getInt("classType");
		if (DIRECTLY_CLASS_TYPE == classType) {
			// 设置DirectlyRouteCondition 独立的属性
			directlyRouteCondition = new DirectlyRouteCondition();
			if (jsonContainsKey(jsonObject, "tables")) {
				directlyRouteCondition = generateDirectlyRouteCondition(jsonObject);
			}
		} else if (ADVANCED_DIRECTLY_CLASS_TYPE == classType) {
			// 设置AdvancedDirectlyRouteCondition 独立的属性
			AdvancedDirectlyRouteCondition advancedDirectlyRouteCondition = new AdvancedDirectlyRouteCondition();
			// 处理AdvancedDirectlyRouteCondition的 Map<String, List<Map<String,
			// String>>> tableMap属性
			if (jsonContainsKey(jsonObject, "shardTableMap")) {
				advancedDirectlyRouteCondition = generateAdvancedDirectlyRouteCondition(jsonObject);
			}
			// 这样做主要保证directlyRouteCondition这个实例一定不为null;
			directlyRouteCondition = advancedDirectlyRouteCondition;
		} else {
			throw new RuntimeException(
					"not Sport RouteCondition Type Error ! classType: "
							+ classType);
		}

		decodeDbId(directlyRouteCondition, jsonObject);
		decodeVirtualTableName(directlyRouteCondition, jsonObject);
		decodeRouteType(directlyRouteCondition, jsonObject);
		routeCondition = directlyRouteCondition;
		return routeCondition;
	}

	/**
	 * 用户使用的hint解析 modified by jiechen.qzm 类型严格匹配
	 * 
	 * @param jsonObject
	 * @return
	 * @throws JSONException
	 */
	public static RouteCondition decodeNoComparativeRouteCondition4Outer(
			JSONObject jsonObject, RouteMethod type) throws JSONException {
		DirectlyRouteCondition directlyRouteCondition = null;

		if (type.equals(RouteMethod.executeByDB)) {
			directlyRouteCondition = new DirectlyRouteCondition();
			decodeDbId(directlyRouteCondition, jsonObject);
		} else if (type.equals(RouteMethod.executeByDBAndTab)) {
			// 设置DirectlyRouteCondition 独立的属性
			directlyRouteCondition = generateDirectlyRouteCondition(jsonObject);
			decodeDbId(directlyRouteCondition, jsonObject);
			decodeVirtualTableName(directlyRouteCondition, jsonObject);
		} else if (type.equals(RouteMethod.executeByDBAndMutiReplace)) {
			// 处理AdvancedDirectlyRouteCondition的 Map<String, List<Map<String,
			// String>>> tableMap属性
			directlyRouteCondition = generateAdvancedDirectlyRouteCondition(jsonObject);
		}
		return directlyRouteCondition;
	}

	/**
	 * 生成DirectlyRouteCondition
	 * 
	 * @param jsonObject
	 * @return
	 * @throws JSONException
	 * @author junyu
	 */
	private static DirectlyRouteCondition generateDirectlyRouteCondition(
			JSONObject jsonObject) throws JSONException {
		DirectlyRouteCondition directlyRouteCondition = new DirectlyRouteCondition();

		// modified by jiechen.qzm 确保一定有tables这个参数
		String tableString = jsonContainsKeyAndValueNotBlank(jsonObject,
				"tables");
		if (tableString == null) {
			throw new RuntimeException("hint contains no property 'tables'.");
		}

		JSONArray jsonTables = JSONArray.fromObject(tableString);
		// 设置table的Set<String>
		if (jsonTables.size() > 0) {
			Set<String> tables = new HashSet<String>(jsonTables.size());
			for (int i = 0; i < jsonTables.size(); i++) {
				tables.add(jsonTables.getString(i));
			}
			directlyRouteCondition.setTables(tables);
		}
		return directlyRouteCondition;
	}

	/**
	 * 生成AdvancedDirectlyRouteCondition
	 * 
	 * @param jsonObject
	 * @return
	 * @throws JSONException
	 */
	@SuppressWarnings("unchecked")
	private static AdvancedDirectlyRouteCondition generateAdvancedDirectlyRouteCondition(
			JSONObject jsonObject) throws JSONException {
		// modified by jiechen.qzm 确保一定有shardTableMap这个参数
		String tableMapString = jsonContainsKeyAndValueNotBlank(jsonObject,
				"shardTableMap");
		if (tableMapString == null) {
			throw new RuntimeException(
					"hint contains no property 'shardTableMap'.");
		}

		AdvancedDirectlyRouteCondition advancedDirectlyRouteCondition = new AdvancedDirectlyRouteCondition();
		JSONObject jsonShardTableMap = JSONObject.fromObject(tableMapString);
		if (jsonShardTableMap.size() > 0) {
			Iterator<String> shardTableMapKeys = jsonShardTableMap.keys();
			Map<String, List<Map<String, String>>> shardTableMap = new HashMap<String, List<Map<String, String>>>(
					jsonShardTableMap.size());
			while (shardTableMapKeys.hasNext()) {
				String key = shardTableMapKeys.next();
				JSONArray jsonTableList = jsonShardTableMap.getJSONArray(key);
				if (null != jsonTableList && jsonTableList.size() > 0) {
					List<Map<String, String>> tableMapList = new ArrayList<Map<String, String>>(
							jsonTableList.size());
					for (int i = 0; i < jsonTableList.size(); i++) {
						String jsonTabStr = jsonTableList.getString(i);
						if (StringUtil.isNotBlank(jsonTabStr)) {
							JSONObject jsonTableMap = JSONObject.fromObject(jsonTabStr);
							Map<String, String> tableMap = new HashMap<String, String>(
									jsonTableMap.size());
							Iterator<String> tableMapKey = jsonTableMap.keys();
							while (tableMapKey.hasNext()) {
								String tableKey = tableMapKey.next();
								String tableValue = jsonTableMap
										.getString(tableKey);
								if (StringUtil.isNotBlank(tableValue)) {
									tableMap.put(tableKey, tableValue);
								}
							}
							tableMapList.add(tableMap);
						}
					}
					shardTableMap.put(key, tableMapList);
				}
			}
			advancedDirectlyRouteCondition.setShardTableMap(shardTableMap);
		}
		return advancedDirectlyRouteCondition;
	}

	/**
	 * DbId解码
	 * 
	 * @param condition
	 * @param jsonObject
	 * @throws JSONException
	 */
	private static void decodeDbId(DirectlyRouteCondition condition,
			JSONObject jsonObject) throws JSONException {
		String dbId = jsonContainsKeyAndValueNotBlank(jsonObject, "dbId");
		if (dbId == null) {
			throw new RuntimeException("hint contains no property 'dbId'.");
		}
		condition.setDBId(dbId);
	}

	/**
	 * virtualTable解码
	 * 
	 * @param condition
	 * @param jsonObject
	 * @throws JSONException
	 */
	private static void decodeVirtualTableName(RouteCondition condition,
			JSONObject jsonObject) throws JSONException {

		String virtualTableName = jsonContainsKeyAndValueNotBlank(jsonObject,
				"virtualTableName");
		if (virtualTableName == null) {
			throw new RuntimeException(
					"hint contains no property 'virtualTableName'.");
		}
		condition.setVirtualTableName(virtualTableName);
	}

	/**
	 * DBPROXY使用的sql hint解码
	 * 
	 * @param condition
	 * @param jsonObject
	 * @throws JSONException
	 */
	private static void decodeRouteType(RouteCondition condition,
			JSONObject jsonObject) throws JSONException {
		if (jsonContainsKey(jsonObject, "routeType")) {
			int routeType = jsonObject.getInt("routeType");
			if (routeType == ROUTE_TYPE_FLUSH_ON_CLOSECONNECTION) {
				condition.setRouteType(ROUTE_TYPE.FLUSH_ON_CLOSECONNECTION);
			} else if (routeType == ROUTE_TYPE_FLUSH_ON_EXECUTE) {
				condition.setRouteType(ROUTE_TYPE.FLUSH_ON_EXECUTE);
			}
		}
	}

	/**
	 * json中包含某个属性，并且值不为空，返回该值，否则返回null
	 * 
	 * @param jsonObject
	 * @param key
	 * @return
	 * @throws JSONException
	 */
	private static String jsonContainsKeyAndValueNotBlank(
			JSONObject jsonObject, String key) throws JSONException {
		if (!jsonContainsKey(jsonObject, key)) {
			return null;
		}
		String value = jsonObject.getString(key);
		if (StringUtil.isBlank(value)) {
			return null;
		}
		return value;
	}

	/*
	 * 判断JSON是否存在指定key
	 * 
	 * @param jsonObject
	 * 
	 * @param key
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private static boolean jsonContainsKey(JSONObject jsonObject, String key) {
		boolean res = false;
		Iterator<String> it = jsonObject.keys();
		while (it.hasNext()) {
			String itKey = it.next();
			if (StringUtil.equals(itKey, key)) {
				res = true;
				break;
			}
		}
		return res;
	}

	/**
	 * THREAD_LOACL的KEY 的枚举对象，描述了THREAD_LOCAL中KEY与VALUE的 类型对应 Description:
	 * 
	 * @author: qihao
	 * @version: 1.0 Filename: DBProxyThreadLocalHepler.java Create at: Nov 8,
	 *           2010 4:07:32 PM
	 * 
	 *           Copyright: Copyright (c)2010 Company: TaoBao
	 * 
	 *           Modification History: Date Author Version Description
	 *           ----------
	 *           -------------------------------------------------------- Nov 8,
	 *           2010 qihao 1.0 1.0 Version
	 */
	private enum ThreadLocalKey {
		DATASOURCE_INDEX(ThreadLocalString.DATASOURCE_INDEX, INTEGER_TYPE), PARALLEL_EXECUTE(
				ThreadLocalString.PARALLEL_EXECUTE, BOOLEAN_TYPE), IS_EXIST_QUITE(
				ThreadLocalString.IS_EXIST_QUITE, BOOLEAN_TYPE), DB_SELECTOR(
				ThreadLocalString.DB_SELECTOR, ROUTE_CONDITION_TYPE), ROUTE_CONDITION(
				ThreadLocalString.ROUTE_CONDITION, ROUTE_CONDITION_TYPE), RULE_SELECTOR(
				ThreadLocalString.RULE_SELECTOR, ROUTE_CONDITION_TYPE);

		private String key;

		private int type;

		/**
		 * @param key
		 * @param type
		 */
		private ThreadLocalKey(String key, int type) {
			this.key = key;
			this.type = type;
		}

		public static ThreadLocalKey getThreadLocalKey(String key) {
			ThreadLocalKey enumKey = null;
			ThreadLocalKey[] ThreadLocalKeys = ThreadLocalKey.values();
			for (ThreadLocalKey threadLocalKey : ThreadLocalKeys) {
				if (threadLocalKey.getKey().equals(key)) {
					enumKey = threadLocalKey;
					break;
				}
			}
			return enumKey;
		}

		public String getKey() {
			return key;
		}

		public int getType() {
			return type;
		}
	}
}
