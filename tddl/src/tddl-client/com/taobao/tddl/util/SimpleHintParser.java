package com.taobao.tddl.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.common.lang.StringUtil;
import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.client.databus.StartInfo;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.interact.sqljep.ComparativeAND;
import com.taobao.tddl.interact.sqljep.ComparativeBaseList;
import com.taobao.tddl.interact.sqljep.ComparativeOR;
import com.taobao.tddl.sqlobjecttree.OrderByEle;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.DirectlyRouteCondition;
import com.taobao.tddl.util.IDAndDateCondition.routeCondImp.SimpleCondition;

/**
 * HintParser的修改版本
 * 
 * @author junyu
 * 
 */
public class SimpleHintParser {
	public static Log log = LogFactory.getLog(SimpleHintParser.class);

	public static RouteCondition convertHint2RouteCondition(StartInfo startInfo) {
		String tddlHint = HintParserHelper.extractHint(startInfo.getSql(),
				startInfo.getSqlParam());
		// decode 成RouteCondition
		if (null != tddlHint && !tddlHint.equals("")) {
			try {
				JSONObject jsonObject = JSONObject.fromObject(tddlHint);
				SimpleRouteMethod type = SimpleRouteMethod.valueOf(jsonObject
						.getString("type"));
				if (type == SimpleRouteMethod.direct) {
					return decodeDirect(jsonObject);
				} else if (type == SimpleRouteMethod.condition) {
					return decodeCondition(jsonObject);
				} else {
					log.error("not supported type! the type is:" + type.value());
				}
			} catch (JSONException e) {
				log.error(
						"convert tddl hint to RouteContion faild,check the hint string!",
						e);
				throw e;
			}
			return null;
		} else {
			return null;
		}
	}

	public static RouteCondition decodeDirect(JSONObject jsonObject) {
		DirectlyRouteCondition rc = new DirectlyRouteCondition();
		String tableString = HintParserHelper.containsKvNotBlank(jsonObject,
				"realtabs");
		if (tableString != null) {
			JSONArray jsonTables = JSONArray.fromObject(tableString);
			// 设置table的Set<String>
			if (jsonTables.size() > 0) {
				Set<String> tables = new HashSet<String>(jsonTables.size());
				for (int i = 0; i < jsonTables.size(); i++) {
					tables.add(jsonTables.getString(i));
				}
				rc.setTables(tables);
				//direct只需要在实际表有的前提下解析即可。
				decodeVtab(rc, jsonObject);
			}
		}

		String dbId = HintParserHelper.containsKvNotBlank(jsonObject, "dbid");
		if (dbId == null) {
			throw new RuntimeException("hint contains no property 'dbid'.");
		}

		rc.setDBId(dbId);
		return rc;
	}

	public static RouteCondition decodeCondition(JSONObject jsonObject) {
		SimpleCondition sc = new SimpleCondition();
		decodeVtab(sc, jsonObject);
		decodeSpecifyInfo(sc, jsonObject);

		String paramsStr = HintParserHelper.containsKvNotBlank(jsonObject,
				"params");
		if (paramsStr != null) {
			JSONArray params = JSONArray.fromObject(paramsStr);
			if (params != null) {
				for (int i = 0; i < params.size(); i++) {
					JSONObject o = params.getJSONObject(i);
					JSONArray exprs = o.getJSONArray("expr");
					String paramtype = o.getString("paramtype");

					if (o.has("relation")) {
						String relation = o.getString("relation");
						ComparativeBaseList comList = null;
						if (relation != null && "and".equals(relation)) {
							comList = new ComparativeAND();
						} else if (relation != null && "or".equals(relation)) {
							comList = new ComparativeOR();
						} else {
							throw new RuntimeException(
									"multi param but no relation,the hint is:"
											+ sc.toString());
						}
						String key = null;
						for (int j = 0; j < exprs.size(); j++) {
							Comparative comparative = decodeComparative(
									exprs.getString(j), paramtype);
							comList.addComparative(comparative);

							String temp = getComparativeKey(exprs.getString(j))
									.trim();
							if (null == key) {
								key = temp;
							} else if (!temp.equals(key)) {
								throw new RuntimeException(
										"decodeCondition not support one relation with multi key,the relation is:["
												+ relation + "],expr list is:["
												+ exprs.toString());
							}
						}
						sc.put(key, comList);
					} else {
						if (exprs.size() == 1) {
							String key = getComparativeKey(exprs.getString(0));
							Comparative comparative = decodeComparative(
									exprs.getString(0), paramtype);
							sc.put(key, comparative);
						} else {
							throw new RuntimeException(
									"relation neither 'and' nor 'or',but expr size is not 1");
						}
					}
				}
			}
		}

		return sc;
	}

	public static String getComparativeKey(String expr) {
		int opType = Comparative.getComparisonByCompleteString(expr);
		String operator = Comparative.getComparisonName(opType);
		int index = expr.indexOf(operator);
		return StringUtil.substring(expr, 0, index);
	}

	public static Comparative decodeComparative(String expr, String type) {
		Comparative comparative = null;
		int opType = Comparative.getComparisonByCompleteString(expr);
		String operator = Comparative.getComparisonName(opType);
		int index = expr.indexOf(operator);
		String value = StringUtil.substring(expr, index + operator.length());
		String nType = type.trim();
		if ("i".equals(nType)) {
			comparative = new Comparative(opType, Integer.valueOf(value));
		} else if ("l".equals(nType)) {
			comparative = new Comparative(opType, Long.valueOf(value));
		} else if ("s".equals(nType)) {
			comparative = new Comparative(opType, value);
		} else if ("d".equals(nType)) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				comparative = new Comparative(opType, sdf.parse(value));
			} catch (ParseException e) {
				throw new RuntimeException(
						"only support 'yyyy-MM-dd',now date string is:" + value);
			}
		} else if ("int".equals(nType)) {
			comparative = new Comparative(opType, Integer.valueOf(value));
		} else if ("long".equals(nType)) {
			comparative = new Comparative(opType, Long.valueOf(value));
		} else if ("string".equals(nType)) {
			comparative = new Comparative(opType, value);
		} else if ("date".equals(nType)) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			try {
				comparative = new Comparative(opType, sdf.parse(value));
			} catch (ParseException e) {
				throw new RuntimeException(
						"only support 'yyyy-MM-dd',now date string is:" + value);
			}
		} else {
			throw new RuntimeException(
					"decodeComparative Error notSupport Comparative valueType value: "
							+ type);
		}

		return comparative;
	}
	
	private static void decodeVtab(RouteCondition rc, JSONObject jsonObject)
			throws JSONException {
		String virtualTableName = HintParserHelper.containsKvNotBlank(
				jsonObject, "vtab");
		if (virtualTableName == null) {
			throw new RuntimeException("hint contains no property 'vtab'.");
		}
		rc.setVirtualTableName(virtualTableName);
	}

	private static void decodeSpecifyInfo(SimpleCondition condition,
			JSONObject jsonObject) throws JSONException {
		String skip = HintParserHelper.containsKvNotBlank(jsonObject, "skip");
		if (skip != null) {
			condition.setSkip(Integer.valueOf(skip));
		}

		String max = HintParserHelper.containsKvNotBlank(jsonObject, "max");
		if (max != null) {
			condition.setMax(Integer.valueOf(max));
		}

		String orderby = HintParserHelper.containsKvNotBlank(jsonObject,
				"orderby");
		if (orderby != null) {
			String table = condition.getVirtualTableName();
			if (table == null) {
				throw new RuntimeException("decode the vtab first!code bug.");
			}

			boolean isAsc = false;
			String isAscStr = HintParserHelper.containsKvNotBlank(jsonObject,
					"asc");
			if (isAscStr != null) {
				isAsc = Boolean.valueOf(isAscStr);
			}

			OrderByEle ele = new OrderByEle(table, orderby, isAsc);
			List<OrderByEle> eles = new ArrayList<OrderByEle>(1);
			eles.add(ele);
			condition.setOrderBys(eles);
		}
	}

	protected enum SimpleRouteMethod {
		direct("direct"), condition("condition");

		private String type;

		private SimpleRouteMethod(String type) {
			this.type = type;
		}

		public String value() {
			return this.type;
		}
	}

	public static void main(String[] args) {
		// Map<Integer, ParameterContext> re = new HashMap<Integer,
		// ParameterContext>();
		// ParameterContext pc1 = new ParameterContext();
		// pc1.setArgs(new Object[] { 1, 1 });
		// ParameterContext pc2 = new ParameterContext();
		// pc2.setArgs(new Object[] { 2, 2 });
		// ParameterContext pc3 = new ParameterContext();
		// pc3.setArgs(new Object[] { 3, 3 });
		// re.put(1, pc1);
		// re.put(2, pc2);
		// re.put(3, pc3);
		// String sql =
		// "/*+TDDL({key:?,key2:?})*//*+FULL(tab)*/ select * from tab where b=?";
		// System.out.println(HintParser.extractTDDLHintString(sql, re));
		// StartInfo startInfo = new StartInfo();
		// startInfo.setSql(sql);
		// startInfo.setSqlParam(re);
		// HintParser.removeTddlHintAndParameter(startInfo);
		// System.out.println(startInfo.getSql());
		// System.out.println(re);

		Map<String, Object> map1 = new HashMap<String, Object>();
		map1.put("relation", "and");
		map1.put("paramtype", "int");
		List<String> exprs = new ArrayList<String>();
		exprs.add("pk>4");
		exprs.add("pk<10");
		JSONArray arr = JSONArray.fromObject(exprs);
		map1.put("expr", arr);
		JSONObject object = JSONObject.fromObject(map1);
		List<Object> paramsO = new ArrayList<Object>();
		paramsO.add(object);
		JSONArray a = JSONArray.fromObject(paramsO);
		Map<String, Object> hintmap = new HashMap<String, Object>();
		hintmap.put("params", a);
		hintmap.put("type", "condition");
		;
		hintmap.put("vtab", "vtabxxx");
		JSONObject o = JSONObject.fromObject(hintmap);
		System.out.println(o.toString());
	}
}
