//Copyright(c) Taobao.com
package com.taobao.tddl.util;

import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.client.databus.StartInfo;
import com.taobao.tddl.common.jdbc.ParameterContext;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2010-12-24ÉÏÎç10:32:16
 */
public class HintParser {
	public static Log log = LogFactory.getLog(HintParser.class);

	public static RouteCondition convertHint2RouteCondition(StartInfo startInfo) {
		String tddlHint = HintParserHelper.extractHint(startInfo.getSql(),
				startInfo.getSqlParam());
		// decode ³ÉRouteCondition
		if (null != tddlHint && !tddlHint.equals("")) {
			try {
				JSONObject jsonObject = JSONObject.fromObject(tddlHint);
				RouteMethod type = RouteMethod.valueOf(jsonObject
						.getString("type"));
				if (type.equals(RouteMethod.executeByDB)
						|| type.equals(RouteMethod.executeByDBAndTab)
						|| type.equals(RouteMethod.executeByDBAndMutiReplace)) {
					return DBProxyThreadLocalHepler
							.decodeNoComparativeRouteCondition4Outer(
									jsonObject, type);
				} else if (type.equals(RouteMethod.executeByCondition)
				/* || type.equals(RouteMethod.executeByAdvancedCondition) */) {
					return DBProxyThreadLocalHepler
							.decodeComparativeRouteCondition4Outer(jsonObject);
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

	protected enum RouteMethod {
		executeByDBAndTab("executeByDBAndTab"), executeByDBAndMutiReplace(
				"executeByDBAndMutiReplace"), executeByDB("executeByDB"), executeByRule(
				"executeByRule"), executeByCondition("executeByCondition"), executeByAdvancedCondition(
				"executeByAdvancedCondition");

		private String type;

		private RouteMethod(String type) {
			this.type = type;
		}

		public String value() {
			return this.type;
		}
	}

	public static void main(String[] args) {
		Map<Integer, ParameterContext> re = new HashMap<Integer, ParameterContext>();
		ParameterContext pc1 = new ParameterContext();
		pc1.setArgs(new Object[] { 1, 1 });
		ParameterContext pc2 = new ParameterContext();
		pc2.setArgs(new Object[] { 2, 2 });
		ParameterContext pc3 = new ParameterContext();
		pc3.setArgs(new Object[] { 3, 3 });
		re.put(1, pc1);
		re.put(2, pc2);
		re.put(3, pc3);
		String sql = "/*+TDDL({key:?,key2:?})*//*+FULL(tab)*/ select * from tab where b=?";
		System.out.println(HintParserHelper.extractHint(sql, re));
		StartInfo startInfo = new StartInfo();
		startInfo.setSql(sql);
		startInfo.setSqlParam(re);
		HintParserHelper.removeTddlHintAndParameter(startInfo);
		System.out.println(startInfo.getSql());
		System.out.println(re);
	}
}
