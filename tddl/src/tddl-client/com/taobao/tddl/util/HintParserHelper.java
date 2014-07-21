package com.taobao.tddl.util;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import com.alibaba.common.lang.StringUtil;
import com.taobao.tddl.client.databus.StartInfo;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;
import com.taobao.tddl.common.util.TStringUtil;

public class HintParserHelper {
	/**
	 * 从sql中解出hint,并且将hint里面的?替换为参数的String形式
	 * @param sql
	 * @param parameterSettings
	 * @return
	 */
	public static String extractHint(String sql,
			Map<Integer, ParameterContext> parameterSettings) {
		String tddlHint = TStringUtil.getBetween(sql, "/*+TDDL(", ")*/");
		if (null == tddlHint || "".endsWith(tddlHint)) {
			return null;
		}
		StringBuffer sb = new StringBuffer();
		int size = tddlHint.length();
		int parameters = 1;
		for (int i = 0; i < size; i++) {
			if (tddlHint.charAt(i) == '?') {
				// TDDLHINT只能设置简单值
				ParameterContext param = parameterSettings.get(parameters);
				if (param.getParameterMethod() == ParameterMethod.setString) {
					sb.append("'");
					sb.append(parameterSettings.get(parameters).getArgs()[1]);
					sb.append("'");
				} else {
					sb.append(parameterSettings.get(parameters).getArgs()[1]);
				}
				
				parameters++;
			} else {
				sb.append(tddlHint.charAt(i));
			}
		}
		return sb.toString();
	}
	
	public static void removeTddlHintAndParameter(StartInfo startInfo) {
		String sql = startInfo.getSql();
		String tddlHint = TStringUtil.getBetween(sql, "/*+TDDL(", ")*/");
		if (null == tddlHint || "".endsWith(tddlHint)) {
			return;
		}
		int size = tddlHint.length();
		int parameters = 0;
		for (int i = 0; i < size; i++) {
			if (tddlHint.charAt(i) == '?') {
				parameters++;
			}
		}

		sql = TStringUtil.removeBetweenWithSplitor(sql, "/*+TDDL(", ")*/");
		startInfo.setSql(sql);

		// 如果parameters为0，说明TDDLhint中没有参数，所以直接返回sql即可
		if (parameters == 0) {
			return;
		}

		Map<Integer, ParameterContext> parametersettings = startInfo
				.getSqlParam();
		// TDDL的hint必需写在SQL语句的最前面，如果和ORACLE hint一起用，
		// 也必需写在hint字符串的最前面，否则参数非常难以处理，也就会出错
		SortedMap<Integer, ParameterContext> tempMap = new TreeMap<Integer, ParameterContext>();
		for (int i = 1; i <= parameters; i++) {
			parametersettings.remove(i);
		}

		tempMap.putAll(parametersettings);
		parametersettings.clear();
		// 这段需要性能优化
		int tempMapSize = tempMap.size();
		for (int i = 1; i <= tempMapSize; i++) {
			Integer ind = tempMap.firstKey();
			ParameterContext pc = tempMap.get(ind);
			pc.getArgs()[0] = i;
			parametersettings.put(i, pc);
			tempMap.remove(ind);
		}
	} 
	
	public static String containsKvNotBlank(
			JSONObject jsonObject, String key) throws JSONException {
		if (!containsKey(jsonObject, key)) {
			return null;
		}
		
		String value = jsonObject.getString(key);
		if (StringUtil.isBlank(value)) {
			return null;
		}
		return value;
	}
	
	@SuppressWarnings("unchecked")
	public static boolean containsKey(JSONObject jsonObject, String key) {
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
}
