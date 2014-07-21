package com.taobao.tddl.rule.ruleengine.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.sequence.Config;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.TabRule;

/**
 * rightConvertMod_3_128
 * 表名一个输入数据的最后3位字符串，按照16进制转10进制转化为数字，然后mod 128。
 * @author shenxun
 *
 */
public class RightConvertModTabProvider extends CommonTableRuleProvider {
	private static final Log log = LogFactory
			.getLog(RightConvertModTabProvider.class);

	public static final String FUNCTION_NAME = "rightConvertMod";

	public static final int DEFAULT_NUM = -1;

	public int getVal(String str, String exceptionStr) {
		int temp = DEFAULT_NUM;
		try {

			temp = Integer.valueOf(str);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("分析rightConvertMod参数发生错误，"
					+ exceptionStr + "为:" + str);
		}
		return temp;
	}
	
	protected Set<String> addAEqComparabToXXXFix(TabRule tab,
			String vTabName, Comparative comparative, int offset, Config config) {
		Set<String> temp = new HashSet<String>(1);
		ModDecIntValueClass modDecInt = getDecimalIntAndMod(tab, comparative.getValue(),null);
		String n = processOne(Integer.valueOf((int) (modDecInt.decIntStart % modDecInt.modVal))
				+ offset, tab, vTabName);
		if (n != null) {
			temp.add(n);
		}
		return temp;
	}
	private ModDecIntValueClass getDecimalIntAndMod(TabRule tab,
			Comparable<?> comparative,Comparable<?> comparativeEnd) {
		// 因为在获得此Provider时已经做过not null检查不用再做啦
		String expression = tab.getExpFunction();
		String[] expToken = expression.split("_");
		String valueStrStart = comparative.toString();
		String valueStrEnd = null;
		if(comparativeEnd != null){
			valueStrEnd = comparativeEnd.toString();
		}
                int expTokenLength=expToken.length;
                int rightLength =0;
                int modVal = 0;
                //从convertNumberSystem进制转化为10进制
                int convertNumberSystem=16;
		if (expTokenLength == 3) {
                    rightLength = getVal(expToken[1], "subLength");
                    modVal = getVal(expToken[2], "mod");
		}else if(expTokenLength == 4){
                    rightLength = getVal(expToken[1], "subLength");
                    convertNumberSystem = getVal(expToken[2], "convertNumberSystem");
                    modVal = getVal(expToken[3], "mod");
                }else{
                    throw new IllegalArgumentException("rightConvertMod 必须有两个或三个参数");
                }
		 
		long decInt = getTargetValueToDecimal(valueStrStart, rightLength,convertNumberSystem);
		long decIntEnd = getTargetValueToDecimal(valueStrEnd, rightLength,convertNumberSystem);
		
		debugOutput(expression, valueStrStart,valueStrEnd, rightLength,modVal);
		ModDecIntValueClass modDecInt=new ModDecIntValueClass();
		modDecInt.decIntStart=decInt;
		modDecInt.modVal=modVal;
		modDecInt.decIntEnd=decIntEnd;
		return modDecInt;
	}
	private static class ModDecIntValueClass{
		public int modVal;
		public long decIntStart;
		public long decIntEnd;
	}
	private void debugOutput(String expression, String valueStr,String valueStrEnd, int rightLength
			,int modVal) {
		if (log.isDebugEnabled()) {
			StringBuilder buffer = new StringBuilder();
			buffer.append("expression = ").append(expression).append(", ");
			buffer.append("rightLength = ").append(rightLength).append(", ");
			buffer.append("value = ").append(valueStr).append(", ");
			buffer.append("End value = ").append(valueStrEnd).append(", ");
			buffer.append("modValue = ").append(modVal).append(", ");
			log.debug(buffer.toString());
		}
	}
	private long getTargetValueToDecimal(String valueStr, int rightLength,int convertNumberSystem) {
		if(valueStr == null){
			return DEFAULT_NUM;
		}
		int valueStrLength =valueStr.length();
		int startIndex=0;
		if(valueStrLength>rightLength){
			startIndex = valueStrLength-rightLength;
		}else{
			startIndex = 0;
		}
		String subString=valueStr.substring(startIndex, valueStrLength);
		long beModedValue=0;
		try {
			beModedValue = Long.valueOf(subString,convertNumberSystem);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("只支持16进制的数据，当前输入的数据为:"+valueStr);
		}
		return beModedValue;
	}

	
	/* (non-Javadoc)
	 * @see com.taobao.tddl.rule.ruleengine.impl.CommonTableRuleProvider#openRangeCheck(java.lang.Comparable, java.lang.Comparable)
	 * String的比较的时候比较特殊因此要独立出来
	 */
	@SuppressWarnings("unchecked")
	protected void openRangeCheck(TabRule tab,Comparable st, Comparable ed) {
		ModDecIntValueClass modDecInt = getDecimalIntAndMod(tab, st,ed);
		if ((modDecInt.decIntStart)>(modDecInt.decIntEnd)) {
			log.info("大于最大值，小于最小值的开区间的情况");
			return ;
		}
	}
	protected List<Object> getXxxfixlist(Comparative start, Comparative end,
			int offset, TabRule tab) {
		List<Object> li = new ArrayList<Object>();
		
		int startType = getType(start);
		int endType = getType(end);
		ModDecIntValueClass modDecInt = getDecimalIntAndMod(tab, start.getValue(),end.getValue());
		long st = modDecInt.decIntStart;
		long ed = modDecInt.decIntEnd;
		int modulus = modDecInt.modVal;
		if (startType == LESS_GREAT) {
			st++;
		}
		if (endType == LESS_OR_EQUAL_GREAT_OR_EQUAL) {
			ed++;
		}
		for (long i = st; i < ed; i++) {
			li.add(Integer.valueOf((int) (i % modulus) + offset));
		}
		return li;
	}
}
