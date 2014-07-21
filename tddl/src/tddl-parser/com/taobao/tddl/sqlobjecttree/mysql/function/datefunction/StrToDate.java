package com.taobao.tddl.sqlobjecttree.mysql.function.datefunction;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.taobao.tddl.common.exception.runtime.NotSupportException;
import com.taobao.tddl.sqlobjecttree.Utils;
import com.taobao.tddl.sqlobjecttree.common.value.OperationBeforTwoArgsFunction;

public class StrToDate extends OperationBeforTwoArgsFunction {
	private static final String[] dateNotSupport=new String[]{
		"%a",
		"%b",
		"%D", 
	 };
	private volatile DateFormat toDateFormat;
	@Override
	public String getFuncName() {
		return "STR_TO_DATE";
	}
	@Override
	public Comparable<?> getVal(List<Object> args) {
			Object obj=null;
			DateFormat df;
			Date returnDate;
			String timeArgument;
			try {
				obj=Utils.getVal(args, arg2);
				String temp=(String)obj;
				df = getDF(temp);
				obj=Utils.getVal(args, arg1);
				timeArgument=(String)obj;
			} catch (ClassCastException e) {
				throw new IllegalArgumentException("错误的转换函数，"+obj+"该参数必须为String");
			}
			try {
				returnDate=df.parse(timeArgument);
			} catch (ParseException e) {
				throw new IllegalArgumentException("错误的时间函数，当前时间函数String为"+timeArgument,e);
			}
			return returnDate;
	}
	protected DateFormat getDF(String source){
		String temp;
		if(toDateFormat==null){
			synchronized (this) {
				if(toDateFormat==null){
					temp=replaceToJavaFormat(source);
					toDateFormat=new SimpleDateFormat(temp);
				}else{
					//do nothing
				}
			}
		}
		return toDateFormat;
	}
	/**
	 * 提供从oracle的format格式转换为java format格式的转换函数
	 * @param source
	 * @return
	 */
	protected static final String replaceToJavaFormat(String source){
		if(source==null){
			return null;
		}else{
			for(String s:dateNotSupport){
				if(source.contains(s))
				{
					throw new NotSupportException("mysql转换函数不支持使用当前函数:"+s+",您输入的是:"+source);
				}
			}
			//year
			source=source.replaceAll("%Y", "yyyy");
			source=source.replaceAll("%y", "yy");
			//month
			source=source.replaceAll("%m", "MM");
			source=source.replaceAll("%c", "MM");
			//date
//			source=source.replaceAll("MON", "MMM");
//			source=source.replaceAll("WW", "www");
			source=source.replaceAll("%d", "dd");
			//hour
			source=source.replaceAll("%H", "HH");
			source=source.replaceAll("%h", "hh");
			source=source.replaceAll("%I", "hh");
			source=source.replaceAll("%p", "aaa");
			
			source=source.replaceAll("%i", "mm");
			source=source.replaceAll("%s", "ss");
			source=source.replaceAll("%S", "ss");
			return source;
		}
		
	}

}
