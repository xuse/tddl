package com.taobao.tddl.rule.bean;

import groovy.lang.GroovyClassLoader;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.common.lang.StringUtil;
import com.taobao.tddl.rule.ruleengine.entities.abstractentities.SharedElement;
import com.taobao.tddl.rule.ruleengine.entities.convientobjectmaker.TableMapProvider;
import com.taobao.tddl.rule.ruleengine.util.RuleUtils;

/**
 * 提供通过拼装的方式来生成SimpleTableMap的方式
 * 
 * 使用方法:在父类所有方法的基础上，还需要指定fromDateString ,toDateString, calendarType 三个属性，
 * 
 * 如果需要变更日期函数，还需要将日期函数也一并带入，也就是timeStyle.
 * 
 * 就能够在原来的基础上添加一个新的date字段在上面。
 * 
 * 
 * TODO: 重构这部分代码，SimpleTableMapProvider和此代码有交集，使用此代码替换
 * 
 * @author shenxun
 * 
 */
public class SimpleDateTableMapProvider implements TableMapProvider {
	public enum TYPE {
		NORMAL, CUSTOM
	}

	public static final String NORMAL_TAOBAO_TYPE = "NORMAL";

	public static final String DEFAULT_PADDING = "_";

	protected static final int DEFAULT_INT = -1;

	public static final int DEFAULT_TABLES_NUM_FOR_EACH_DB = -1;

	private String type = NORMAL_TAOBAO_TYPE;
	/**
	 * table[padding]suffix 默认的padding是_
	 */
	private String padding;
	/**
	 * width 宽度
	 */
	private int width = DEFAULT_INT;
	/**
	 * 分表标识因子。就是说最开始打头的是什么表，如果不指定则默认是逻辑表名
	 */
	private String tableFactor;
	/**
	 * 逻辑表名
	 */
	private String logicTable;
	/**
	 * 每次自增数
	 */
	private int step = 1;

	/**
	 * 每个数据库的表的个数，如果指定了这项则每个表内的个数就为指定多个
	 */
	private int tablesNumberForEachDatabases = DEFAULT_TABLES_NUM_FOR_EACH_DB;
	/**
	 * database id
	 */
	private String parentID;
	/**
	 * 每个数据库的表的个数有多少个 >= ?
	 */
	private int from = DEFAULT_INT;
	/**
	 * <= ?
	 */
	private int to = DEFAULT_INT;

	private boolean doesNotSetTablesNumberForEachDatabases() {
		return tablesNumberForEachDatabases == -1;
	}

	public int getFrom() {
		return from;
	}

	public String getPadding() {
		return padding;
	}

	public String getParentID() {
		return parentID;
	}

	public int getStep() {
		return step;
	}

	private static final Log logger = LogFactory
			.getLog(SimpleDateTableMapProvider.class);
	/**
	 * simple date format 格式
	 */
	private String timeStyle = "yyMM";
	/**
	 * 设定输入参数的输入格式
	 */
	private final static String inputTimeStyle = "yyyyMM";

	/**
	 * 从哪开始
	 */
	private String fromDateString;

	/**
	 * 到哪结束
	 */
	private String toDateString;

	/**
	 * 自增计算日期用的calendar格式
	 */
	private CALENDAR_TYPE calendarType = CALENDAR_TYPE.MONTH;

	private boolean isOnlyDateSharding = false;

	public enum CALENDAR_TYPE {
		DATE(Calendar.DATE), MONTH(Calendar.MONTH), YEAR(Calendar.YEAR), HOUR(
				Calendar.HOUR), QUARTER(Calendar.MONTH), HALF_A_YEAR(
				Calendar.MONTH),WEEK_OF_MONTH(Calendar.WEEK_OF_MONTH)
				,WEEK_OF_YEAR(Calendar.WEEK_OF_YEAR),GROOVY(Calendar.MONTH);

		private int i;

		public int value() {
			return this.i;
		}

		private CALENDAR_TYPE(int i) {
			this.i = i;
		}

	}
	
	private String groovyScript;
	

	public static void main(String[] args) {
		System.out.println(CALENDAR_TYPE.DATE);
		System.out.println(CALENDAR_TYPE.DATE.value());

		System.out.println(CALENDAR_TYPE.QUARTER);
		System.out.println(CALENDAR_TYPE.QUARTER.value());

		System.out.println(CALENDAR_TYPE.DATE == CALENDAR_TYPE.QUARTER);
	}

	SimpleDateFormat simpleDateFormat;

	public Map<String, SharedElement> getTablesMap() {

		if (tableFactor == null && logicTable != null) {
			tableFactor = logicTable;
		}
		if (tableFactor == null) {
			throw new IllegalArgumentException("没有表名生成因子");
		}

		List<String> dateArgsList = getDateStringList();
	
		logger.warn(dateArgsList);
		TYPE typeEnum = TYPE.valueOf(type);

		makeRealTableNameTaobaoLike(typeEnum);

		// 如果没有设置每个数据库表的个数，那么表示所有表都用统一的表名，类似(tab_0~tab_3)*16个数据库=64张表
		if (doesNotSetTablesNumberForEachDatabases()) {
			return getSuffixList(from, to, width, step, tableFactor, padding,
					dateArgsList);
		} else {
			// 如果设置了每个数据库表的个数，那么表示所有表用不同的表名，类似(tab_0~tab63),分布在16个数据库上
			int multiple = 0;
			try {
				multiple = Integer.valueOf(parentID);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(
						"使用simpleTableMapProvider并且指定了tablesNumberForEachDatabase参数，database的index值必须是个integer数字"
								+ "当前database的index是:" + parentID);
			}
			int start = tablesNumberForEachDatabases * multiple;
			// 因为尾缀的范围是到<=的数字，所以要-1.
			int end = start + tablesNumberForEachDatabases - 1;
			// 设置当前database里面的表名
			return getSuffixList(start, end, width, step, tableFactor, padding,
					dateArgsList);
		}

	}

	/**
	 * 用于处理时间字串的拼接问题
	 * 
	 * @author shenxun
	 * 
	 */
	private static interface DateStringListHandler {
		List<String> getDateStringList(String tableFactor, String logicTable,
				String timeStyle, String fromDateString,
				String toDateString, CALENDAR_TYPE calendarType,
				String groovyScript);
	}

	
	public static abstract class CustomStringListHandlerCommon implements
			DateStringListHandler {
		// 加入了季度以后 一般来说有两种用法，一种是加年份，4位 2位，一种是不加年份。
		public List<String> getDateStringList(String tableFactor,
				String logicTable, String timeStyle,
				String fromDateString, String toDateString,
				CALENDAR_TYPE calendarType, String groovyScript) {
			List<String> dateArgsList = null;
			logger.warn("init data table map");
			// 在初始化的时候生成TableMap ;
			if (tableFactor == null && logicTable != null) {
				tableFactor = logicTable;
			}
			if (tableFactor == null) {
				throw new IllegalArgumentException("没有表名生成因子");
			}
			if (timeStyle == null) {
				throw new IllegalArgumentException("时间错误");
			}
			dateArgsList = new ArrayList<String>();
			SimpleDateFormat inputDateFormat = new SimpleDateFormat(inputTimeStyle);
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat(timeStyle);
			Date fromDate = null;
			Date toDate = null;
			Calendar cal = Calendar.getInstance();

			Calendar calDis = Calendar.getInstance();
			try {
				fromDate = inputDateFormat.parse(fromDateString);
				toDate = inputDateFormat.parse(toDateString);

				cal.setTime(fromDate);
				calDis.setTime(toDate);
				logger.warn("from date :" + fromDate);
				logger.warn("to date :" + toDate);
			} catch (ParseException e) {
				throw new IllegalArgumentException("from : " + fromDateString
						+ ". to : " + toDateString + " inputDateFormat : "
						+ inputDateFormat);
			} catch (NullPointerException e) {
				throw new IllegalArgumentException("from : " + fromDateString
						+ ". to : " + toDateString + " inputDateFormat : "
						+ inputDateFormat);
			}

			// 获取年份从哪开始，到哪结束
			
			int start = timeStyle.indexOf("y");
			start = (start == -1)?0:start;
			int end = timeStyle.lastIndexOf("y") + 1;
			// first element
			Date date = cal.getTime();
			String dateStr = buildQuarterString(simpleDateFormat, calendarType,
					cal, start, end, date, getDuration());
			dateArgsList.add(dateStr);

			while (true) {
				cal.add(calendarType.value(), 3);

				if (cal.compareTo(calDis) > 0) {
					date = calDis.getTime();
					dateStr = buildQuarterString(simpleDateFormat,
							calendarType, calDis, start, end, date,
							getDuration());
					dateArgsList.add(dateStr);
					break;
				} else {
					date = cal.getTime();
					dateStr = buildQuarterString(simpleDateFormat,
							calendarType, cal, start, end, date, getDuration());
					dateArgsList.add(dateStr);
				}

			}
			logger.warn(dateArgsList + "inited");
			return dateArgsList;
		}

		protected abstract int getDuration();
	}

	public static class QuarterStringListHandler extends
			CustomStringListHandlerCommon {

		@Override
		protected int getDuration() {
			return 3;
		}

	}

	public static class HalfayearStringListHandler extends
			CustomStringListHandlerCommon {

		@Override
		protected int getDuration() {
			return 6;
		}

	}

	/**
	 * 能想到的有三类 yyyyQ yyQ Q Q是quarter的数字表示，从1开始到4
	 * 
	 * @param simpleDateFormat
	 * @param calendarType
	 * @param cal
	 * @param start
	 * @param end
	 * @param date
	 * @return
	 */
	private static String buildQuarterString(SimpleDateFormat simpleDateFormat,
			CALENDAR_TYPE calendarType, Calendar cal, int start, int end,
			Date date, int duration) {
		String dateStr = simpleDateFormat.format(date);
		// 将year截取出来放到新builder里
		StringBuilder dateStringBuilder = new StringBuilder();
		dateStringBuilder.append(dateStr.subSequence(start, end));
		int dateField = cal.get(calendarType.value());
		
		int value = dateField / duration + 1;
		dateStringBuilder.append(RuleUtils.placeHolder(2, value));
		dateStr = dateStringBuilder.toString();
		return dateStr;
	}

	/**
	 * 默认的处理器
	 * 
	 * @author shenxun
	 * 
	 */
	public static class DefaultStringListHandler implements
			DateStringListHandler {

		public List<String> getDateStringList(String tableFactor,
				String logicTable, String timeStyle,
				String fromDateString, String toDateString,
				CALENDAR_TYPE calendarType, String groovyScript) {
			
			// 在初始化的时候生成TableMap ;
			if (tableFactor == null && logicTable != null) {
				tableFactor = logicTable;
			}
			if (tableFactor == null) {
				throw new IllegalArgumentException("没有表名生成因子");
			}
			if (timeStyle == null) {
				throw new IllegalArgumentException("时间错误");
			}
			SimpleDateFormat inputDateFormat = new SimpleDateFormat(inputTimeStyle);
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat(timeStyle);
			Date fromDate = null;
			Date toDate = null;
			Calendar cal = Calendar.getInstance();

			Calendar calDis = Calendar.getInstance();
			try {
				fromDate = inputDateFormat.parse(fromDateString);
				toDate = inputDateFormat.parse(toDateString);

				cal.setTime(fromDate);
				calDis.setTime(toDate);
				logger.warn("from date :" + fromDate);
				logger.warn("to date :" + toDate);
			} catch (ParseException e) {
				throw new IllegalArgumentException("from : " + fromDateString
						+ ". to : " + toDateString + " inputDateFormat : "
						+ inputTimeStyle);
			} catch (NullPointerException e) {
				throw new IllegalArgumentException("from : " + fromDateString
						+ ". to : " + toDateString + " inputDateFormat : "
						+ inputTimeStyle);
			}
			List<String> dateArgsList = buildArgsList(calendarType,
					simpleDateFormat, cal, calDis);
			return dateArgsList;
		}

		protected List<String> buildArgsList(CALENDAR_TYPE calendarType,
				SimpleDateFormat simpleDateFormat, Calendar cal, Calendar calDis) {
			List<String> dateArgsList = new ArrayList<String>();
			dateArgsList.add(simpleDateFormat.format(cal.getTime()));
			while (true) {
				cal.add(calendarType.value(), 1);
				if (cal.compareTo(calDis) > 0) {
					dateArgsList.add(simpleDateFormat.format(calDis.getTime()));
					break;
				} else {
					dateArgsList.add(simpleDateFormat.format(cal.getTime()));
				}

			}
			return dateArgsList;
		}

	}

	/**
	 * 获取dateStringList
	 * 
	 * 默认的格式是yyyyMMdd 全部是数字。会用于表名拼装，同时也会被用于添加到分表的key中去。
	 * 
	 * 期望的用法有两种。 1. 默认配置情况下，这个yyyyMMdd可以直接转换为数字 2.
	 * 如果业务指定了自己的格式，那么需要在业务的key中也拼出对应的模式即可。
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private List<String> getDateStringList() {
		DateStringListHandler handler;
		if (CALENDAR_TYPE.QUARTER == calendarType) {
			handler = new QuarterStringListHandler();
		} else if (CALENDAR_TYPE.HALF_A_YEAR == calendarType) {
			handler = new HalfayearStringListHandler();
		} else if (CALENDAR_TYPE.GROOVY == calendarType) {
			GroovyClassLoader loader = new GroovyClassLoader(SimpleDateTableMapProvider.class.getClassLoader());
			Class<DateStringListHandler> handlerClass = loader.parseClass(groovyScript);
			try {
				handler = handlerClass.newInstance();
			} catch (InstantiationException e) {
				throw new IllegalArgumentException("groovy script new instance error , script is "
						+groovyScript,e);
			} catch (IllegalAccessException e) {
				throw new IllegalArgumentException("groovy script new instance error , script is "
						+groovyScript,e);
			}
		} else {
			// 其他的非季度或半年的情況
			handler = new DefaultStringListHandler();
		}
		List<String> dateArgsList = handler.getDateStringList(tableFactor,
				logicTable, timeStyle, fromDateString, toDateString,
				calendarType, groovyScript);
		return dateArgsList;
	}

	/**
	 * 如果是正常的情况下，那么应该是表名因子+"_"+最长数字位数那样长的尾缀，类似 tab_001~tab_100
	 */
	protected void makeRealTableNameTaobaoLike(TYPE typeEnum) {
		if (typeEnum == TYPE.CUSTOM) {
			// do nothing
		} else {
			if (padding == null)
				padding = DEFAULT_PADDING;
			if (to != DEFAULT_INT && width == DEFAULT_INT) {
				width = String.valueOf(to).length();
			}
		}

	}

	protected Map<String, SharedElement> getSuffixList(int from, int to,
			int width, int step, String tableFactor, String padding,
			List<String> dateArgsList) {
		Map<String, SharedElement> map = null;
		if (!isOnlyDateSharding) {
			map = buildTableMapWithTableSharding(from, to, width, step,
					tableFactor, padding, dateArgsList);
		} else {
			map = buildTableMapWithoutTableSharding(from, to, width, step,
					tableFactor, padding, dateArgsList);
		}

		logger.info("table map :" + map + "inited!");
		return map;
	}

	private Map<String, SharedElement> buildTableMapWithoutTableSharding(
			int from, int to, int width, int step, String tableFactor,
			String padding, List<String> dateArgsList) {
		Map<String, SharedElement> tableMap = new HashMap<String, SharedElement>(
				1);
		StringBuilder sb = new StringBuilder();
		sb.append(tableFactor);
		sb.append(padding);
		String tableProfix = sb.toString();

		for (String time : dateArgsList) {
			String key = time;
			StringBuilder singleTableBuilder = new StringBuilder(tableProfix);
			singleTableBuilder.append(time);
			tableMap.put(key, new Table(singleTableBuilder.toString()));
		}
		
		return tableMap;
	}

	private Map<String, SharedElement> buildTableMapWithTableSharding(int from,
			int to, int width, int step, String tableFactor, String padding,
			List<String> dateArgsList) {
		if (from == DEFAULT_INT || to == DEFAULT_INT) {
			throw new IllegalArgumentException("from,to must be spec!");
		}
		int length = to - from + 1;
		Map<String, SharedElement> tableMap = new HashMap<String, SharedElement>(
				length);
		StringBuilder sb = new StringBuilder();
		sb.append(tableFactor);
		sb.append(padding);
		String tableProfix = sb.toString();
		int tableForEachDB = to - from + 1 ;
		for (int i = from; i <= to; i = i + step) {
			//key 在多个databases中必须是一致的。
			int index = i % tableForEachDB;
			//用于添加在月份后成为表尾缀
			String keySuffix = null;
			//用于添加在表后的表尾缀，tableSuffix的int值 mod 当前库的表的个数，就等于keySuffix的值。
			String tableSuffix = null;
			if (width != DEFAULT_INT) {
				tableSuffix = RuleUtils.placeHolder(width, i);
				keySuffix = RuleUtils.placeHolder(width, index);
			} else {
				// 如果不显式指定width，或指定为-1，则不补零，直接以数值为后缀
				tableSuffix = String.valueOf(i);
				keySuffix = String.valueOf(index);
			}
			for (String time : dateArgsList) {
				String key = time + keySuffix;
				StringBuilder singleTableBuilder = new StringBuilder(
						tableProfix);
				singleTableBuilder.append(time);
				singleTableBuilder.append(padding);
				singleTableBuilder.append(tableSuffix);
				tableMap.put(key, new Table(singleTableBuilder.toString()));
			}
		}
		return tableMap;
	}

	public String getTimeStyle() {
		return timeStyle;
	}

	public void setTimeStyle(String timeStyle) {
		if (timeStyle != null) {
			this.timeStyle = timeStyle;
		}
	}

	public String getFromDateString() {
		return fromDateString;
	}

	public void setFromDateString(String fromDateString) {
		if (fromDateString != null) {
			this.fromDateString = fromDateString;
		}
	}

	public String getToDateString() {
		return toDateString;
	}

	public void setToDateString(String toDateString) {
		if (toDateString != null) {
			this.toDateString = toDateString;
		}
	}

	public CALENDAR_TYPE getCalendarType() {
		return calendarType;
	}

	public void setCalendarType(CALENDAR_TYPE calendarType) {
		this.calendarType = calendarType;
	}

	public void setCalendarTypeString(String calendarTypeString) {
		if (calendarTypeString != null) {
			calendarTypeString = StringUtil.toUpperCase(calendarTypeString);
			calendarType = CALENDAR_TYPE.valueOf(calendarTypeString);
		}

	}

	public void setFrom(int from) {
		this.from = from;
	}

	public void setLogicTable(String logicTable) {
		this.logicTable = logicTable;
	}

	public void setPadding(String padding) {
		this.padding = padding;
	}

	public void setParentID(String parentID) {
		this.parentID = parentID;
	}

	public void setStep(int step) {
		this.step = step;
	}

	public void setTableFactor(String tableFactor) {
		this.tableFactor = tableFactor;
	}

	public void setTablesNumberForEachDatabases(int tablesNumberForEachDatabases) {
		this.tablesNumberForEachDatabases = tablesNumberForEachDatabases;
	}

	public void setTo(int to) {
		this.to = to;
	}

	public void setType(String type) {
		this.type = type;
	}


	public boolean isOnlyDateSharding() {
		return isOnlyDateSharding;
	}

	public void setOnlyDateSharding(boolean isOnlyDateSharding) {
		this.isOnlyDateSharding = isOnlyDateSharding;
	}

	public void setWidth(int width) {
		if (width > 8) {
			throw new IllegalArgumentException("占位符不能超过8位");
		}
		// 表后缀占位符可以为0, 此时不补零
		if (width < 0) {
			throw new IllegalArgumentException("占位符不能为负值");
		}
		this.width = width;
	}
	

	public String getGroovyScript() {
		return groovyScript;
	}

	public void setGroovyScript(String groovyScript) {
		this.groovyScript = groovyScript;
	}

	
	public String getInputTimeStyle() {
		return inputTimeStyle;
	}

	

	@Override
	public String toString() {
		return "SimpleDateTableMapProvider [calendarType=" + calendarType
				+ ", from=" + from + ", fromDateString=" + fromDateString
				+ ", isOnlyDateSharding=" + isOnlyDateSharding
				+ ", logicTable=" + logicTable + ", padding=" + padding
				+ ", parentID=" + parentID + ", simpleDateFormat="
				+ simpleDateFormat + ", step=" + step + ", tableFactor="
				+ tableFactor + ", tablesNumberForEachDatabases="
				+ tablesNumberForEachDatabases + ", timeStyle=" + timeStyle
				+ ", to=" + to + ", toDateString=" + toDateString + ", type="
				+ type + ", width=" + width + "]";
	}
	
}
