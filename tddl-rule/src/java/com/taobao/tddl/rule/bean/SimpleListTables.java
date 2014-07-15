//package com.taobao.tddl.rule.bean;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import com.taobao.tddl.rule.ruleengine.util.RuleUtils;
//
///**
// * 提供一种简易的方式，让业务方可以通过简单的几个参数动态生成所有的表，不用一个一个
// * 去指定
// * @author shenxun
// *
// */
//public class SimpleListTables extends Tables {
//	
//	public enum TYPE {
//		NORMAL, CUSTOM
//	}
//
//	
//	public static final String NORMAL_TAOBAO_TYPE = "NORMAL";
//
//	public static final String DEFAULT_PADDING = "_";
//	
//	private static final int DEFAULT_STEP = 1;
//
//	private String type = NORMAL_TAOBAO_TYPE;
//	private String padding;
//	private int width;
//	private String tableFactor;
//	private int step = DEFAULT_STEP;
//
//	/**
//	 * 记录当前已经到达的index值是多少
//	 */
//	private int currentIndex = 0;
//	/**
//	 * 每个数据库的表的个数有多少个
//	 */
//	private int tablesNumberInEachDatabases = -1;
//	private int from;
//	private int to;
//
//	public int getWidth() {
//		return width;
//	}
//
//	public void setWidth(int width) {
//		if (width > 8) {
//			throw new IllegalArgumentException("占位符不能超过8位");
//		}
//		if (width <= 0) {
//			throw new IllegalArgumentException("占位符不能为负值或为0");
//		}
//		this.width = width;
//
//	}
//
//	/* (non-Javadoc)
//	 * @see com.taobao.tddl.rule.ruleengine.entities.abstractentities.ListSharedElement#init()
//	 */
//	public boolean init() {
//		boolean isChanged = false;
//		TYPE typeEnum = TYPE.valueOf(type);
//		switch (typeEnum) {
//		case NORMAL:
//			// 如果是正常的情况下，那么应该是表名因子+"_"+最长数字位数那样长的尾缀
//			// 类似 tab_001~tab_100
//			padding = DEFAULT_PADDING;
//			String tempStr = String.valueOf(to);
//			int endNumberLength = tempStr.length();
//			width = endNumberLength;
//			break;
//		default:
//			break;
//		}
//		List<String> tableNames;
//		//如果没有设置每个数据库表的个数，那么表示所有表都用统一的表名，类似(tab_0~tab_3)*16个数据库=64张表
//		
//		if(tablesNumberInEachDatabases == -1){
//			tableNames = getSuffixList(from, to, width, step,
//					tableFactor, padding);
//		}else{
//			//如果设置了每个数据库表的个数，那么表示所有表用不同的表名，类似(tab_0~tab63)
//			int start = currentIndex;
//			//因为尾缀的范围是到<=的数字，所以要-1.
//			int end =currentIndex+tablesNumberInEachDatabases-1;
//			//当前index 应该是当前取到元素尾缀+1
//			currentIndex = end+1;
//			tableNames = getSuffixList(start, end, width, step, tableFactor, padding);
//		}
//		List<Table> tables = null;
//		tables = new ArrayList<Table>(tableNames.size());
//		for (String tableName : tableNames) {
//			Table tab = new Table();
//			tab.setTableName(tableName);
//			tables.add(tab);
//		}
//		setTablesList(tables);
//		isChanged =super.init();
//		return isChanged;
//	}
//
//
//
//	protected List<String> getSuffixList(int from, int to, int width, int step,
//			String tableFactor, String padding) {
//		int length = to - from + 1;
//		List<String> tableList = new ArrayList<String>(length);
//		StringBuilder sb = new StringBuilder();
//		sb.append(tableFactor);
//		sb.append(padding);
//
//		for (int i = from; i <= to; i = i + step) {
//			StringBuilder singleTableBuilder = new StringBuilder(sb.toString());
//			String suffix = RuleUtils.placeHolder(width, i);
//			singleTableBuilder.append(suffix);
//			tableList.add(singleTableBuilder.toString());
//
//		}
//		return tableList;
//	}
//
//	public String getType() {
//		return type;
//	}
//
//	public void setType(String type) {
//		this.type = type;
//	}
//
//	public String getPadding() {
//		return padding;
//	}
//
//	public void setPadding(String padding) {
//		this.padding = padding;
//	}
//
//	public String getTableFactor() {
//		return tableFactor;
//	}
//
//	public void setTableFactor(String tableFactor) {
//		this.tableFactor = tableFactor;
//	}
//
//	public int getStep() {
//		return step;
//	}
//
//	public void setStep(int step) {
//		this.step = step;
//	}
//
//	public int getFrom() {
//		return from;
//	}
//
//	public void setFrom(int from) {
//		this.from = from;
//	}
//
//	public int getTo() {
//		return to;
//	}
//
//	public void setTo(int to) {
//		this.to = to;
//	}
//	
//
//}
