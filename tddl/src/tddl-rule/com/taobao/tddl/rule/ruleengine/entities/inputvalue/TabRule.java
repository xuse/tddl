package com.taobao.tddl.rule.ruleengine.entities.inputvalue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.taobao.tddl.common.exception.runtime.CantIdentifyNumberExcpetion;
import com.taobao.tddl.common.exception.runtime.NotSupportException;
import com.taobao.tddl.common.exception.runtime.TDLRunTimeException;
import com.taobao.tddl.rule.ruleengine.TableRuleType;
import com.taobao.tddl.rule.ruleengine.impl.type.TableNameTypeHandler;
import com.taobao.tddl.rule.ruleengine.impl.type.TypeHandlerUtils;
import com.taobao.tddl.rule.ruleengine.impl.type.TypeRegister;

public class TabRule {
	/**
	 * tab1,tab2,tab3。。。。
	 */
	public final static int SPLIT_BY_COMMA = 1;

	/**
	 * 虚拟表名+数字范围vtab[1-2]
	 */
	public final static int VTAB_NAME_PLUS_NUMBER_RANGE = 2;
	private String padding="";
//	private boolean initedPadding=false;
//	private boolean initedPlaceHolder=false;
	private int width=TypeHandlerUtils.DEFAULT_BIT;
	private String primaryKey="";
	private String parameter="";
	private String expFunction="";
	private TableRuleType tableType=TableRuleType.SUFFIX;
	private Collection<String> defaultTable;
//	private Set<String> allowTables;
	private int offset = 0;

	public TabRule() {
	}

//	public void addAllowTable(String allowTab) {
//		Collection<String> retCol = new HashSet<String>();
//		addToCollection(retCol, allowTab);
//		this.allowTables = (Set<String>) retCol;
//	}

	public boolean containThisTable(String table) {
		if (defaultTable == null || defaultTable.size() == 0) {
			return true;
		}
		return defaultTable.contains(table);
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}



	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		if(primaryKey!=null){
			if(primaryKey.contains(",")){
				throw new IllegalArgumentException("不能有两个或多个主键");
			}
			this.primaryKey = primaryKey.toLowerCase();
		}
	}
	

	public String getPadding() {
		return padding;
	}

	public void setPadding(String padding) {
//		initedPadding=true;
		if(padding!=null){
			this.padding = padding.trim().toLowerCase();
		}
	}

	public int getWidth() {
		return width;
	}

	public void setWidth(String width) {
//		initedPlaceHolder=true;
		if(width==null||width.equals("")){
			//do nothing;
		}else{
			Integer widthInt=Integer.valueOf(width);
			if(widthInt>8){
				throw new IllegalArgumentException("占位符不能超过8位");
			}
			if(widthInt<=0){
				throw new IllegalArgumentException("占位符不能为负值或为0");
			}
			this.width = widthInt;
		}
	}

	/**
	 * 做了空指针检查
	 * 
	 * @param defaultTable
	 */
	public void setDefaultTable(String defaultTable) {
		
		this.defaultTable = getCollection( defaultTable);

	}


	private Collection<String> getCollection( String targetTables) {
		//不做这个检查了，但如果需要设置padding和placeHolder必须制定这两个参数
//		if(!initedPadding||!initedPlaceHolder){
//			throw new IllegalStateException("应先初始化padding和placeHolder");
//		}
		List<String> retCol = new ArrayList<String>();;
		int type = -100;
		if (targetTables != null && !targetTables.equals("")) {
			targetTables=targetTables.toLowerCase();
			if (targetTables.contains("[") && targetTables.contains("]")) {
				type = 2;
			} else {
				type = 1;
			}

			switch (type) {
			case 1:
				String[] retStr = null;
				retStr = targetTables.split(",");
				int i=0;
				for (String str : retStr) {
					String obj=str;
					retCol.add(obj);
					i++;
				}
				break;

			case 2:
				String[] tempMessage = targetTables.split(",");
				if (tempMessage.length == 1) {
					getDefaultTables(retCol, targetTables, 1, tableType);
				} else if (tempMessage.length == 2) {
					Integer inte = Integer.valueOf(tempMessage[1].trim());
					getDefaultTables(retCol, tempMessage[0], inte, tableType);
				}
				break;
			default:
				throw new NotSupportException("还不支持的情况");
			}
		}
		return retCol;
	}

	private void getDefaultTables(Collection<String> collection,
			String tabToken, int step, TableRuleType type) {
		List<Integer> startEnd = new ArrayList<Integer>(); 
		StringBuffer vTab = new StringBuffer();;
		TableNameTypeHandler tHandler = TypeRegister.getTableNameHandler(type);
		int curPos = parseDefaultTables(0, tabToken,startEnd, vTab);
		if (curPos < tabToken.length() -1) {
			//有2个 []
			List<Integer> startEnd2 = new ArrayList<Integer>();
			curPos = parseDefaultTables(curPos, tabToken, startEnd2, vTab);
			if (curPos == -1) {
				throw new TDLRunTimeException("解析defaultTableRange出错" + tabToken);
			}
			tHandler.buildAllPassableTable(collection, step, step, vTab.toString(), startEnd.get(0), startEnd2.get(0), startEnd.get(1), startEnd2.get(1), padding,width);
		} else {
			tHandler.buildAllPassableTable(collection, step, vTab.toString(), startEnd.get(0), startEnd.get(1), padding,width);
		}

	}
	
	/**
	 * 解析字符串 [00-09]
	 * @return 结束的位置， -1表示解析不成功
	 */
	private int parseDefaultTables(int cur, String tabToken, List<Integer> startEnd, StringBuffer vTab) {
		int st = tabToken.indexOf("[", cur);
		int ed = tabToken.indexOf("]", st);
		if (st == -1 || ed == -1) {
			return -1;
		}
		String[] range = tabToken.substring(st + 1, ed).split("-");
		if (cur == 0) {
			vTab.append(tabToken.substring(0, st).trim());
		}
		try {
			int rStart = Integer.valueOf(range[0].trim()).intValue();
			int rEnd = Integer.valueOf(range[1].trim()).intValue();
			if (rStart > rEnd) {
				int temp = rStart;
				rStart = rEnd;
				rEnd = temp;
			}
			startEnd.add(rStart);
			startEnd.add(rEnd);
		} catch (NumberFormatException e) {
			throw new CantIdentifyNumberExcpetion(range[0], range[1], e);
		}
		return ed;
	}


	public Collection<String> getDefaultTable() {
		return defaultTable;
	}

	public void setDefaultTables(Collection<String>  tabs) {
		defaultTable = tabs;
	}
	
	
	public String[] getAllParameter() {
		return parameter.split("\\|");
	}

	
	public String getParameter() {

		return parameter;
	}

	public String getExpFunction() {

		return expFunction;
	}

	public void setParameter(String parameter) {
		if(parameter!=null){
			this.parameter = parameter.toLowerCase();
		}
	}

	public void setExpFunction(String expFunction) {
		if(expFunction!=null){
			this.expFunction = expFunction.toLowerCase();
		}
	}

	public TableRuleType getTableType() {
		return tableType;
	}

	public void setTableType(TableRuleType tableType) {
		this.tableType = tableType;
	}

}
