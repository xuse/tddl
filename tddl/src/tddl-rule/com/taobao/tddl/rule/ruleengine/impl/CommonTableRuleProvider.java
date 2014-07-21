package com.taobao.tddl.rule.ruleengine.impl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.exception.checked.CantFindPositionByParamException;
import com.taobao.tddl.common.exception.checked.ComparativeArraysOutOfBoundsException;
import com.taobao.tddl.common.exception.checked.ParseSQLJEPException;
import com.taobao.tddl.common.exception.checked.TDLCheckedExcption;
import com.taobao.tddl.common.exception.runtime.CantFindTargetTabRuleTypeException;
import com.taobao.tddl.common.exception.runtime.NotSupportException;
import com.taobao.tddl.common.sequence.Config;
import com.taobao.tddl.common.util.NestThreadLocalMap;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.interact.sqljep.ComparativeAND;
import com.taobao.tddl.interact.sqljep.ComparativeBaseList;
import com.taobao.tddl.interact.sqljep.ComparativeOR;
import com.taobao.tddl.rule.ruleengine.TableRuleProvider;
import com.taobao.tddl.rule.ruleengine.TableRuleType;
import com.taobao.tddl.rule.ruleengine.entities.inputvalue.TabRule;
import com.taobao.tddl.rule.ruleengine.impl.type.TableNameTypeHandler;
import com.taobao.tddl.rule.ruleengine.impl.type.TypeRegister;

/**
 * 抽象公共基类，主要用于处理date类型的对象
 * @author shenxun
 *
 */
public abstract class CommonTableRuleProvider implements TableRuleProvider {
	protected final static String CALENDAR="CALENDAR";
	private static final Log log = LogFactory.getLog(CommonTableRuleProvider.class);
	protected final static  int LESS_GREAT=1;
	protected final static  int LESS_OR_EQUAL_GREAT_OR_EQUAL=0;
	/**
	 * 复写此方法的子类可直接调用对应calendar里面的常量；
	 * 
	 * @return
	 */
	protected  int getCalendarType(){
		return -1000;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.taobao.tdl.client.ruleEngine.tableDetector.TableRuleProvider#getTables
	 * (java.lang.Comparable, java.util.Map,
	 * com.taobao.tdl.client.ruleEngine.entities.inputValue.TabRule,
	 * java.lang.String)
	 * 旧方法，只用于测试了。
	 */
	public Set<String> getTables(Comparable<?>[] row,
			Map<String, Integer> position, TabRule tab, String vTabName)
			throws CantFindPositionByParamException, ParseSQLJEPException,
			ComparativeArraysOutOfBoundsException {
		log.debug("Thread is in TabProvider's getTables method now");

		validTabRule(tab);
		Integer posInt = position.get(tab.getParameter());
	
		if (posInt == null) {
			throw new CantFindPositionByParamException(tab.getParameter());
		}
		Comparable<?> comparable = row[posInt.intValue()];
		return parseTableNameObj(tab, vTabName, comparable,null);

	}
	public Set<String> getTables(Map<String, Comparative> map, TabRule tab, String tabName, Config config)
			throws TDLCheckedExcption {
		validTabRule(tab);
		
		Comparable<?> comparable= null;
			comparable = map.get(tab.getParameter());
		return parseTableNameObj(tab, tabName, comparable,config);
	}
	protected Set<String> parseTableNameObj(TabRule tab, String vTabName,
			Comparable<?> comparable,Config config) throws ParseSQLJEPException {
		if (comparable instanceof Comparative) {
			//if current input is a subType of Comparative
			return analyzeComparative(tab, vTabName, comparable,config);

		} else if (comparable == null) {
			//if comparable is null,return DefaultTable()
			Set<String> temp = getDefaultTabSet(tab);
			return temp;
		} else {
			throw new NotSupportException("不支持除了Comparative类型和其子类型的其他情况");
		}
	}

	protected Set<String> getDefaultTabSet(TabRule tab) {
		Set<String> temp = new HashSet<String>();
		temp.addAll(getDefaultTabCollection(tab));
		return temp;
	}

	protected Collection<String> getDefaultTabCollection(TabRule tab) {
		return Collections.emptySet();
	}

	/**
	 * analyze a instance of Comparative .temporary we now only support 
	 * @param tab
	 * @param vTabName
	 * @param comparable
	 * @return
	 * @throws ParseSQLJEPException
	 */
	private Set<String> analyzeComparative(TabRule tab, String vTabName,
			Comparable<?> comparable,Config config) throws ParseSQLJEPException {
		Comparative comparative;
		comparative = (Comparative) comparable;
		if (comparative instanceof ComparativeAND) {
			log.debug("comparative is a instance of and ");
			// and中最简单的单独区间内，能直接搞定
			ComparativeAND and = (ComparativeAND) comparative;
			List<Comparative> list = and.getList();
			Set<String> temp = new HashSet<String>();
			getXxxfixedByAndRange(temp,list, tab,vTabName);
			return temp;
		} else if (comparative.getComparison() == Comparative.Equivalent) {
			Set<String> temp = addAEqComparabToXXXFix(tab, vTabName,
					comparative,tab.getOffset(), config);
			return temp;
		} else if (comparative instanceof ComparativeOR) {
			ComparativeOR or=(ComparativeOR)comparative;

			Set<String> temp = new HashSet<String>();
			
			List<Comparative> list=or.getList();
			for(Comparative comp:list){
				temp.addAll(analyzeComparative(tab, vTabName, comp,config));
			}
			log.info("ComparativeOr的情况");
			return temp;
			//bigFix by shenxun 5 25 :测试中发现还有可能一个comparative.getvalue()获取的仍然是comparative的这类情况，因此做了bugfix
		}else if(comparative.getValue() instanceof Comparative){
			return parseTableNameObj(tab, vTabName,comparative.getValue(), config);
		}else {
			Set<String> temp = getDefaultTabSet(tab);
			return temp;
		}
	}

	/**
	 * 重写此方法时要注意offset的处理
	 * @param tab
	 * @param vTabName
	 * @param comparative
	 * @param offset
	 * @param config 
	 * @return
	 */
	protected Set<String> addAEqComparabToXXXFix(TabRule tab,
			String vTabName, Comparative comparative,int offset, Config config) {
		// =的情况也能搞定
		Date date = getDateFromComparative(comparative);
		Integer calType = getCalendarType();
		if (calType == null) {
			throw new CantFindTargetTabRuleTypeException(tab
					.getExpFunction());
		}
//		//为了提高性能，做了一个map缓存Calendar
//		Calendar cal=(Calendar)NestThreadLocalMap.get("CTRP_Calendar");
//		if(cal==null){
//			cal = Calendar.getInstance();
//			NestThreadLocalMap.put("CTRP_Calendar", cal);
//		}
		Calendar cal = getCalendarInThreadLocalMap();
		cal.setTime(date);
		int retInt = getReturnInt(cal, calType);
		
		Set<String> temp = new HashSet<String>();
		String n=processOne(retInt+offset, tab, vTabName);
		if(n!=null){
			temp.add(n);
		}
		return temp;
	}

	protected Date getDateFromComparative(Comparative comparative) {
		Date date = null;
		Comparable<?> comp = comparative.getValue();
		// FIXME:依然写死的了，这里需要为了满足更多的条件进行重构
//		if (comp instanceof CompableBindValue) {
//			CompableBindValue var = (CompableBindValue) comp;
//			date = (Date) var.getBindVal();
//		} else {
			date = (Date) comp;
//		}
		return date;
	}

	protected void validTabRule(TabRule tab) {
		if (tab.getTableType() == null) {
			throw new IllegalArgumentException(
					"不能找到tableRule的tableType属性，这个属性是必填的");
		}
	}

	protected String processOne(Object xxxfix,TabRule tab,String logicTab){
		String vTab = logicTab;
		String physicsTab = null;
		TableRuleType tableType = tab.getTableType();
		TableNameTypeHandler handler = TypeRegister
				.getTableNameHandler(tableType);

		physicsTab = handler.buildOnePhsicTab(xxxfix, tab, vTab);

		return physicsTab;
	}
	protected void processes( Set<String> tabs,List<Object> xxxfixes, TabRule tab,
			String logicTab) {
		String vTab = logicTab;
		List<String> physicsTab = null;
		TableRuleType tableType = tab.getTableType();
		TableNameTypeHandler handler = TypeRegister
				.getTableNameHandler(tableType);

		physicsTab = handler.buildPhysicTab(xxxfixes, tab, vTab);
		tabs.addAll(physicsTab);
	}

	/**
	 * 只支持有一个大于或大于等于，有一个小于或小于等于的这种情况。 xxxfix=suffix or prefix.
	 * 
	 * @param comps
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private void getXxxfixedByAndRange(Set<String> temp,List<Comparative> comps,
			TabRule tab,String vTabName) throws ParseSQLJEPException {
		Comparative start = null;
		Comparative end = null;
		long time = 0;
		if (log.isDebugEnabled()) {
			time = System.currentTimeMillis();
		}
		// 分析start和end
		if (comps.size() == 2) {
			
			for (Comparative c : comps) {
				if(c instanceof ComparativeBaseList){
					throw new IllegalArgumentException("不允许连续and超过3个，或者and ( or )这样的条件作为分表条件");
				}
				if (c.getComparison() == Comparative.GreaterThan
						|| c.getComparison() == Comparative.GreaterThanOrEqual) {
					if (start == null) {

						start = c;
					} else {
						temp.addAll(getDefaultTabCollection(tab));
						// 如果都是大于或大于等于则Default
						log.info("都大于或大于等于这种开区间情况");
						return ;
					}
				} else if (c.getComparison() == Comparative.LessThan
						|| c.getComparison() == Comparative.LessThanOrEqual) {
					if (end == null) {

						end = c;
					} else {
						temp.addAll(getDefaultTabCollection(tab));
						//如果是小于或小于等于的情況，則default
						log.info("都小于或小于等于这种开区间的情况");
						return ;
					}
				} else {
					throw new NotSupportException("and 条件是两个，但至少有一个条件为等于。");
				}
			}
		} else {
			throw new NotSupportException("and条件多于两个");
		}
		Comparable st = start.getValue();

		Comparable ed = end.getValue();
//		if (st instanceof CompableBindValue) {
//			st = ((CompableBindValue) st).getBindVal();
//		}
//		if (ed instanceof CompableBindValue) {
//			ed = ((CompableBindValue) ed).getBindVal();
//		}
		openRangeCheck(tab,st, ed);
		log.debug("start:" + start.getValue() + " comparative signal:"
				+ start.getComparison() + " end: " + end.getValue()
				+ " comparative signal:" + start.getComparison());
		List<Object> retInt=getXxxfixlist(start, end,tab.getOffset(), tab);
		processes(temp, retInt, tab, vTabName);
		if (log.isDebugEnabled()) {
			log.debug("calculation xxxfix finish,elapsed time:"
					+ (System.currentTimeMillis() - time));
			log.debug("ret xxxfix tabSize" + temp.size());
		}
		return ;

	}

	@SuppressWarnings("unchecked")
	protected void openRangeCheck(TabRule tabRule,Comparable st, Comparable ed) {
		if (st.compareTo(ed) > 0) {
			log.info("大于最大值，小于最小值的开区间的情况");
			return ;
		}
	}

	/**
	 * 可通过重写此方法支持更多的基于start一个值，end一个值的尾缀添加逻辑，重写此方法需要注意对offset的处理
	 * @param start
	 * @param end
	 * @param tab TODO
	 * @param temp
	 */
	protected List<Object> getXxxfixlist(Comparative start, Comparative end,int offset, TabRule tab) {

		TreeSet<Integer> ret = new TreeSet<Integer>();
		Calendar cal = getCalendarInThreadLocalMap();
		int startType = getType(start);
		int endType = getType(end);
		// 这里也做了修复。不是很好看
		Date endDate = getDateFromComparative(end);
		int calType = getCalendarType();
		Date st = getDateFromComparative(start);
		int compRes=st.compareTo(endDate);
		if(compRes==0){
			if(startType==LESS_OR_EQUAL_GREAT_OR_EQUAL&&endType==LESS_OR_EQUAL_GREAT_OR_EQUAL){
				//闭区间的情况下，有交集
				List<Object> li=new ArrayList<Object>(1);
				cal.setTime(st);
				li.add(getReturnInt(cal, calType)+offset);
				return li;
			}else{
				//开闭，开开，无交集
				return Collections.emptyList();
			}
		}else if(compRes>0){
			return Collections.emptyList();
		}
		cal.setTime(st);
	
		//添加stDate
		ret.add(getReturnInt(cal, calType)+offset);
		//计算添加后的时间
		Calendar anotherCal=(Calendar)cal.clone();
		//获取当前时间日期的整天数.基准天
		anotherCal.clear();
		anotherCal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DATE));
		
		int max = cal.getMaximum(calType)+1;
		// 用calendar获取时间表
		for (int i = 0; i < max; i++) {
			anotherCal.add(calType, 1);
			//如果自增1天的时间>当前时间
			int compResult=anotherCal.getTime().compareTo(endDate);
			if (compResult>0) {
					//设置end值
					cal.setTime(endDate);
					//利用TreeSet保证数值的唯一性。
					ret.add(getReturnInt(cal, calType)+offset);
					break;
			}else if(compResult == 0 ){
				if(endType == LESS_OR_EQUAL_GREAT_OR_EQUAL){
					//基准值等于end值,添加end值进去
					cal.setTime(endDate);
					ret.add(getReturnInt(cal, calType)+offset);
				}
//				else{
//					endDate小于基准时间，同时基准时间是一天的新开始。所以不添加这个新的一天。	
//				}
				
				break;
			}else{
				//自增天小于当前时间
				ret.add(getReturnInt(anotherCal, calType)+offset);
			}
		}
		List<Object> temp=new ArrayList<Object>(ret.size());
		for (Integer i : ret) {
			temp.add(i);
		}
		return temp;
	}
	protected int getReturnInt(Calendar cal,int calType){
		int ret = cal.get(calType);
		return ret;
	}
	protected Calendar getCalendarInThreadLocalMap() {
		Calendar cal;
		cal = (Calendar)NestThreadLocalMap.get(CALENDAR);
		if(cal==null){
			cal = Calendar.getInstance();
			NestThreadLocalMap.put(CALENDAR, cal);
		}
		return cal;
	}

	/**
	 * 1为>或< 0为>=或<=
	 * 
	 * @param compDate
	 * @return
	 */
	protected int getType(Comparative compDate) {
		int type = -100;
		if (compDate.getComparison() == Comparative.GreaterThanOrEqual
				|| compDate.getComparison() == Comparative.LessThanOrEqual) {
			type = LESS_OR_EQUAL_GREAT_OR_EQUAL;
		} else {
			type = LESS_GREAT;
		}
		return type;
	}
}
