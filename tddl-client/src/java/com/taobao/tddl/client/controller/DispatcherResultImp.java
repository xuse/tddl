package com.taobao.tddl.client.controller;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.client.dispatcher.DispatcherResult;
import com.taobao.tddl.client.dispatcher.EXECUTE_PLAN;
import com.taobao.tddl.client.dispatcher.LogicTableName;
import com.taobao.tddl.interact.bean.TargetDB;
import com.taobao.tddl.sqlobjecttree.GroupFunctionType;
import com.taobao.tddl.sqlobjecttree.SetElement;

/**
 * 一个最终告诉Statement要怎么做的东西
 * 
 * 即包含了分库分表的结果，也包含了SQL本身的信息，和规则中的一些配置
 * 
 * 是从SQL解析、规则读取和匹配结果中抽取需要信息组装而成的
 * 
 * TargetDBMeta 和 TargetDBMetaData 合并拉直而来 1. 修改了分表键可以为多个
 * 
 * @author linxuan
 * 
 */
public class DispatcherResultImp implements DispatcherResult {
	/**
	 * max值，如果sql中给定了limit m,n,或rownum<xx 则max值会随之变化为应用中的值
	 * 需要注意的是，max本身是limitTo的含义，因此其实最后都会变为 xxx<max这样的语义
	 * <p>
	 * 对于oracle: rownum<=n max=n+1
	 * </p>
	 * <p>
	 * 对于mysql: limit m,n max=m+n
	 * </p>
	 */
	private final int max;

	/**
	 * skip值，如果sql中给定了limit m,n,或rownum>xx 则skip值会随之变化为应用中的值
	 * 需要注意的是，skip本身是limitFrom的含义，因此其实最后都会变为 xxx>=m这样的语义
	 * <p>
	 * 对于oracle: rownum>n skip=n+1
	 * </p>
	 * <p>
	 * 对于mysql: limit m,n skip=m
	 * </p>
	 */
	private final int skip;

	/**
	 * sql 中的order by 信息
	 */
	private final OrderByMessages orderByMessages;

	/**
	 * 在sql中最外层嵌套的select中的columns里面的group function信息。 若该处有group
	 * function,则parser会对其进行判断，确保只有一个group function，没有其他列。如果有则抛出异常 若经检查除了group
	 * function以外没有其他列存在，则会返回该group function对应的Type 如果没有group
	 * function或者是其他类型的sql(insert update等)。则返回normal.
	 */
	private final GroupFunctionType groupFunctionType;

	/**
	 * 主键，分库键本身是不允许多个的
	 */
	private ColumnMetaData uniqueKey;

	/**
	 * 分库键列表，因为分库键本身是允许多个的，所以是个list.里面如果在xml中配置了parameters项，则每一个
	 * 用','分隔的项目都对应list中的一项。ColumnMetaData中的key对应了parameters里每一个用','分隔的项目
	 * 而value对应已经通过计算并且绑定了变量以后的值，这个值允许为null,为null则表示用户没有在sql中给出对应 的参数。
	 */
	private final List<ColumnMetaData> splitDB = new LinkedList<ColumnMetaData>();

	/**
	 * 分表键，因为分表键本身是允许多个的，所以是个ColumnMetaData对象.里面如果在xml中配置了表规则中的parameters项
	 * ，则每一个用','分隔的项目都对应list中的一项。ColumnMetaData中的key对应了parameters里每一个用','分隔的项目
	 * 而value对应已经通过计算并且绑定了变量以后的值，这个值允许为null,为null则表示用户没有在sql中给出对应 的参数。
	 */
	private final List<ColumnMetaData> splitTab = new LinkedList<ColumnMetaData>();

	/**
	 * 数据库执行计划
	 */
	private EXECUTE_PLAN databaseExecutePlan;

	/**
	 * 表的执行计划，如果有多个库里面的多个表的个数不同，那么按照表的数量最多的那个值为准。
	 * 即：如db1~5，表的个数分别为0,0,0,0,1:那么返回的表执行计划为SINGLE
	 * 若，表的个数分别为0,1,2,3,4,5：那么返回表的执行计划为MULTIPLE.
	 */
	private EXECUTE_PLAN tableExecutePlan;

	private List<String> distinctColumns;

	/**
	 * 是否允许反向输出
	 */
	private boolean allowReverseOutput;

	/**
	 * 是否允许行复制
	 */
	private final boolean needRowCopy;

	/**
	 * 虚拟表名
	 */
	private final LogicTableName virtualTableName;

	private List<SetElement> setElements;

	private List<DatabaseExecutionContext> databaseExecutionContexts;

	/** 被join的虚拟表名 */
	List<String> virtualJoinTableNames = new ArrayList<String>();

	public DispatcherResultImp(LogicTableName virtualTableName,
			List<DatabaseExecutionContext> databaseExecutionContexts,
			boolean needRowCopy, boolean allowReverseOutput, int skip, int max,
			OrderByMessages orderByMessages,
			GroupFunctionType groupFunctionType, List<String> distinctColumns) {
		this.skip = skip;
		this.max = max;
		this.orderByMessages = orderByMessages;
		this.groupFunctionType = groupFunctionType;
		this.databaseExecutionContexts = databaseExecutionContexts;
		this.virtualTableName = virtualTableName;
		this.needRowCopy = needRowCopy;
		this.allowReverseOutput = allowReverseOutput;
		this.distinctColumns = distinctColumns;
	}

	@SuppressWarnings("deprecation")
	public List<TargetDB> getTarget() {
		List<TargetDB> targetDBs = new ArrayList<TargetDB>(
				databaseExecutionContexts.size());
		for (DatabaseExecutionContext databaseExecutionContext : databaseExecutionContexts) {
			targetDBs.add(databaseExecutionContext.getTargetDB());
		}
		return targetDBs;
	}

	public List<DatabaseExecutionContext> getDatabaseExecutionContexts() {
		return databaseExecutionContexts;
	}

	public int getMax() {
		return max;
	}

	public int getSkip() {
		return skip;
	}

	public OrderByMessages getOrderByMessages() {
		return orderByMessages;
	}

	public LogicTableName getVirtualTableName() {
		return this.virtualTableName;
	}

	public boolean needRowCopy() {
		return this.needRowCopy;
	}

	public ColumnMetaData getPrimaryKey() {
		return uniqueKey;
	}

	public void setUniqueKey(ColumnMetaData uniqueKey) {
		this.uniqueKey = uniqueKey;
	}

	public List<ColumnMetaData> getSplitDB() {
		return splitDB;
	}

	public void addSplitDB(ColumnMetaData splitDB) {
		this.splitDB.add(splitDB);
	}

	public void addSplitTab(ColumnMetaData splitTab) {
		this.splitTab.add(splitTab);
	}

	public boolean allowReverseOutput() {
		return this.allowReverseOutput;
	}

	public void needAllowReverseOutput(boolean reverse) {
		this.allowReverseOutput = reverse;
	}

	public GroupFunctionType getGroupFunctionType() {
		return groupFunctionType;
	}

	public List<ColumnMetaData> getSplitTab() {
		return splitTab;
	}

	public EXECUTE_PLAN getDatabaseExecutePlan() {
		return databaseExecutePlan;
	}

	public void setDatabaseExecutePlan(EXECUTE_PLAN databaseExecutePlan) {
		this.databaseExecutePlan = databaseExecutePlan;
	}

	public EXECUTE_PLAN getTableExecutePlan() {
		return tableExecutePlan;
	}

	public void setTableExecutePlan(EXECUTE_PLAN executePlan) {
		this.tableExecutePlan = executePlan;
	}

	public void setSetElements(List<SetElement> setElements) {
		this.setElements = setElements;
	}

	public List<SetElement> getSetElements() {
		return setElements;
	}

	public List<String> getVirtualJoinTableNames() {
		return virtualJoinTableNames;
	}

	public void setVirtualJoinTableNames(List<String> virtualJoinTableNames) {
		this.virtualJoinTableNames.addAll(virtualJoinTableNames);
	}

	public List<DatabaseExecutionContext> getDataBaseExecutionContexts() {
		return databaseExecutionContexts;
	}

	public List<String> getDistinctColumns() {
		return distinctColumns;
	}
}
