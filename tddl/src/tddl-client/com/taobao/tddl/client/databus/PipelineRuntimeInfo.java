package com.taobao.tddl.client.databus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.client.controller.DatabaseExecutionContext;
import com.taobao.tddl.client.dispatcher.DispatcherResult;
import com.taobao.tddl.client.dispatcher.SqlDispatcher;
import com.taobao.tddl.client.handler.AbstractHandler.FlowType;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.common.channel.SqlMetaData;
import com.taobao.tddl.interact.bean.MatcherResult;
import com.taobao.tddl.interact.bean.TargetDB;
import com.taobao.tddl.interact.rule.VirtualTableRoot;
import com.taobao.tddl.parser.SQLParser;
import com.taobao.tddl.rule.bean.TDDLRoot;
import com.taobao.tddl.sqlobjecttree.SqlParserResult;

/**
 * 存放一次查询或者一次数据更新过程中的数据
 * 
 * @author junyu
 *
 */
public class PipelineRuntimeInfo {
	public static final String INFO_NAME="PipelineRuntimeInfo";
	/**
	 * 初始信息如原始sql,参数等
	 */
	private StartInfo startInfo;
	
	/**
	 * Sql解析结果（RouteCondition也会模拟产生一个）
	 */
    private SqlParserResult sqlParserResult;
    
    /**
     * 是否经过了Sql解析器（RouteCondition设置false）
     */
    private boolean isSqlParsed;
    
    /**
     * 逻辑表名，可以从sqlParserResult中得到
     */
    private Set<String> logicTableNames;
    
    /**
     * 是否允许反向输出
     */
    private boolean isAllowReverseOutput;
   
    /**
     * 包含真正要执行的sql和数据库
     */
	private DispatcherResult metaData;
	
	/**
	 * 规则匹配器匹配出的结果
	 */
	private MatcherResult matcherResult ;
	
	/**
	 * 执行计划，最终交给数据查询或者数据更新处理器的数据
	 */
	private ExecutionPlan executionPlan;
	
	/**
	 * 保存sql解析的结果和原始sql信息
	 */
	private SqlMetaData SqlMetaData;

	/**
	 * 代表一套规则，在初始化时从规则池里选中（也有可能指定了其中一套规则）
	 */
	private SqlDispatcher sqlDispatcher;
	
	/**
	 * 流转类型
	 */
	private FlowType flowType;
	
	private boolean needRowCopy=false;
	
	private List<String> uniqueColumns;
    
	/**
	 * 被join的虚拟表名
	 */
	private List<String> virtualJoinTableNames= new ArrayList<String>();
	
	List<List<TargetDB>> targetDBList;
	
	List<DatabaseExecutionContext> dataBaseExecutionContext;
	
	private Map<String, String> alias ;
	
	/**
	 * id in归组标识
	 */
	private boolean needIdInGroup;
	
	/**
	 * 多库多表distinct标识
	 */
	private boolean completeDistinct;
	
	/**
	 * sql中带的groupHint,需要在通过sql解析器
	 * 之前去除,在解析后添加回,暂时不支持参数
	 * 占位
	 */
	private String groupHintStr;
	
	public SqlParserResult getSqlParserResult() {
		return sqlParserResult;
	}
	
	public void setSqlParserResult(SqlParserResult sqlParserResult) {
		this.sqlParserResult = sqlParserResult;
	}
	
	public boolean getIsSqlParsed() {
		return isSqlParsed;
	}
	
	public void setIsSqlParsed(boolean isSqlParsed) {
		this.isSqlParsed = isSqlParsed;
	}
	
	public Set<String> getLogicTableNames() {
		return logicTableNames;
	}
	
	public void setLogicTableNames(Set<String> logicTableNames) {
		this.logicTableNames = logicTableNames;
	}
	
	public boolean isAllowReverseOutput() {
		return isAllowReverseOutput;
	}
	
	public void setAllowReverseOutput(boolean isAllowReverseOutput) {
		this.isAllowReverseOutput = isAllowReverseOutput;
	}
	
	public SqlDispatcher getSqlDispatcher() {
		return sqlDispatcher;
	}
	
	public void setSqlDispatcher(SqlDispatcher sqlDispatcher) {
		this.sqlDispatcher = sqlDispatcher;
	}
	
	public DispatcherResult getMetaData() {
		return metaData;
	}
	
	public void setMetaData(DispatcherResult metaData) {
		this.metaData = metaData;
	}
	
	public MatcherResult getMatcherResult() {
		return matcherResult;
	}
	
	public void setMatcherResult(MatcherResult matcherResult) {
		this.matcherResult = matcherResult;
	}
	
	public ExecutionPlan getExecutionPlan() {
		return executionPlan;
	}
	
	public void setExecutionPlan(ExecutionPlan executionPlan) {
		this.executionPlan = executionPlan;
	}
	
	public TDDLRoot getTDDLRoot(){
		return this.sqlDispatcher.getRoot();
	}
	
	public VirtualTableRoot getVirtualTableRoot(){
		return this.sqlDispatcher.getVtabroot();
	}
	
	public SQLParser getSQLParser(){
		return this.sqlDispatcher.getParser();
	}
	
	public StartInfo getStartInfo() {
		return startInfo;
	}
	
	public void setStartInfo(StartInfo startInfo) {
		this.startInfo = startInfo;
	}

	public List<String> getVirtualJoinTableNames() {
		return virtualJoinTableNames;
	}

	public void setVirtualJoinTableNames(List<String> virtualJoinTableNames) {
		this.virtualJoinTableNames = virtualJoinTableNames;
	}

	public FlowType getFlowType() {
		return flowType;
	}

	public void setFlowType(FlowType flowType) {
		this.flowType = flowType;
	}

	public List<List<TargetDB>> getTargetDBList() {
		return targetDBList;
	}

	public void setTargetDBList(List<List<TargetDB>> targetDBList) {
		this.targetDBList = targetDBList;
	}

	public List<DatabaseExecutionContext> getDataBaseExecutionContext() {
		return dataBaseExecutionContext;
	}

	public void setDataBaseExecutionContext(
			List<DatabaseExecutionContext> dataBaseExecutionContext) {
		this.dataBaseExecutionContext = dataBaseExecutionContext;
	}

	public Map<String, String> getAlias() {
		return alias;
	}

	public void setAlias(Map<String, String> alias) {
		this.alias = alias;
	}

	public boolean isNeedRowCopy() {
		return needRowCopy;
	}

	public void setNeedRowCopy(boolean needRowCopy) {
		this.needRowCopy = needRowCopy;
	}

	public List<String> getUniqueColumns() {
		return uniqueColumns;
	}

	public void setUniqueColumns(List<String> uniqueColumns) {
		this.uniqueColumns = uniqueColumns;
	}

	public boolean isNeedIdInGroup() {
		return needIdInGroup;
	}

	public void setNeedIdInGroup(boolean needIdInGroup) {
		this.needIdInGroup = needIdInGroup;
	}

	public boolean isCompleteDistinct() {
		return completeDistinct;
	}

	public void setCompleteDistinct(boolean completeDistinct) {
		this.completeDistinct = completeDistinct;
	}

	public String getGroupHintStr() {
		return groupHintStr;
	}

	public void setGroupHintStr(String groupHintStr) {
		this.groupHintStr = groupHintStr;
	}
	
	public SqlMetaData getSqlMetaData() {
		return SqlMetaData;
	}

	public void setSqlMetaData(SqlMetaData sqlMetaData) {
		SqlMetaData = sqlMetaData;
	}
}
