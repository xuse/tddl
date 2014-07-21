package com.taobao.tddl.client.controller;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.common.lang.StringUtil;
import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.client.dispatcher.DispatcherResult;
import com.taobao.tddl.client.dispatcher.Result;
import com.taobao.tddl.client.dispatcher.SingleLogicTableName;
import com.taobao.tddl.client.dispatcher.SqlDispatcher;
import com.taobao.tddl.client.dispatcher.impl.DatabaseAndTablesDispatcherResultImp;
import com.taobao.tddl.client.pipeline.PipelineFactory;
import com.taobao.tddl.client.pipeline.bootstrap.Bootstrap;
import com.taobao.tddl.client.pipeline.bootstrap.PipelineBootstrap;
import com.taobao.tddl.interact.rule.VirtualTable;
import com.taobao.tddl.interact.rule.VirtualTableRoot;
import com.taobao.tddl.parser.SQLParser;
import com.taobao.tddl.rule.bean.LogicTable;
import com.taobao.tddl.rule.bean.TDDLRoot;

/**
 * 主要负责root中拿到需要的信息。然后用matcher进行匹配。
 * 
 * 最后返回需要的结果
 * 
 * @author shenxun
 * 
 */
public class SpringBasedDispatcherImpl implements SqlDispatcher {
	static final Log logger = LogFactory
			.getLog(SpringBasedDispatcherImpl.class);
	/**
	 * 需要注入的sql 解析器对象
	 */
	private SQLParser parser = null;

	/**
	 * TDDL的根节点
	 */
	TDDLRoot root;

	/**
	 * 新规则根节点
	 */
	VirtualTableRoot vtabroot;

	/**
	 * 通过TDataSource初始化时注入的pipelineFactory; 主要供测试使用，当然也可以独立使用
	 */
	private PipelineFactory pipelineFactory;

	private Bootstrap bootstrap;

	/**
	 * 兼容性方法
	 */
	public DispatcherResult getDBAndTables(RouteCondition rc) {
		if (null == bootstrap) {
			bootstrap = new PipelineBootstrap(null, pipelineFactory);
		}
		try {
			return bootstrap.bootstrapForGetDBAndTabs(rc, this);
		} catch (SQLException e) {
			// 这个步骤不应该产生任何异常
			return null;
		}
	}

	/**
	 * 兼容性方法
	 */
	public DispatcherResult getDBAndTables(String sql, List<Object> args) {
		if (null == bootstrap) {
			bootstrap = new PipelineBootstrap(null, pipelineFactory);
		}
		try {
			return bootstrap.bootstrapForGetDBAndTabs(sql, args, this);
		} catch (SQLException e) {
			// 这个步骤不应该产生任何异常
			return null;
		}
	}

	public Result getAllDatabasesAndTables(String logicTableName) {
		if(root==null){
			throw new RuntimeException("the root is null,may be use new rule,use getDbTopology may work!");
		}
		LogicTable logicTable = root.getLogicTable(StringUtil
				.toLowerCase(logicTableName));
		if (logicTable == null) {
			throw new IllegalArgumentException("逻辑表名未找到");
		}
		SingleLogicTableName log = new SingleLogicTableName(logicTableName);
		return new DatabaseAndTablesDispatcherResultImp(
				logicTable.getAllTargetDBList(), log);
	}

	public Map<String, Set<String>> getDbTopology(String logicTableName) {
		if(vtabroot==null){
			throw new RuntimeException("the vtabroot is null,may be use old rule, use getAllDatabasesAndTables may work!");
		}
		
		VirtualTable logicTable = vtabroot.getTableRules().get(logicTableName);
		return logicTable.getActualTopology();
	}
	
	/**
	 * 无逻辑的getter/setter
	 */
	public SQLParser getParser() {
		return parser;
	}

	public void setParser(SQLParser parser) {
		this.parser = parser;
	}

	public TDDLRoot getRoot() {
		return root;
	}

	public void setRoot(TDDLRoot root) {
		this.root = root;
	}

	public VirtualTableRoot getVtabroot() {
		return vtabroot;
	}

	public void setVtabroot(VirtualTableRoot vtabroot) {
		this.vtabroot = vtabroot;
	}

	public void setPipelineFactory(PipelineFactory pipelineFactory) {
		this.pipelineFactory = pipelineFactory;
	}
}
