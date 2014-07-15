//Copyright(c) Taobao.com
package com.taobao.tddl.client.pipeline;

import com.taobao.tddl.client.handler.executionplan.BatchTargetSqlHandler;
import com.taobao.tddl.client.handler.executionplan.ExecutionPlanHandler;
import com.taobao.tddl.client.handler.executionplan.SqlDirectDispatchHandler;
import com.taobao.tddl.client.handler.rulematch.RouteMatchHandler;
import com.taobao.tddl.client.handler.sqlparse.RouteConditionHandler;
import com.taobao.tddl.client.handler.sqlparse.SqlParseHandler;
import com.taobao.tddl.client.handler.validate.SqlDispatchHandler;

/**
 * @description 默认管线工厂实现类,提供一个固定handler的管线,只初始化一次,
 *              且不能动态改变管线中handler种类和次序(虽然有对外接口方法)
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.3
 * @since 1.6
 * @date 2010-08-15下午03:24:42
 */
public class DefaultPipelineFactory extends AbstractPipelineFactory {
	private Pipeline defaultPipeline = new DefaultPipeline();

	{
		/**
		 * 初始化各个管线，管线都为单例
		 * 每次查询和数据更新都会返回同一个管线实例。
		 * 唯一与一个操作对应的是总线数据，并且以
		 * 方法的局部变量在管线中的不同处理器之间传递。
		 * 从而从数据上避免多线程问题。
		 */
		defaultPipeline.addLast(RouteConditionHandler.HANDLER_NAME,new RouteConditionHandler());
		defaultPipeline.addLast(SqlParseHandler.HANDLER_NAME,new SqlParseHandler());
		defaultPipeline.addLast(RouteMatchHandler.HANDLER_NAME,new RouteMatchHandler());
		defaultPipeline.addLast(SqlDirectDispatchHandler.HANDLER_NAME, new SqlDirectDispatchHandler());
		defaultPipeline.addLast(SqlDispatchHandler.HANDLER_NAME,new SqlDispatchHandler()); 
		defaultPipeline.addLast(ExecutionPlanHandler.HANDLER_NAME, new ExecutionPlanHandler());
	    defaultPipeline.addLast(BatchTargetSqlHandler.HANDLER_NAME, new BatchTargetSqlHandler());
	}

	public DefaultPipelineFactory() {}
	
	public DefaultPipelineFactory(Pipeline pipeline) {
		this.defaultPipeline=pipeline;
	}
	
	public Pipeline getPipeline() {
		return defaultPipeline;
	}

	public void setDefaultPipeline(Pipeline defaultPipeline) {
		this.defaultPipeline = defaultPipeline;
	}
}
