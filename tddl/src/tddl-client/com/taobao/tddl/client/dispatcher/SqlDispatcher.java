package com.taobao.tddl.client.dispatcher;

import java.util.List;

import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.client.pipeline.PipelineFactory;
import com.taobao.tddl.common.exception.checked.TDLCheckedExcption;
import com.taobao.tddl.interact.rule.VirtualTableRoot;
import com.taobao.tddl.parser.SQLParser;
import com.taobao.tddl.rule.bean.TDDLRoot;

/**
 * 从TStatement的角度看，只需传入sql和其参数，就可以得到以下信息：
 *    1. 这个sql需要在哪些库表上执行
 *    2. 这些sql包含哪些特殊的函数，需要TStatement做特别的处理。包括
 *       a)
 *       b)
 * 因此有了这个接口
 * 
 * 要完成这个接口要求的事情基本上需要如下步骤：
 *    1. 解析sql得到sql本身的结构化信息
 *    2. 从解析结果得到逻辑表名, 从而得到对应的一套规则
 *    3. 从规则得到分库分表字段信息，从解析结果得到这些字段在sql中的条件
 *    4. 根据分库分表字段在sql中的条件（=或范围），和规则做匹配
 * 
 * 所以要进一步有以下几个接口: 解析、规则、匹配
 *  
 * @author linxuan
 */
public interface SqlDispatcher extends DatabaseChoicer {
	/**
	 * 获取当前数据库和表。
	 * @param sql
	 * @param args
	 * @return
	 * @throws TDLCheckedExcption
	 */
	DispatcherResult getDBAndTables(String sql, List<Object> args);

	/**
	 * 不解析SQL，由ThreadLocal传入的指定对象（RouteCondition），决定库表目的地的接口
	 * @param rc
	 * @return
	 */
	DispatcherResult getDBAndTables(RouteCondition rc);
	
	/**
	 * 取得默认的TDDLRoot
	 * 
	 * add by junyu
	 * 
	 * @return
	 */
    public TDDLRoot getRoot();
    
    /**
     * 取得VirtualTableRoot
     * @return
     */
    public VirtualTableRoot getVtabroot();
    
    /**
     * 取得SqlParser
     * 
     * add by junyu
     * @return
     */
	public SQLParser getParser();
	
	/**
	 * 注入pipelineFactory,供测试使用
	 * 
	 * add by junyu
	 */
	public void setPipelineFactory(PipelineFactory pipelineFactory);
}
