//Copyright(c) Taobao.com
package com.taobao.tddl.client.pipeline;

import java.sql.SQLException;

import com.taobao.tddl.client.databus.DataBus;
import com.taobao.tddl.client.handler.Handler;

/**
 * @description 管线内部使用数据结构(即链表元素),封装了handler,一个handler
 *              对应一个HandlerContext
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.3
 * @since 1.6
 * @date 2010-08-15下午03:54:42
 */
public interface HandlerContext <T> {
	
	/**
	 * 取得当前的管线实例
	 * 
	 * @return
	 */
    Pipeline getPipeLine();
    
    /**
     * 取得当前执行器上下文
     * 
     * @return
     */
    String getName();
    
    /**
     * 取得绑定的执行器
     *
     * @return
     */
    Handler getHandler();
    
    /**
     * 处理完毕后向后传递数据总线
     * 1.context维护其前后节点，由Pipeline初始化绑定。
     * 2.方法调用自身绑定的Handler处理完业务后，
     *   取得其邻接的congtext节点，并且调用下一个
     *   context节点的flowNext
     * 3.如果next节点为null，现在的设计是调用结束。
     * 
     * @param dataBus
     * @throws SQLException
     */
	void flowNext(DataBus dataBus) throws SQLException;
}
