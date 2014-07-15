//Copyright(c) Taobao.com
package com.taobao.tddl.client.pipeline;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.client.databus.DataBus;
import com.taobao.tddl.client.handler.Handler;

/**
 * @description 默认管线实现类,实现一个双向链表,链表存有序的handlers,依次执行handler,
 *              现在只单向调用handler的handleDown(DataBus dataBus)方法进行任务流转
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 2.4.3
 * @since 1.6
 * @date 2010-08-15下午03:24:42
 */
public class DefaultPipeline implements Pipeline {
	private Log logger = LogFactory.getLog(DefaultPipeline.class);

	private volatile DefaultHandlerContext head;
	private volatile DefaultHandlerContext tail;
	private final Map<String, DefaultHandlerContext> name2ctx = new HashMap<String, DefaultHandlerContext>(
			4);
	
	public synchronized void addAfter(String baseName, String name, Handler handler) {
        DefaultHandlerContext ctx=getContextOrDie(baseName);
        if(ctx==tail){
        	addLast(name,handler);
        }else{
        	checkDuplicateName(name);
        	DefaultHandlerContext newCtx=new DefaultHandlerContext(ctx, ctx.next, name, handler);
        	
        	ctx.next.prev=newCtx;
        	ctx.next=newCtx;
        	name2ctx.put(name, newCtx);
        }
	}

	public synchronized void addBefore(String baseName, String name, Handler handler) {
		DefaultHandlerContext ctx=getContextOrDie(baseName);
		if(ctx==head){
			addFirst(name, handler);
		}else{
			checkDuplicateName(name);
			DefaultHandlerContext newCtx=new DefaultHandlerContext(ctx.prev, ctx, name, handler);
			
			ctx.prev.next=newCtx;
			ctx.prev=newCtx;
			name2ctx.put(name, newCtx);
		}

	}

	public synchronized void addFirst(String name, Handler handler) {
		if(name2ctx.isEmpty()){
			init(name, handler);
		}else{
			checkDuplicateName(name);
			DefaultHandlerContext oldHead=head;
			DefaultHandlerContext newHead=new DefaultHandlerContext(null, oldHead, name, handler);
			
			oldHead.prev=newHead;
			head=newHead;
			name2ctx.put(name, newHead);
		}
	}

	public synchronized void addLast(String name, Handler handler) {
		if(name2ctx.isEmpty()){
			init(name,handler);
		}else{
			checkDuplicateName(name);
			DefaultHandlerContext oldTail=tail;
			DefaultHandlerContext newTail=new DefaultHandlerContext(oldTail, null, name, handler);
			
			oldTail.next=newTail;
			tail=newTail;
			name2ctx.put(name, newTail);
		}
	}

	public void startFlow(DataBus dataBus) throws SQLException {
		if(this.head==null){
			logger.warn(
                    "The pipeline contains no next handlers" );
			return;
		}
		
		head.handleDown(head,dataBus);
	}
	
	public synchronized Handler get(String name) {
		DefaultHandlerContext ctx=name2ctx.get(name);
		if(ctx==null){
			return null;
		}else{
			return ctx.getHandler();
		}
	}

	@SuppressWarnings("rawtypes")
	public synchronized HandlerContext getContext(Handler handler) {
		if(handler==null){
			throw new NullPointerException("handler");
		}
		if(name2ctx.isEmpty()){
			return null;
		}
		DefaultHandlerContext ctx=head;
		for(;;){
			if(ctx.getHandler()==handler){
				return ctx;
			}
			ctx=ctx.next;
			if(ctx==null){
				break;
			}
		}
		
		return null;
	}

	@SuppressWarnings("rawtypes")
	public synchronized HandlerContext getContext(String name) {
		if(name==null){
			throw new IllegalArgumentException("name");
		}
		return name2ctx.get(name);
	}

	@SuppressWarnings("rawtypes")
	public HandlerContext getContext(Class<? extends Handler> handlerType) {
		if(handlerType==null){
			throw new NullPointerException("handlerType");
		}
		
		if(name2ctx.isEmpty()){
			return null;
		}
		
		DefaultHandlerContext ctx=head;
		for(;;){
			if(handlerType.isAssignableFrom(ctx.getHandler().getClass())){
				return ctx;
			}
			
			ctx=ctx.next;
			if(ctx==null){
				break;
			}
		}
		return null;
	}

	public synchronized Handler getFirst() {
		DefaultHandlerContext head=this.head;
		if(head==null){
			return null;
		}
		return head.getHandler();
	}

	public synchronized Handler getLast() {
		DefaultHandlerContext tail=this.tail;
		if(tail==null){
			return null;
		}
		return tail.getHandler();
	}

	public synchronized void remove(Handler handler) {
		remove(getContextOrDie(handler));
	}

	public synchronized Handler remove(String name) {
		return remove(getContextOrDie(name)).getHandler();
	}

	@SuppressWarnings("unchecked")
	public synchronized <T extends Handler> T remove(Class<T> handlerType) {
		return (T)remove(getContextOrDie(handlerType)).getHandler();
	}

	public synchronized Handler removeFirst() {
		if(name2ctx.isEmpty()){
			throw new NoSuchElementException();
		}
		
		DefaultHandlerContext oldHead=head;
		if(oldHead==null){
			throw new NoSuchElementException();
		}
		
		if(oldHead.next==null){
			head=tail=null;
			name2ctx.clear();
		}else{
			oldHead.next.prev=null;
			head=oldHead.next;
			name2ctx.remove(oldHead.getName());
		}
		
		return oldHead.getHandler();
	}

	public Handler removeLast() {
	    if(name2ctx.isEmpty()){
	    	throw new NoSuchElementException();
	    }
	    
	    DefaultHandlerContext oldTail=tail;
	    if(oldTail==null){
	    	throw new NoSuchElementException();
	    }
	    
	    if(oldTail.prev==null){
	    	head=tail=null;
	    	name2ctx.clear();
	    }else{
	    	oldTail.prev.next=null;
	    	tail=oldTail.prev;
	    	name2ctx.remove(oldTail.getName());
	    }
	    
		return oldTail.getHandler();
	}

	public synchronized void replace(Handler oldHandler, String newName, Handler newHandler) {
	    replace(getContextOrDie(oldHandler), newName, newHandler);
	}

	public Handler replace(String oldName, String newName, Handler newHandler) {
		return replace(getContextOrDie(oldName),newName,newHandler);
	}

	@SuppressWarnings("unchecked")
	public <T extends Handler> T replace(Class<T> oldHandlerType,
			String newName, Handler newHandler) {
		return (T)replace(getContextOrDie(oldHandlerType),newName,newHandler);
	}
	
	/**
	 * 初始化管线，设置头结点。
	 * 
	 * @param name
	 * @param handler
	 */
	private void init(String name,Handler handler){
		DefaultHandlerContext ctx=new DefaultHandlerContext(null, null, name, handler);
		head=tail=ctx;
		name2ctx.clear();
		name2ctx.put(name, ctx);
	}
	
	/**
	 * 检查指定名字的上下文节点是否为空。
	 * 
	 * @param name
	 * @return
	 */
	private DefaultHandlerContext getContextOrDie(String name){
		DefaultHandlerContext ctx=(DefaultHandlerContext)getContext(name);
		if(ctx==null){
			throw new NoSuchElementException("不存在如下上下文节点："+name);
		}else{
			return ctx;
		}
	}
	
	/**
	 * 检查HandlerContext是否有重名。
	 * 如果重名，初始化失败，直接抛异常终止程序
	 * 
	 * @param name
	 */
	private void checkDuplicateName(String name){
		if(name2ctx.containsKey(name)){
			throw new IllegalArgumentException("处理器上下文重名，重名的是："+name);
		}
	}
	
	/**
	 * 检查指定处理器的上下文节点是否为空
	 * 
	 * @param handler
	 * @return
	 */
	private DefaultHandlerContext getContextOrDie(Handler handler){
		DefaultHandlerContext ctx=(DefaultHandlerContext)getContext(handler);
		if(ctx==null){
			throw new NoSuchElementException("不存在如下上下文节点："+handler.getClass().getName());
		}else{
			return ctx;
		}
	}
	
	/**
	 * 检查指定处理器类型的上下文节点是否为空
	 * 
	 * @param handlerType
	 * @return
	 */
	private DefaultHandlerContext getContextOrDie(Class<? extends Handler> handlerType){
		DefaultHandlerContext ctx=(DefaultHandlerContext)getContext(handlerType);
		if(ctx==null){
			throw new NoSuchElementException("不存在如下上下文节点："+handlerType.getName());
		}else{
			return ctx;
		}
	}
	
	/**
	 * 得到下一个节点。
	 * TODO：改造，应该不需要这么麻烦。
	 * 
	 * @param ctx
	 * @return
	 */
	DefaultHandlerContext getActualFlowContext(DefaultHandlerContext ctx){
		if(ctx==null){
			return null;
		}
		
		DefaultHandlerContext realCtx=ctx;
		realCtx=realCtx.next;
		if(realCtx==null){
			return null;
		}
		
		return realCtx;
	}
	
	/**
	 * 从处理器链表中移除指定处理器上下文节点
	 * 
	 * @param ctx
	 * @return
	 */
	private DefaultHandlerContext remove(DefaultHandlerContext ctx){
		if(head==tail){
			head=tail=null;
			name2ctx.clear();
		}else if(ctx==head){
			removeFirst();
		}else if(ctx==tail){
			removeLast();
		}else{
			DefaultHandlerContext prev=ctx.prev;
			DefaultHandlerContext next=ctx.next;
			prev.next=next;
			next.prev=prev;
			name2ctx.remove(ctx.getName());
		}
		return ctx;
	}
	
	/**
	 * 替换已存在的处理器上下文
	 * 
	 * @param ctx
	 * @param newName
	 * @param newHandler
	 * @return
	 *        返回旧的处理器
	 */
	private Handler replace(DefaultHandlerContext ctx,String newName,Handler newHandler){
		if(ctx==head){
			removeFirst();
			addFirst(newName, newHandler);
		}else if(ctx==tail){
			removeLast();
			addLast(newName,newHandler);
		}else{
			boolean sameName=ctx.getName().equals(newName);
			if(!sameName){
				checkDuplicateName(newName);
			}
			
			DefaultHandlerContext prev=ctx.prev;
			DefaultHandlerContext next=ctx.next;
			DefaultHandlerContext newCtx=new DefaultHandlerContext(prev, next, newName, newHandler);
			
			prev.next=newCtx;
			next.prev=newCtx;
			
			if(!sameName){
				name2ctx.remove(ctx.getName());
				name2ctx.put(newName, newCtx);
			}
		}
		return ctx.getHandler();
	}

    /**
     * 默认的处理器上下文，主要的执行动作在此处流转。
     * 
     * @author junyu
     *
     */
	@SuppressWarnings("rawtypes")
	public class DefaultHandlerContext implements HandlerContext {
		volatile DefaultHandlerContext next;
		volatile DefaultHandlerContext prev;
		private final String name;
		private final Handler handler;

		DefaultHandlerContext(DefaultHandlerContext prev,
				DefaultHandlerContext next, String name, Handler handler) {
			if (name == null) {
				throw new NullPointerException("name");
			}
			if (handler == null) {
				throw new NullPointerException("handler");
			}
			this.handler = handler;
			this.name = name;
			this.prev = prev;
			this.next = next;
		}

		public void flowNext(DataBus dataBus) throws SQLException {
            DefaultHandlerContext realNext=getActualFlowContext(this);
            if(realNext==null){
            	return;
            }
        
            handleDown(realNext,dataBus);
		}
		
		public void handleDown(DefaultHandlerContext realNext,DataBus dataBus) throws SQLException{
			realNext.getHandler().handleDown(dataBus);
            realNext.flowNext(dataBus);
		}

		public Handler getHandler() {
			return handler;
		}

		public String getName() {
			return name;
		}

		public Pipeline getPipeLine() {
			return DefaultPipeline.this;
		}
	}
}
