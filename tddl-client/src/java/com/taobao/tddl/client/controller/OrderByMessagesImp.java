package com.taobao.tddl.client.controller;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.sqlobjecttree.OrderByEle;

public class OrderByMessagesImp implements OrderByMessages {
	private final List<OrderByEle> orderbyList = new ArrayList<OrderByEle>();

	/**
	 * null在这里不作处理,因此orderByList不会出现为null的情况
	 * 
	 * @param orderbyList
	 */
	public OrderByMessagesImp(List<OrderByEle> orderbyList) {
		if (orderbyList != null) {
			for (OrderByEle ele : orderbyList) {
				OrderByEle ele2 = new OrderByEle(ele.getTable(), ele.getName(),
						ele.isASC());
				this.orderbyList.add(ele2);
			}
		}
	}

	public List<OrderByEle> getOrderbyList() {
		return orderbyList;
	}
}
