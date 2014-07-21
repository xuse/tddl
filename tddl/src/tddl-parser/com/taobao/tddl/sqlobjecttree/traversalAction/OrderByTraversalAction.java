package com.taobao.tddl.sqlobjecttree.traversalAction;

import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.sqlobjecttree.OrderByEle;

public class OrderByTraversalAction implements TraversalSQLAction {
	private List<OrderByEle> orderByEles = new LinkedList<OrderByEle>();

	public void actionProformed(TraversalSQLEvent event) {

		List<OrderByEle> temp = event.getCurrStatement().nestGetOrderByList();
		if (orderByEles.size()==0) {
			if (temp != null && temp.size() != 0){
				orderByEles = temp;
			}
		} else {
			if (temp != null&&!temp.isEmpty()){
				//如果list不为null,并且是非空，则表示集合内有数据
				throw new IllegalArgumentException("不允许在嵌套sql中出现多个order by条件");
			}
		}
	}
	public List<OrderByEle> getOrderByEles(){
		return orderByEles;
	}

}
