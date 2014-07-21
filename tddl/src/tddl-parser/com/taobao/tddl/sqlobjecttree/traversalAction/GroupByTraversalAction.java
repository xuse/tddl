package com.taobao.tddl.sqlobjecttree.traversalAction;

import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.sqlobjecttree.OrderByEle;

public class GroupByTraversalAction implements TraversalSQLAction {
	private List<OrderByEle> orderByEles = new LinkedList<OrderByEle>();

	public void actionProformed(TraversalSQLEvent event) {

		List<OrderByEle> temp = event.getCurrStatement().nestGetGroupByList();
		if (orderByEles.size()==0) {
			if (temp != null && temp.size() != 0){
				orderByEles = temp;
			}
		} else {
			if (temp != null&&!temp.isEmpty()){
				//TODO:这个地方要添加一个测试，用于测试嵌套查询中，外嵌套查询有group by 和order by的情况下，是否不会抛出这个异常
				throw new IllegalArgumentException("不允许在嵌套sql中出现多个group by条件");
			}
		}
	}
	public List<OrderByEle> getGroupByEles(){
		return orderByEles;
	}

}
