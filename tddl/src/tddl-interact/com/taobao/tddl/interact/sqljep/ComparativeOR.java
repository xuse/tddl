package com.taobao.tddl.interact.sqljep;

import java.util.Comparator;
/**
 * AND节点
 * 在实际的SQL中，实际上是类似
 * [Comparative]              [comparative]
 * 			\                  /
 * 			  \				  /
 *             [ComparativeOR]
 *             
 * 类似这样的节点出现
 * 
 * @author shenxun
 *
 */
public class ComparativeOR extends ComparativeBaseList{
	
	public ComparativeOR(int function, Comparable<?> value) {
		super(function, value);
	}
	
	public ComparativeOR(){};
	
	public ComparativeOR(Comparative item){
		super(item);
	}
	public ComparativeOR(int capacity){
		super(capacity);
	}
//	@SuppressWarnings("unchecked")
//	public boolean intersect(int function,Comparable other,Comparator comparator){
//		//这里有个问题，
//		for(Comparative source :list){
//			if(source.intersect(function, other, comparator)){
//				return true;
//			}
//		}
//		return false;
//	}
	@Override
	protected String getRelation() {
		return " or ";
	}
}
