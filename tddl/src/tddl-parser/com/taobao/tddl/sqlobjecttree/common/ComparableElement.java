package com.taobao.tddl.sqlobjecttree.common;

public class ComparableElement {
	public ComparableElement(Comparable<?> comp,boolean isAnd,int operator) {
		this.comp=comp;
		this.isAnd=isAnd;
		this.operator=operator;
	}
	public int  operator;
	public Comparable<?> comp;
	/**
	 * 这个参数目前只用是用来表示in的时候是false,其他时候都是true.or表达式会在Or表达式内自己做处理
	 */
	public boolean isAnd;
}
