/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.taobao.tddl.sqlobjecttree.common.value;

/**
 *
 * @author shenxun
 */
public class UnknowValueObject implements  Comparable<Object> {
	public static final UnknowValueObject valObj=new UnknowValueObject();
	public static Comparable<?> getUnknowValueObject(){
		return valObj;
	}
    public int compareTo(Object arg0) {
        throw new UnsupportedOperationException("还不支持使用这个函数作为分库条件字段");
    }

}
