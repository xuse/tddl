/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.taobao.tddl.util;

/**
 *  规则抽象
 * 
 * @author shenxun
 */
public interface Rule {
    String getDBIndex(Object param);
    String getTable(Object param);
}
