//Copyright(c) Taobao.com
package com.taobao.tddl.client.rule.le;

import java.util.List;

import com.taobao.tddl.common.util.TStringUtil;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a> 
 * @version 1.0
 * @since 1.6
 * @date 2011-5-9ÏÂÎç03:08:34
 */
public class RuleLeBeanConvert {
	public static String convertLeAndOriRuleStr2InteractRule(String ruleStr){
		String convertedLeRule=convertLeRuleStr2TddlRuleStr(ruleStr);
		String convertedOriRule=convertOriRuleStr2TddlRuleStr(convertedLeRule);
		return convertedOriRule;
	}
	
    private static String convertLeRuleStr2TddlRuleStr(String ruleStr){
    	List<String> rulePieces=TStringUtil.split(ruleStr, "com.taobao.tddl.rule.le.VirtualTableRoot");
    	StringBuilder rs=new StringBuilder();
    	for(int i=0;i<rulePieces.size();i++){
    		List<String> rulePieces2=TStringUtil.split(rulePieces.get(i), "com.taobao.tddl.rule.le.TableRule");
    		StringBuilder ts=new StringBuilder();
    		for(int j=0;j<rulePieces2.size();j++){
    			ts.append(rulePieces2.get(j));
    			if(j<(rulePieces2.size()-1)){
        			ts.append("com.taobao.tddl.interact.rule.TableRule");
    			}
    		}
    		
    		rs.append(ts);
    		if(i<(rulePieces.size()-1)){
    			rs.append("com.taobao.tddl.interact.rule.VirtualTableRoot");
    		}
    	}
    	
    	return rs.toString();
    }
    
    private static String convertOriRuleStr2TddlRuleStr(String ruleStr){
    	List<String> rulePieces=TStringUtil.split(ruleStr, "com.taobao.tddl.rule.VirtualTableRoot");
    	StringBuilder rs=new StringBuilder();
    	for(int i=0;i<rulePieces.size();i++){
    		List<String> rulePieces2=TStringUtil.split(rulePieces.get(i), "com.taobao.tddl.rule.config.TableRule");
    		StringBuilder ts=new StringBuilder();
    		for(int j=0;j<rulePieces2.size();j++){
    			ts.append(rulePieces2.get(j));
    			if(j<(rulePieces2.size()-1)){
        			ts.append("com.taobao.tddl.interact.rule.TableRule");
    			}
    		}
    		
    		rs.append(ts);
    		if(i<(rulePieces.size()-1)){
    			rs.append("com.taobao.tddl.interact.rule.VirtualTableRoot");
    		}
    	}
    	
    	return rs.toString();
    }
    
    
    public static void main(String[] args){
    	String rule="<?xml version=\"1.0\" encoding=\"gb2312\"?>"+
		"<!DOCTYPE beans PUBLIC \"-//SPRING//DTD BEAN//EN\" \"http://www.springframework.org/dtd/spring-beans.dtd\">"+
		"<beans>"+
		   "<bean id=\"vtabroot\" class=\"com.taobao.tddl.rule.le.VirtualTableRoot\""+
		      "init-method=\"init\">"+
		      "<property name=\"dbType\" value=\"MYSQL\" />"+
		      "<property name=\"tableRules\">"+
		         "<map>"+
		            "<entry key=\"nserch\" value-ref=\"nserch\" />"+
		         "</map>"+
		      "</property>"+
		   "</bean>"+
		
		   "<bean id=\"nserch\" class=\"com.taobao.tddl.rule.le.TableRule\">"+
		      "<property name=\"tbRuleArray\">"+
		         "<value>\"nserch_\"+week(#gmt_create,1_date,7#)%7+\"_\"+Math.abs(#message_id,24#.hashCode()%24)</value>"+
		      "</property>"+
		      "<property name=\"dbRuleArray\">"+
		          "<value>\"NSEARCH_GROUP_\"+(Math.abs(#message_id,3#.hashCode() % 3)+1)</value>"+
		       "</property>"+
		   "</bean>"+
		"</beans>";
    	
    	System.out.println(convertLeAndOriRuleStr2InteractRule(rule));
    	
    	String rule2="<?xml version=\"1.0\" encoding=\"gb2312\"?>"+
    	"<!DOCTYPE beans PUBLIC \"-//SPRING//DTD BEAN//EN\" \"http://www.springframework.org/dtd/spring-beans.dtd\">"+
    	"<beans>"+
    	   "<bean id=\"vtabroot\" class=\"com.taobao.tddl.rule.VirtualTableRoot\""+
    	      "init-method=\"init\">"+
    	      "<property name=\"dbType\" value=\"MYSQL\" />"+
    	      "<property name=\"tableRules\">"+
    	         "<map>"+
    	            "<entry key=\"nserch\" value-ref=\"nserch\" />"+
    	         "</map>"+
    	      "</property>"+
    	   "</bean>"+

    	   "<bean id=\"nserch\" class=\"com.taobao.tddl.rule.config.TableRule\">"+
    	      "<property name=\"tbRuleArray\">"+
    	         "<value>\"nserch_\"+week(#gmt_create,1_date,7#)%7+\"_\"+Math.abs(#message_id,24#.hashCode()%24)</value>"+
    	      "</property>"+
    	      "<property name=\"dbRuleArray\">"+
    	          "<value>\"NSEARCH_GROUP_\"+(Math.abs(#message_id,3#.hashCode() % 3)+1)</value>"+
    	       "</property>"+
    	   "</bean>"+
    	"</beans>";
    	
    	System.out.println(convertLeAndOriRuleStr2InteractRule(rule2));
    	
    }
}
