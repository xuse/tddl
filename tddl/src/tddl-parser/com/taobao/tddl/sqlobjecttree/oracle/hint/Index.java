package com.taobao.tddl.sqlobjecttree.oracle.hint;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.alibaba.common.lang.StringUtil;
import com.taobao.tddl.sqlobjecttree.HintSetter;
import com.taobao.tddl.sqlobjecttree.IndexWrapper;

public class Index implements HintSetter {
	List<String> args=Collections.emptyList();
	public List<String> getArguments() {
		return args;
	}

	public void appendSQL(StringBuilder sb) {
		sb.append("INDEX").append("(");
		boolean firstElement=true;
		for(String str:args){
			if(firstElement){
				firstElement=false;
			}else{
				sb.append(",");
			}
			sb.append(str);
		}
		sb.append(")");
	} 

	public StringBuilder regTableModifiable(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		//这里实现比较诡异，因为regTableModifiable这个方法涉及了太多类需要变动
		//为了这一个index变动去修改整个架构得不偿失。所以暂时采用一种临时的方式来处理这个问题
		//直接在ChangeMethodCommon中添加了需要替换的index方法。
		sb.append("INDEX").append("(");
//		boolean firstElement=true;
		if(args.size()!=2){
			throw new IllegalArgumentException("index hint暂时只支持两个参数");
		}

		if(oraTabName.contains(StringUtil.trim(args.get(0)))){
			list.add(sb.toString());
			IndexWrapper wa=new IndexWrapper();
			wa.setOriginalTableName(args.get(0));
			list.add(wa);
			sb = new StringBuilder();
		}else{
			sb.append(args.get(0));
		}
		sb.append(",");
		//第二个参数在参数内可能会带有需要替换的表名，并且不能完全匹配,所以要做遍历匹配
		String dbIndex = args.get(1);
		int position = 0;
		for(String oraTable: oraTabName){
			int lastIndex = StringUtil.lastIndexOf(dbIndex, oraTable);
			if(-1 != lastIndex){
				//
				if(0 != lastIndex ){
					sb.append(StringUtil.substring(dbIndex, 0,lastIndex));
					position += lastIndex; 
				}
				//表示match了,并且position就等于lastIndex的位置
				//position += dbIndex.length();
				position += oraTable.length();
				
				list.add(sb.toString());
				IndexWrapper wa=new IndexWrapper();
				wa.setOriginalTableName(oraTable);
				list.add(wa);
				sb = new StringBuilder();
				sb.append(StringUtil.substring(dbIndex, position));
				break;
			}
		}
//		list.add(sb.toString());
//		sb=new StringBuilder();
		sb.append(")");
		return sb;
	}

	public void addHint(List<String> args) {
		this.args=args;
	}

}
