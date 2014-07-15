package com.taobao.tddl.sqlobjecttree;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.common.lang.StringUtil;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.interact.sqljep.ComparativeAND;
import com.taobao.tddl.interact.sqljep.ComparativeOR;
import com.taobao.tddl.sqlobjecttree.common.ComparableElement;

/**
 * Visitor，主要保存了一个ComparativeMap.
 * 
 * 解释见{@link Expression}的eval方法
 * 
 * @author shenxun
 *
 */
public class RowJepVisitor{
	private Map<String, Comparative> comparable=new HashMap<String,Comparative>();

//	private List<Object> args=Collections.emptyList();
	/**
	 * 将expression 的 key和对应的value放到Comparative中。
	 * 要解决的一个问题是，因为在遍历Expression的过程中，遍历的过程是Expression树的后序遍历，所以遍历是从子节点向
	 * 父节点一层一层的进行分析的，这样就会出现一个问题，必须在一个地方保存所有的and节点和or节点信息，将其全部
	 * 拼入到访问树中才不会引起错误。
	 *
	 * 因此在expression的eval方法里带有了一个boolean值来标识正在被处理的某个子节点的父节点到底是一个and节点还是一个or
	 * 的节点，在RowJepVisitor中则根据and节点和or节点的不同，构造了不同的ComparativeAnd或comparativeOr来与之对应。
	 *
	 * 
	 * @param key
	 * @param ele
	 */
	public void put(String key,ComparableElement ele){
		Comparative val=comparable.get(key);
		if(val==null){
			if(ele.comp instanceof ComparativeOR){
				comparable.put(key, (ComparativeOR)ele.comp);
			}else{
				comparable.put(key, new Comparative(ele.operator,ele.comp));
			}
		}else{
			if(ele.isAnd){
				ComparativeAND and=new ComparativeAND();
				and.addComparative(val);
				if(ele.comp instanceof ComparativeOR){
				and.addComparative((ComparativeOR)ele.comp);	
				}else{
				and.addComparative(new Comparative(ele.operator,ele.comp));
				}
				comparable.put(key, and);
			}else{
				//实际的OR节点并未使用这个选择支
				//本选择支只用于in等这类要做OR语义转译的Expression使用
				ComparativeOR or=new ComparativeOR();

				or.addComparative(val);
				or.addComparative(new Comparative(ele.operator,ele.comp));
				comparable.put(key, or);
			}
		}
	}
	public Comparative get(String key){
		return comparable.get(StringUtil.toUpperCase(key));
	}
	public Map<String, Comparative> getComparable() {
		return comparable;
	}
	public void setComparable(Map<String, Comparative> comparable) {
		this.comparable = comparable;
	}
//	public List<Object> getArgs() {
//		return args;
//	}
//	public void setArgs(List<Object> args) {
//		this.args = args;
//	}
}
