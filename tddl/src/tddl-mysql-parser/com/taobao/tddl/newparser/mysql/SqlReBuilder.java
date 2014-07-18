package com.taobao.tddl.newparser.mysql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.expression.primary.ParamMarker;
import com.alibaba.cobar.parser.ast.fragment.Limit;
import com.alibaba.cobar.parser.ast.fragment.tableref.IndexHint;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableRefFactor;

public class SqlReBuilder extends OutputVisitor {
	private final Map<String, String> tableMap;
	private final Map<Integer,Object> changedParam=new HashMap<Integer, Object>();
	private final Number newSize;
	private final Number newSkip;

	public SqlReBuilder(StringBuilder appendable,
			Map<String, String> tableMap, Number newSize, Number newSkip) {
		super(appendable);
		this.tableMap = tableMap;
		this.newSize = newSize;
		this.newSkip = newSkip;
	}
	
	public Map<Integer,Object> getChangedParam(){
		return this.changedParam;
	}

	@Override
	public void visit(TableRefFactor node) {
		replaceTable(node.getTable());
		String alias = node.getAlias();
		if (alias != null) {
			appendable.append(" AS ").append(alias);
		}
		List<IndexHint> list = node.getHintList();
		if (list != null && !list.isEmpty()) {
			appendable.append(' ');
			printList(list, " ");
		}
	}

	private void replaceTable(Identifier table) {
		Expression parent = table.getParent();
		if (parent != null) {
			parent.accept(this);
			appendable.append('.');
		}
		
		String tableName=this.tableMap.get(table.getIdTextUpUnescape());
		if(tableName!=null){
			appendable.append(tableName);
		}else{
			appendable.append(table.getIdText());
		}
	}

	@Override
	public void visit(Limit node) {
		appendable.append("LIMIT ");
        Object offset = node.getOffset();
        if (offset instanceof ParamMarker) {
        	changedParam.put(((ParamMarker) offset).getParamIndex()-1,newSkip);
        	appendable.append("?");
        } else {
            appendable.append(String.valueOf(newSkip));
        }
        appendable.append(", ");
        Object size = node.getSize();
        if (size instanceof ParamMarker) {
        	changedParam.put(((ParamMarker) size).getParamIndex()-1,newSize);
        	appendable.append("?");
        } else {
            appendable.append(String.valueOf(newSize));
        }
	}
}
