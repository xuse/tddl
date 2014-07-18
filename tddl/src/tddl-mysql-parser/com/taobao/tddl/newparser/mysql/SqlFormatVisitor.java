package com.taobao.tddl.newparser.mysql;

import java.util.List;

import com.alibaba.cobar.parser.ast.ASTNode;
import com.alibaba.cobar.parser.ast.expression.misc.InExpressionList;
import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.expression.primary.ParamMarker;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableRefFactor;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLDeleteStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLInsertStatement;
import com.taobao.tddl.common.channel.impl.SqlMetaDataImpl;

public class SqlFormatVisitor extends OutputVisitor {

	private SqlMetaDataImpl sqlMetaData;

	public SqlFormatVisitor(SqlMetaDataImpl sqlMetaData, String oriSql) {
		super(sqlMetaData.getSqlBuilder(), null);
		sqlMetaData.setParsed(true);
		sqlMetaData.setOriSql(oriSql);
		this.sqlMetaData = sqlMetaData;
	}

	@Override
	public void visit(DMLDeleteStatement node) {
		sqlMetaData.addLogicTables(node.getTableNames().get(0).getIdText());
		super.visit(node);
	}
	
	@Override
	public void visit(DMLInsertStatement node) {
		sqlMetaData.addLogicTables(node.getTable().getIdText());
		super.visit(node);
	}

	@Override
	public void visit(TableRefFactor node) {
		Identifier table = node.getTable();
		sqlMetaData.addLogicTables(table.getIdText());
		super.visit(node);
	}

	@Override
	public void visit(InExpressionList node) {
		appendable.append('(');
		printInList(node.getList());
		appendable.append(')');
	}

	private void printInList(List<? extends ASTNode> list) {
		if (allParamMarker(list))
			printOneMarker(list);
		else
			printList(list);
	}

	private void printOneMarker(List<? extends ASTNode> list) {
		list.get(0).accept(this);
	}

	protected boolean allParamMarker(List<? extends ASTNode> list) {
		boolean result = true;
		for (ASTNode node : list) {
			if (node instanceof ParamMarker)
				continue;
			result = false;
			break;
		}
		return result && list.size() > 0;
	}

	@Override
	public void visit(ParamMarker node) {
		appendable.append('?');
		appendArgsIndex(node.getParamIndex() - 1);
	}

}
