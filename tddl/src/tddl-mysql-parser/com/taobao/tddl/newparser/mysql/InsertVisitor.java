package com.taobao.tddl.newparser.mysql;

import java.util.List;

import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.expression.primary.RowExpression;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLInsertStatement;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.newparser.mysql.parserresult.MySqlStandardParserResult;
import com.taobao.tddl.sqlobjecttree.common.TableNameImp;

public class InsertVisitor extends EmptySQLASTVisitor {
	private MySqlStandardParserResult result;

	public InsertVisitor(MySqlStandardParserResult result) {
		this.result = result;
	}

	@Override
	public void visit(DMLInsertStatement node) {
		this.handleTableName(node);
		List<RowExpression> exprList = node.getRowList();
		if (exprList != null && exprList.size() == 1) {
			handleRow(node);
		} else {
			throw new RuntimeException("could not support multi row values.");
		}
	}

	private void handleTableName(DMLInsertStatement node) {
		TableNameImp tbName = new TableNameImp();
		tbName.setTablename(node.getTable().getIdTextUpUnescape());
		tbName.setAlias(null);
		if (node.getTable().getParent() != null) {
			tbName.setSchemaName(node.getTable().getParent()
					.getIdTextUpUnescape());
		}
		result.addTableName(tbName);
	}

	private void handleRow(DMLInsertStatement node) {
		List<Identifier> columnNames = node.getColumnNameList();
		RowExpression row = node.getRowList().get(0);
		if (columnNames != null
				&& (columnNames.size() == row.getRowExprList().size())) {
			for (int i = 0; i < columnNames.size(); i++) {
				ExprVisitor vi = new ExprVisitor(this.result);
				row.getRowExprList().get(i).accept(vi);
				Object obj = vi.getColumnOrValue();
				Comparative com = ExprVisitor.convertComparative(obj,
						Comparative.Equivalent);
				this.result.addColumn(columnNames.get(i).getIdTextUpUnescape(),
						com);
			}
		} else {
			throw new RuntimeException(
					"column number different from value number!");
		}
	}

	public MySqlStandardParserResult getResult() {
		return result;
	}
}
