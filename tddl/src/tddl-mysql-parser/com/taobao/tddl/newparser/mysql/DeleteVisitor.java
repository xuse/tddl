package com.taobao.tddl.newparser.mysql;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLDeleteStatement;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.newparser.mysql.parserresult.MySqlStandardParserResult;
import com.taobao.tddl.sqlobjecttree.common.TableNameImp;

public class DeleteVisitor extends EmptySQLASTVisitor {
	private MySqlStandardParserResult result;

	public DeleteVisitor(MySqlStandardParserResult result) {
		this.result = result;
	}

	@Override
	public void visit(DMLDeleteStatement node) {
		if (node.getTableNames().size() == 1) {
			this.handleTableName(node);
		} else {
			throw new RuntimeException("not support multi table delete");
		}

		Expression expr = node.getWhereCondition();
		if (expr != null) {
			ExprVisitor mv = new ExprVisitor(this.result);
			expr.accept(mv);
		}
	}

	private void handleTableName(DMLDeleteStatement node) {
		TableNameImp tbName = new TableNameImp();
		// 这里有alias和schema的问题，FIXME
		tbName.setTablename(node.getTableNames().get(0).getIdTextUpUnescape());
		tbName.setAlias(null);
		if (node.getTableNames().get(0).getParent() != null) {
			tbName.setSchemaName(node.getTableNames().get(0).getParent()
					.getIdTextUpUnescape());
		}

		result.addTableName(tbName);
	}

	public MySqlStandardParserResult getResult() {
		return result;
	}
}
