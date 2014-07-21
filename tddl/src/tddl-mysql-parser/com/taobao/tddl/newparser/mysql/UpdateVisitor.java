package com.taobao.tddl.newparser.mysql;

import java.util.List;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReference;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLUpdateStatement;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.newparser.mysql.parserresult.MySqlStandardParserResult;

public class UpdateVisitor extends EmptySQLASTVisitor {
	private MySqlStandardParserResult result;

	public UpdateVisitor(MySqlStandardParserResult result) {
		this.result = result;
	}

	@Override
	public void visit(DMLUpdateStatement node) {
		this.handleTableName(node);

//		set 的expr不需要让tddl知道
//		List<Pair<Identifier, Expression>> cvs = node.getValues();
//		for (int i = 0; i < cvs.size(); i++) {
//			Pair<Identifier, Expression> p = cvs.get(i);
//			MySqlExprVisitor mv = new MySqlExprVisitor(this.result);
//			p.getValue().accept(mv);
//			Object obj = mv.getColumnOrValue();
//			Comparable c = null;
//			if (obj instanceof ParamMarker) {
//				c = new BindVar(((ParamMarker) obj).getParamIndex() - 1);
//			} else {
//				c = (Comparable) obj;
//			}
//			Comparative com = new Comparative(Comparative.Equivalent, c);
//			this.result.addColumn(p.getKey().getIdTextUpUnescape(), com);
//		}

		Expression expr = node.getWhere();
		if (expr != null) {
			queryCondition(expr);
		}
	}

	private void handleTableName(DMLUpdateStatement node) {
		List<TableReference> trs = node.getTableRefs().getTableReferenceList();
		if(trs.size()==1){
			ExprVisitor mtv = new ExprVisitor(this.result);
			trs.get(0).accept(mtv);
		}else{
			throw new RuntimeException("not support update more than one table!");
		}
	}

	private void queryCondition(Expression expr) {
		ExprVisitor mv = new ExprVisitor(this.result);
		expr.accept(mv);
	}

	public MySqlStandardParserResult getResult() {
		return result;
	}
}
