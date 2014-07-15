package com.taobao.tddl.sqlobjecttree.common.value;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.Column;
import com.taobao.tddl.sqlobjecttree.Function;
import com.taobao.tddl.sqlobjecttree.Utils;

public abstract class OperationBeforOneOrTwoArgsFunction implements Function {
	protected Object arg1;
	protected Object arg2;

	public void setValue(List<Object> values) {
		if (values.size() == 1) {
			arg1 = values.get(0);
		} else if (values.size() == 2) {
			arg1 = values.get(0);
			arg2 = values.get(1);
		} else {
			throw new IllegalArgumentException("参数多于2个");
		}
	}

	public final Comparable<?> eval() {
		return this;
	}

	public String getNestedColName() {
		String ret = null;

		if (arg1 instanceof Column) {
			ret = ((Column) arg1).getColumn();
		}
		if (arg2 instanceof Column) {
			if (ret == null) {
				ret = ((Column) arg2).getColumn();
			} else {
				throw new IllegalArgumentException("双参数函数中不能出现多个列名的函数");
			}
		}
		return ret;
	}

	public abstract String getFuncName();

	public void appendSQL(StringBuilder sb) {
		sb.append(getFuncName());
		sb.append("(");
		Utils.appendSQLList(arg1, sb);
		if (arg2 != null) {
			sb.append(",");
			Utils.appendSQLList(arg2, sb);
		}
		sb.append(")");
	}

	public StringBuilder regTableModifiable(Set<String> oraTabName,
			List<Object> list, StringBuilder sb) {
		sb.append(getFuncName());
		sb.append("(");
		sb = Utils.appendSQLListWithList(oraTabName, arg1, sb, list);
		if (arg2 != null) {
			sb.append(",");
			sb = Utils.appendSQLListWithList(oraTabName, arg2, sb, list);
		}
		sb.append(")");
		return sb;

	}

	public Comparable<?> getVal(List<Object> args) {
		return UnknowValueObject.getUnknowValueObject();
	}

	public int compareTo(Object o) {
		throw new IllegalStateException("should not be here");
	}
}
