package com.taobao.tddl.sqlobjecttree;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.sqlobjecttree.Column;
import com.taobao.tddl.common.sqlobjecttree.SQLFragment;

/**
 * 用于标注join语句。 对于inner join ,实际上有两种写法。 一种是使用标准的join语句。 另外一种则是使用 id = id 的方式来进行。
 * 
 * 这里的对象是针对第一种类型的sql进行的。
 * 
 * @author whisper select * from tab a inner(left,right..etc) join tab2 as b on
 *         a.id = b.id where user_id = ? 这里用于记录join tab2 as b on a.id = b.id
 *         这个字段。
 * 
 *         为了方便起见，这个数据字段还是跟着表名走比较方便一些
 */
public class JoinClause implements SQLFragment {

	/**
	 * 用于标注表名
	 */
	private TableName tableName;

	private Column leftCondition;

	private Column rightCondition;

	private JOIN_TYPE joinType;

	public void appendSQL(StringBuilder sb) {
		sb.append(" ");
		if (joinType != null) {
			if (joinType.equals(JOIN_TYPE.LEFT_OUTER)) {
				sb.append("LEFT OUTER");
			} else if (joinType.equals(JOIN_TYPE.RIGHT_OUTER)) {
				sb.append("RIGHT OUTER");
			} else if (joinType.equals(JOIN_TYPE.FULL_OUTER)) {
				sb.append("FULL OUTER");
			} else {
				sb.append(joinType.toString());
			}
			sb.append(" ");
		}
		sb.append("JOIN ");
		tableName.appendSQL(sb);
		sb.append(" ").append("ON").append(" ");
		if (leftCondition == null || rightCondition == null) {
			throw new IllegalArgumentException("condition should not be null");
		}
		leftCondition.appendSQL(sb);
		sb.append(" = ");
		rightCondition.appendSQL(sb);

	}

	public TableName getTableName() {
		return tableName;
	}

	public void setTableName(TableName tableName) {
		this.tableName = tableName;
	}

	public Column getLeftCondition() {
		return leftCondition;
	}

	public void setLeftCondition(Column leftCondition) {
		this.leftCondition = leftCondition;
	}

	public Column getRightCondition() {
		return rightCondition;
	}

	public void setRightCondition(Column rightCondition) {
		this.rightCondition = rightCondition;
	}

	public JOIN_TYPE getJoinType() {
		return joinType;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof JoinClause)) {
			return false;
		}
		JoinClause join = (JoinClause) obj;
		return join.getJoinType().equals(joinType)
				&& join.getTableName().equals(tableName)
				&& join.getLeftCondition().equals(leftCondition)
				&& join.getRightCondition().equals(rightCondition);

	}

	public void setJoinType(JOIN_TYPE joinType) {
		this.joinType = joinType;
	}

	public void appendAliasToSQLMap(Map<String, SQLFragment> map) {
		// 只有表名那个地方是允许指定别名的
		if (tableName != null) {
			tableName.appendAliasToSQLMap(map);
		}
	}

	public StringBuilder regTableModifiable(Set<String> logicTableNames,
			List<Object> list, StringBuilder sb) {
		sb.append(" ");
		if (joinType != null) {
			if (joinType.equals(JOIN_TYPE.LEFT_OUTER)) {
				sb.append("LEFT OUTER");
			} else if (joinType.equals(JOIN_TYPE.RIGHT_OUTER)) {
				sb.append("RIGHT OUTER");
			} else if (joinType.equals(JOIN_TYPE.FULL_OUTER)) {
				sb.append("FULL OUTER");
			} else {
				sb.append(joinType.toString());
			}
			sb.append(" ");
		}
		sb.append("JOIN ");
		tableName.regTableModifiable(logicTableNames, list, sb);
		sb.append(" ").append("ON").append(" ");
		if (leftCondition == null || rightCondition == null) {
			throw new IllegalArgumentException("condition should not be null");
		}
		leftCondition.regTableModifiable(logicTableNames, list, sb);
		sb.append(" = ");
		rightCondition.regTableModifiable(logicTableNames, list, sb);
		return sb;
	}

}
