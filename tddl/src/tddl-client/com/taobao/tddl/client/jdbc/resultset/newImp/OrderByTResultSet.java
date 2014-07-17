package com.taobao.tddl.client.jdbc.resultset.newImp;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.taobao.tddl.client.jdbc.ConnectionManager;
import com.taobao.tddl.client.jdbc.OrderByColumn;
import com.taobao.tddl.client.jdbc.TStatementImp;
import com.taobao.tddl.client.jdbc.executeplan.ExecutionPlan;
import com.taobao.tddl.client.jdbc.resultset.helper.ComparatorRealizer;
import com.taobao.tddl.client.jdbc.resultset.helper.ComparatorRealizer.BothNullComparator;
import com.taobao.tddl.client.jdbc.sqlexecutor.RealSqlExecutor;

/**
 * @author guangxia
 * @author junyu modify the Comparator
 */
public class OrderByTResultSet extends BaseTResultSet {
	private int needNext = -1;
	private boolean inited;

	private OrderByColumn[] orderByColumns;
	private int[] sortIndexes;
	private SortedSet<Integer> order;
	private Comparator<Integer> setComparator;
	private List<Comparator<Object>> sortFieldComparators;

	public OrderByTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,
			RealSqlExecutor realSqlExecutor) throws SQLException {
		super(tStatementImp, connectionManager, executionPlan, realSqlExecutor);
	}

	public OrderByTResultSet(TStatementImp tStatementImp,
			ConnectionManager connectionManager, ExecutionPlan executionPlan,
			RealSqlExecutor realSqlExecutor, boolean init) throws SQLException {
		super(tStatementImp, connectionManager, executionPlan, realSqlExecutor,
				init);
	}

	@Override
	protected boolean internNext() throws SQLException {
		if (!inited) {
			inited = true;
			reduce();
		}
		try {
			if (needNext != -1) {
				if (actualResultSets.get(needNext).next()) {
					order.add(needNext);
				}
			}
			if (order.isEmpty()) {
				return false;
			}

			Integer first = order.first();

			/**
			 * add by junyu
			 */
			super.currentResultSet = actualResultSets.get(first);

			order.remove(first);
			needNext = first;
			limitTo--;
		} catch (RuntimeException exp) {
			Throwable cause = exp.getCause();
			if (cause instanceof SQLException) {
				throw (SQLException) cause;
			} else if (cause instanceof CompareTypeUnsupported) {
				SQLException sqlException = new SQLException(cause.toString());
				sqlException.setStackTrace(cause.getStackTrace());
				throw sqlException;
			} else {
				throw exp;
			}
		}
		return true;
	}

	protected ResultSet reduce() throws SQLException {
		if (actualResultSets.size() == 0) {
			throw new RuntimeException("This should not happen!!");
		}
		if (actualResultSets.size() == 1) {
			return actualResultSets.get(0);
		}

		initSort();
		skipLimitFrom();
		return null;
	}

	protected void initSort() throws SQLException {
		sortIndexes = new int[orderByColumns.length];
		for (int i = 0; i < sortIndexes.length; i++) {
			sortIndexes[i] = -1;
		}

		sortFieldComparators = new ArrayList<Comparator<Object>>(
				orderByColumns.length);
		for (int i = 0; i < orderByColumns.length; i++) {
			sortFieldComparators.add(null);
		}

		// TODO:
		setComparator = new Comparator<Integer>() {
			public int compare(Integer left, Integer right) {
				ResultSet resultSet1, resultSet2;
				resultSet1 = actualResultSets.get(left);
				resultSet2 = actualResultSets.get(right);

				int ret;

				for (int indexOfOrderByColumn = 0; indexOfOrderByColumn < orderByColumns.length; indexOfOrderByColumn++) {
					try {
						if (sortIndexes[indexOfOrderByColumn] == -1) {
							sortIndexes[indexOfOrderByColumn] = actualResultSets
									.get(0)
									.findColumn(
											orderByColumns[indexOfOrderByColumn]
													.getColumnName());
						}
						final int sortIndex = sortIndexes[indexOfOrderByColumn];

						Comparator<Object> sortFieldComparator = sortFieldComparators
								.get(indexOfOrderByColumn);
						if (sortFieldComparator == null||sortFieldComparator instanceof BothNullComparator) {
							Object o1 = resultSet1.getObject(sortIndex);
							if (null == o1) {
								o1 = resultSet2.getObject(sortIndex);
							}

							if (null == o1) {
								sortFieldComparator = ComparatorRealizer
										.getBothNullComparator();
							} else {
								Class<?> sortType = o1.getClass();
								sortFieldComparator = ComparatorRealizer
										.getObjectComparator(sortType);
							}
							
							if (null == sortFieldComparator) {
								throw new RuntimeException(
										new CompareTypeUnsupported(
												"unsupported compare type!"));
							}

							sortFieldComparators.set(indexOfOrderByColumn,
									sortFieldComparator);
						}

						ret = sortFieldComparator.compare(
								resultSet1.getObject(sortIndex),
								resultSet2.getObject(sortIndex));
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}

					if (ret == 0 && resultSet1 != resultSet2) {
						continue;
					}
					if (orderByColumns[indexOfOrderByColumn].isAsc()) {
						return ret;
					} else {
						return -ret;
					}
				}

				/*
				 * 由于TreeSet不允许存在相同的对象，所以利用hashCode把相同的对象区分开
				 * 如果存在hashCode也相同的2个对象，那他们的顺序是无关紧要的
				 */
				return System.identityHashCode(resultSet1) < System
						.identityHashCode(resultSet2) ? -1 : 1;
			}

		};

		try {
			order = new TreeSet<Integer>(setComparator);
			for (int i = 0; i < actualResultSets.size(); i++) {
				if (actualResultSets.get(i).next()) {
					order.add(i);
				}
			}
		} catch (RuntimeException exp) {
			Throwable cause = exp.getCause();
			if (cause instanceof SQLException) {
				throw (SQLException) cause;
			} else if (cause instanceof CompareTypeUnsupported) {
				SQLException sqlException = new SQLException(cause.toString());
				sqlException.setStackTrace(cause.getStackTrace());
				throw sqlException;
			} else {
				throw exp;
			}
		}
	}

	protected void skipLimitFrom() throws SQLException {
		for (int i = 0; i < limitFrom; i++) {
			next();
		}
	}

	public void setOrderByColumns(OrderByColumn[] orderByColumns) {
		this.orderByColumns = orderByColumns;
	}
}
