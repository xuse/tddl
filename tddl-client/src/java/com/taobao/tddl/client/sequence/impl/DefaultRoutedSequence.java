package com.taobao.tddl.client.sequence.impl;

import org.dom4j.DocumentException;

import com.taobao.tddl.client.sequence.RoutedSequence;
import com.taobao.tddl.client.sequence.Sequence;
import com.taobao.tddl.client.sequence.exception.SequenceException;
import com.taobao.tddl.common.sequence.Config;
import com.taobao.tddl.common.sequence.Config.ConfigException;
import com.taobao.tddl.common.sequence.Config.Route;
import com.taobao.tddl.common.sequence.Config.Route.Expression;

/**
 * 带路由信息序列默认实现
 *
 * @author guangxia
 *
 * @param <DatabaseRouteType> 数据库路由信息类型
 * @param <TableRouteType> 表路由信息类型
 */
public class DefaultRoutedSequence<DatabaseRouteType, TableRouteType> implements RoutedSequence<DatabaseRouteType, TableRouteType> {
	private final static long[] pow10 = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
	10000000000L, 100000000000L, 1000000000000L, 10000000000000L, 100000000000000L, 1000000000000000L, 10000000000000000L,
	100000000000000000L, 1000000000000000000L};

	private Config config;
	private long mulDatabase;
	private long mulRoute;
	private int routeSize;
	private Route databaseRoute;
	private Route tableRoute;
	private Expression<DatabaseRouteType> databaseExpression;
	private Expression<TableRouteType> tableExpression;

	private Sequence sequence;

	@SuppressWarnings("unchecked")
	public DefaultRoutedSequence(String path, String id, Sequence sequence) throws SequenceException {
		try {
			this.config = new Config.Factory(path).newInstance(id);
		} catch (DocumentException e) {
			throw new SequenceException(e);
		} catch (ConfigException e) {
			throw new SequenceException(e);
		}

		databaseRoute = config.getDatabaseRoute();
		if (databaseRoute != null) {
			databaseExpression = (Expression<DatabaseRouteType>) databaseRoute.getExpression();
			routeSize += databaseRoute.getSize();
		}

		tableRoute = config.getTableRoute();
		if (tableRoute != null) {
			tableExpression = (Expression<TableRouteType>) tableRoute.getExpression();
			routeSize += tableRoute.getSize();
			mulDatabase = pow10[tableRoute.getSize()];
		}

		if (routeSize != 0) {
			if (config.isPositionRight() == false) {
				mulRoute = pow10[19 - routeSize];
			} else {
				mulRoute = pow10[routeSize];
			}
		}

		this.setSequence(sequence);
	}

	public DefaultRoutedSequence(String path, String id) throws SequenceException {
		this(path, id, null);
	}

	public long nextValue(DatabaseRouteType databaseRoute, TableRouteType tableRoute) throws SequenceException {
		int routeBits = 0;
		int tmp;

		if (databaseExpression != null) {
			tmp = databaseExpression.execute(databaseRoute);
			if (tmp >= pow10[config.getDatabaseRoute().getSize()]) {
				throw new SequenceException("Overflow");
			}
			routeBits = tmp;
		}

		if (tableExpression != null) {
			if (routeBits != 0) {
				routeBits *= mulDatabase;
			}
			tmp = tableExpression.execute(tableRoute);
			if (tmp >= pow10[config.getTableRoute().getSize()]) {
				throw new SequenceException("Overflow");
			}
			routeBits += tmp;
		}

		long nextId = sequence.nextValue();
		if (routeSize != 0) {
			if (config.isPositionRight()) {
				nextId = nextId * mulRoute + routeBits;
			} else {
				nextId += routeBits * mulRoute;
			}
		}

		return nextId;
	}

	public void setSequence(Sequence sequence) {
		this.sequence = sequence;
	}

	public Sequence getSequence() {
		return sequence;
	}
}
