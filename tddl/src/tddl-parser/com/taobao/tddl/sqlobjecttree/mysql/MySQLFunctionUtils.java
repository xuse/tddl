package com.taobao.tddl.sqlobjecttree.mysql;

import java.util.Calendar;
import java.util.Date;

public class MySQLFunctionUtils {
	private static final int BASE_YEAR = 1970;
	private static final int BASE_MONTH = Calendar.JANUARY;
	private static final int BASE_DAY = 1;

	/**
	 * days elapsed since 1970-01-01, mysql to_days('1970-01-01')
	 */
	private static final int BASE_DAYS_OFFSET = 719528;

	/**
	 * In accordance with mysql function from_days
	 *
	 * @param days
	 * @return
	 */
	public static Date fromDays(int days) {
		if (days < BASE_DAYS_OFFSET) {
			throw new IllegalArgumentException("days cannot be less than " + BASE_DAYS_OFFSET + " (mysql to_days('1970-01-01'))");
		}

		Calendar cal = Calendar.getInstance();
		cal.set(BASE_YEAR, BASE_MONTH, BASE_DAY, 0, 0, 0);
		cal.set(Calendar.MILLISECOND, 0);

		cal.add(Calendar.DAY_OF_MONTH, days - BASE_DAYS_OFFSET);

		return cal.getTime();
	}

	/**
	 * In accordance with mysql function to_days
	 *
	 * @param date
	 * @return
	 */
	public static int toDays(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET));

		return (int) (cal.getTimeInMillis() / 1000 / 60 / 60 / 24) + BASE_DAYS_OFFSET;
	}
}
