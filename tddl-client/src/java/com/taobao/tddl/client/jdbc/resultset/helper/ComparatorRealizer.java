package com.taobao.tddl.client.jdbc.resultset.helper;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * @author junyu
 * 
 */
public class ComparatorRealizer {
	private static final String BOTH_NULL = "both_null";
	private static Map<String, Comparator<Object>> comparators = new HashMap<String, Comparator<Object>>();
	static {
		comparators.put(Integer.class.getName(), new IntegerComparator());
		comparators.put(BigDecimal.class.getName(), new BigDecimalComparator());
		comparators.put(String.class.getName(), new StringComparator());
		comparators.put(java.sql.Timestamp.class.getName(),
				new TimestampComparator());
		comparators.put(Short.class.getName(), new ShortComparator());
		comparators.put(Long.class.getName(), new LongComparator());
		comparators.put(BigInteger.class.getName(), new BigIntegerComparator());
		comparators.put(Float.class.getName(), new FloatComparator());
		comparators.put(Double.class.getName(), new DoubleComparator());
		comparators.put(Byte.class.getName(), new ByteComparator());
		comparators.put(Boolean.class.getName(), new BooleanComparator());
		comparators.put(java.sql.Date.class.getName(), new DateComparator());
		comparators.put(java.sql.Time.class.getName(), new TimeComparator());
		comparators.put(BOTH_NULL, new BothNullComparator());
	}

	@SuppressWarnings("rawtypes")
	public static Comparator<Object> getObjectComparator(Class clazz) {
		return comparators.get(clazz.getName());
	}

	public static Comparator<Object> getBothNullComparator() {
		return comparators.get(BOTH_NULL);
	}

	@SuppressWarnings("serial")
	public static class BothNullComparator implements Comparator<Object>,
			Serializable {
		@Override
		public int compare(Object o1, Object o2) {
			return 0;
		}

	}

	private static class IntegerComparator implements Comparator<Object>,
			Serializable {
		private static final long serialVersionUID = 5667463144686630897L;

		public int compare(Object r1, Object r2) {
			if (r1 != null && r2 != null) {
				int l = (Integer) r1;
				int r = (Integer) r2;
				return l == r ? 0 : (l < r ? -1 : 1);
			} else if (r1 == null && r2 != null) {
				int r = (Integer) r2;
				return 0 < r ? -1 : 1;
			} else if (r1 != null && r2 == null) {
				int l = (Integer) r1;
				return l < 0 ? -1 : 1;
			} else {
				return 0;
			}
		}
	}

	private static class BigDecimalComparator implements Comparator<Object>,
			Serializable {
		private static final long serialVersionUID = 1935143297506831500L;

		public int compare(Object r1, Object r2) {
			if (r1 != null && r2 != null) {
				BigDecimal b1 = (BigDecimal) r1;
				BigDecimal b2 = (BigDecimal) r2;
				return b1.compareTo(b2);
			} else if (r1 == null && r2 != null) {
				return -1;
			} else if (r1 != null && r2 == null) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	private static class StringComparator implements Comparator<Object>,
			Serializable {
		private static final long serialVersionUID = 2646282697658311223L;

		public int compare(Object r1, Object r2) {
			if (r1 != null && r2 != null) {
				String s1 = (String) r1;
				String s2 = (String) r2;
				return s1.compareTo(s2);
			} else if (r1 == null && r2 != null) {
				return -1;
			} else if (r1 != null && r2 == null) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	private static class TimestampComparator implements Comparator<Object>,
			Serializable {
		private static final long serialVersionUID = -1307150815524702537L;

		public int compare(Object r1, Object r2) {
			if (r1 != null && r2 != null) {
				java.sql.Timestamp t1 = (java.sql.Timestamp) r1;
				java.sql.Timestamp t2 = (java.sql.Timestamp) r2;
				return t1.compareTo(t2);
			} else if (r1 == null && r2 != null) {
				return -1;
			} else if (r1 != null && r2 == null) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	private static class ShortComparator implements Comparator<Object>,
			Serializable {
		private static final long serialVersionUID = 4816347996381592373L;

		public int compare(Object r1, Object r2) {
			if (r1 != null && r2 != null) {
				short s1 = (Short) r1;
				short s2 = (Short) r2;
				return s1 == s2 ? 0 : (s1 < s2 ? -1 : 1);
			} else if (r1 == null && r2 != null) {
				short s2 = (Short) r2;
				return 0 < s2 ? -1 : 1;
			} else if (r1 != null && r2 == null) {
				short s1 = (Short) r1;
				return s1 < 0 ? -1 : 1;
			} else {
				return 0;
			}
		}
	}
	
	private static class BigIntegerComparator implements Comparator<Object>,
			Serializable {
		
		private static final long serialVersionUID = -1599320826663707388L;

		public int compare(Object r1, Object r2) {
			if (r1 != null && r2 != null) {
				BigInteger b1 = (BigInteger) r1;
				BigInteger b2 = (BigInteger) r2;
				return b1.compareTo(b2);
			} else if (r1 == null && r2 != null) {
				BigInteger b2 = (BigInteger) r2;
				return b2.compareTo(new BigInteger("0")) > 0 ? -1 : 1;
			} else if (r1 != null && r2 == null) {
				BigInteger b1 = (BigInteger) r1;
				return b1.compareTo(new BigInteger("0"));
			} else {
				return 0;
			}
		}
	}

	private static class LongComparator implements Comparator<Object>,
			Serializable {
		private static final long serialVersionUID = 3715803260495696600L;

		public int compare(Object r1, Object r2) {
			if (r1 != null && r2 != null) {
				long l1 = (Long) r1;
				long l2 = (Long) r2;
				return l1 == l2 ? 0 : (l1 < l2 ? -1 : 1);
			} else if (r1 == null && r2 != null) {
				long l2 = (Long) r2;
				return 0 < l2 ? -1 : 1;
			} else if (r1 != null && r2 == null) {
				long l1 = (Long) r1;
				return l1 < 0 ? -1 : 1;
			} else {
				return 0;
			}
		}
	}

	private static class FloatComparator implements Comparator<Object>,
			Serializable {
		private static final long serialVersionUID = -7381280113304667070L;

		public int compare(Object r1, Object r2) {
			if (r1 != null && r2 != null) {
				float f1 = (Float) r1;
				float f2 = (Float) r2;
				return f1 == f2 ? 0 : (f1 < f2 ? -1 : 1);
			} else if (r1 == null && r2 != null) {
				float f2 = (Float) r2;
				return 0 < f2 ? -1 : 1;
			} else if (r1 != null && r2 == null) {
				float f1 = (Float) r1;
				return f1 < 0 ? -1 : 1;
			} else {
				return 0;
			}
		}
	}

	private static class DoubleComparator implements Comparator<Object>,
			Serializable {
		private static final long serialVersionUID = 1309653334825180274L;

		public int compare(Object r1, Object r2) {
			if (r1 != null && r2 != null) {
				double d1 = (Double) r1;
				double d2 = (Double) r2;
				return d1 == d2 ? 0 : (d1 < d2 ? -1 : 1);
			} else if (r1 == null && r2 != null) {
				double d2 = (Double) r2;
				return 0 < d2 ? -1 : 1;
			} else if (r1 != null && r2 == null) {
				double d1 = (Double) r1;
				return d1 < 0 ? -1 : 1;
			} else {
				return 0;
			}
		}
	}

	private static class ByteComparator implements Comparator<Object>,
			Serializable {
		private static final long serialVersionUID = -5517415727670106744L;

		public int compare(Object r1, Object r2) {
			if (r1 != null && r2 != null) {
				byte b1 = (Byte) r1;
				byte b2 = (Byte) r2;
				return b1 == b2 ? 0 : (b1 < b2 ? -1 : 1);
			} else if (r1 == null && r2 != null) {
				return -1;
			} else if (r1 != null && r2 == null) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	private static class BooleanComparator implements Comparator<Object>,
			Serializable {
		private static final long serialVersionUID = 2462499445166978825L;

		public int compare(Object r1, Object r2) {
			if (r1 != null && r2 != null) {
				boolean b1 = (Boolean) r1;
				boolean b2 = (Boolean) r2;
				return b1 == b2 ? 0 : (b1 ? 1 : -1);
			} else if (r1 == null && r2 != null) {
				return -1;
			} else if (r1 != null && r2 == null) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	private static class DateComparator implements Comparator<Object>,
			Serializable {
		private static final long serialVersionUID = -1384424759889207483L;

		public int compare(Object r1, Object r2) {
			if (r1 != null && r2 != null) {
				java.sql.Date d1 = (java.sql.Date) r1;
				java.sql.Date d2 = (java.sql.Date) r2;
				return d1.compareTo(d2);
			} else if (r1 == null && r2 != null) {
				return -1;
			} else if (r1 != null && r2 == null) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	private static class TimeComparator implements Comparator<Object>,
			Serializable {
		private static final long serialVersionUID = 1690128776314769246L;

		public int compare(Object r1, Object r2) {
			if (r1 != null && r2 != null) {
				java.sql.Time t1 = (java.sql.Time) r1;
				java.sql.Time t2 = (java.sql.Time) r2;
				return t1.compareTo(t2);
			} else if (r1 == null && r2 != null) {
				return -1;
			} else if (r1 != null && r2 == null) {
				return 1;
			} else {
				return 0;
			}
		}
	}

}
