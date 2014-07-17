package com.taobao.tddl.client.jdbc.resultset.helper;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * @author junyu
 *
 */
public class AddRealizer {
    private static Map<String,Add<Object>> adds=new HashMap<String,Add<Object>>();
    
    static{
    	adds.put(Integer.class.getName(), new IntegerAdd());
    	adds.put(Float.class.getName(),new FloatAdd());
    	adds.put(Long.class.getName(),new LongAdd());
    	adds.put(Double.class.getName(),new DoubleAdd());
    	adds.put(BigDecimal.class.getName(),new BigDecimalAdd());
    	adds.put(Short.class.getName(),new ShortAdd());
    	adds.put(Byte.class.getName(), new ByteAdd());
    }

	public static Add<Object> getNumberAdd(Object obj){
		return adds.get(obj.getClass().getName());
	}
	
	public static void main(String[] args){
		getNumberAdd(new StringBuffer());
	}	
	
	public interface Add<T> {
		T add(T left, T right);
	}
	
    private static class IntegerAdd implements Add<Object>{
		public Object add(Object left, Object right) {
			return (Integer)left+(Integer)right;
		}
    }
    
    private static class FloatAdd implements Add<Object>{
		public Object add(Object left, Object right) {
			return (Float)left+(Float)right;
		}
    }
    
    private static class LongAdd implements Add<Object>{
		public Object add(Object left, Object right) {
			return (Long)left+(Long)right;
		}
    }
    
    private  static class DoubleAdd implements Add<Object>{
		public Object add(Object left, Object right) {
			return (Double)left+(Double)right;
		}
    }
    
    private static class BigDecimalAdd implements Add<Object>{
		public Object add(Object left, Object right) {
			return ((BigDecimal) left).add((BigDecimal) right);
		}
    }
    
    private static class ShortAdd implements Add<Object>{
		public Object add(Object left, Object right) {
			return (Short)left+(Short)right;
		}
    }
    
    private static class ByteAdd implements Add<Object>{
		public Object add(Object left, Object right) {
			return (Byte)left+(Byte)right;
		}
    }
}
