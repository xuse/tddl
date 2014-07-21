package com.taobao.tddl.sqlobjecttree.oracle;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.sqlobjecttree.Column;
import com.taobao.tddl.common.sqlobjecttree.SQLFragment;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.interact.sqljep.ComparativeAND;
import com.taobao.tddl.interact.sqljep.ComparativeOR;
import com.taobao.tddl.sqlobjecttree.DMLCommon;
import com.taobao.tddl.sqlobjecttree.Expression;
import com.taobao.tddl.sqlobjecttree.RowJepVisitor;
import com.taobao.tddl.sqlobjecttree.Select;
import com.taobao.tddl.sqlobjecttree.TableName;
import com.taobao.tddl.sqlobjecttree.common.TableNameSubQueryImp;
import com.taobao.tddl.sqlobjecttree.common.expression.ComparableExpression;
import com.taobao.tddl.sqlobjecttree.common.expression.ExpressionGroup;
import com.taobao.tddl.sqlobjecttree.common.expression.OrExpressionGroup;

/**
 * 解析oracle skip 和max值的工具类
 * @author shenxun
 *
 */
public class SkipMaxUtils {
	
    /**
     * mysql: limit 0,20
     * 实际数据在数据库中的取法: x>0 and x<=20
     * 对应java的limitTo limitFrom:lTo=1-1 lFrom=21-1(因为mysql第一条数据对应java数组的0)
     * @param co
     * @return
     */
    public static int getRowNumSkipToInt(Comparative co) {
    	if(co==null){
    		return DMLCommon.DEFAULT_SKIP_MAX;
    	}
        if (co instanceof ComparativeAND) {
            int snapSkip = DMLCommon.DEFAULT_SKIP_MAX;
            ComparativeAND coAnd = (ComparativeAND) co;
            List<Comparative> compList = coAnd.getList();

            for (Comparative c : compList) {
                int tempRowNum = getRowNumSkipToInt(c);
                if (tempRowNum != DMLCommon.DEFAULT_SKIP_MAX && tempRowNum > snapSkip) {
                    snapSkip = tempRowNum;
                }
            }
            return snapSkip;
        } else if (co instanceof ComparativeOR) {
            throw new IllegalArgumentException("不支持一条sql中rownum出现or的情况");
        } else if (co.getComparison() == Comparative.Equivalent || co.getComparison() == 0) {
            throw new IllegalArgumentException("rownum目前不支持=关系");
        } else if (co.getComparison() == Comparative.GreaterThan) {
            //> 的情况
            return getComparativeToInt(co);
        } else if (co.getComparison() == Comparative.GreaterThanOrEqual) {
            //>=的情况
            return getComparativeToInt(co) - 1;
        }
        return DMLCommon.DEFAULT_SKIP_MAX;
//        else {
//          //其他标签的情况
//        }
    }
   public static int getRowNumMaxToInt(Comparative co) {
		if(co==null){
    		return DMLCommon.DEFAULT_SKIP_MAX;
    	}
        if (co instanceof ComparativeAND) {
            int snapSkip = DMLCommon.DEFAULT_SKIP_MAX;
            ComparativeAND coAnd = (ComparativeAND) co;
            List<Comparative> compList = coAnd.getList();

            for (Comparative c : compList) {
                int tempRowNum = getRowNumMaxToInt(c);
                if (tempRowNum != DMLCommon.DEFAULT_SKIP_MAX && tempRowNum > snapSkip) {
                    snapSkip = tempRowNum;
                }
            }
            return snapSkip;
        } else if (co instanceof ComparativeOR) {
            throw new IllegalArgumentException("不支持一条sql中rownum出现or的情况");
        } else if (co.getComparison() == Comparative.Equivalent || co.getComparison() == 0) {
            throw new IllegalArgumentException("rownum目前不支持=关系");
        } else if (co.getComparison() == Comparative.LessThan) {
            //< 的情况
            return getComparativeToInt(co)-1;
        } else if (co.getComparison() == Comparative.LessThanOrEqual) {
            //<=的情况
            return getComparativeToInt(co);
        } 
        return DMLCommon.DEFAULT_SKIP_MAX;
//        else {
//		  其他的比较情况
//            throw new IllegalStateException("不应到达此处");
//        }
    }
    private static int getComparativeToInt(Comparative co) {
        Comparable<?> ctemp = co.getValue();
        if (ctemp instanceof Integer) {
            return (Integer) ctemp;
        }
        if (ctemp instanceof BigDecimal) {
            return ((BigDecimal) ctemp).intValueExact();
        } else {
            throw new IllegalArgumentException("目前只支持bigDecimal和integer类型的rownum参数,当前参数为:"+ctemp.getClass()+"|"+ctemp);
        }
    }

	/**
	 * 找到所有的包含rownum或其别名的关系数组，放到一个ExpressionGroup中
	 */
	protected  static ComparativeAND buildRownumGroup(OrExpressionGroup orExpressionGroup,
			List<TableName> tbNames,Map<String, SQLFragment>  aliasToSQLFragementMap){
	
		ExpressionGroup rownum = new ExpressionGroup();
		
		nestedBuildRownumGroup(orExpressionGroup, tbNames, rownum,aliasToSQLFragementMap);
		
		RowJepVisitor visitor = new RowJepVisitor();

		rownum.eval(visitor, true);
		
		Map<String, Comparative> map = visitor.getComparable();
		
		ComparativeAND and=new ComparativeAND();
		
		for(Comparative com:map.values()){
			//因为有别名指向rownum，所以不能简单的使用map.get("rownum")来获取目标Comparative
			and.addComparative(com);
		}
		
		return and;
	}
	
    
    /**
     * 这个方法会循环遍历整个对象树，从里面获取rownum的表达式
     * 包括rownum的别名组织表达式，如rownum rn,然后在其他地方rn <=10这样。也会找出来
     * @param source
     * @param tabNames
     * @param targetRownumGroup
     * @param aliasMap
     */
    public static void nestedBuildRownumGroup(ExpressionGroup source,
			List<TableName> tabNames, ExpressionGroup targetRownumGroup,Map<String,SQLFragment> aliasMap) {
		if (targetRownumGroup != null) {
			for (TableName tname : tabNames) {
				//先找表名是个select的。
				if (tname instanceof TableNameSubQueryImp) {
					Select select = ((TableNameSubQueryImp) tname)
							.getSubSelect();
					nestedBuildRownumGroup(select.getWhere().getExpGroup(), select
							.getTbNames(), targetRownumGroup,aliasMap);
				}
				// else{
				// //简单表名或函数表名
				// }
			}
			putRowNumIntoRownumExpGroup(source, targetRownumGroup,aliasMap);
		}
	}

	/**
	 * 专门处理一个select级别中的where条件
	 * 拼装条件中只采用了and关系，因为rownum是伪列，想不出有什么情况会让rownum这个select列有rownum >2 or rownum<2这样的条件
	 * @param source
	 * @param targetRowNumGroup
	 * @param aliasMap 别名Map,存放别名->sql元素的映射。
	 */
	protected static void putRowNumIntoRownumExpGroup(ExpressionGroup source,
			ExpressionGroup targetRowNumGroup,Map<String,SQLFragment> aliasMap) {
		List<Expression> exps = source.getExpressions();
		for (Expression exp : exps) {
			if (exp instanceof ExpressionGroup) {
				// 表达式组嵌套。循环进入内部处理
				putRowNumIntoRownumExpGroup(((ExpressionGroup) exp),
						targetRowNumGroup,aliasMap);
			} else if (exp instanceof ComparableExpression) {
				Object left = ((ComparableExpression) exp).getLeft();
				if (left instanceof Column) {
					//左边一般来说都是列名，整个解析树默认遵守的规则
					//TODO:对左右颠倒的要想个办法在不影响当前功能的情况下抛出异常，抛得时候要小心col=col+1这种情况。
					String colName = ((Column) left).getColumn();
					if (colName != null) {
						putRownumColumnToRownumExpression(targetRowNumGroup,
								exp, colName);
						//处理rownum别名的情况
						SQLFragment fragement = aliasMap.get(colName
								.toUpperCase());
						if (fragement instanceof Column) {
							String tempCol = ((Column) fragement).getColumn();
							//看一下别名所对应的真正列名是否是一个rownum列。如果是就添加到rownum数组中
							if (tempCol != null) {
								putRownumColumnToRownumExpression(
										targetRowNumGroup, exp, tempCol);
							}
						}
						// else{
						// //其它左边部分非column的情况。忽略，主要有两种可能，第一种是左右颠倒，第二种是非rownum。都忽略
						// }
					}
				}
				Object right = ((ComparableExpression) exp).getRight();
				if (right instanceof Select) {
					// 如果右边是一个Select
					// 递归的从select中抽取ExpGroup
					Select select = ((Select) right);
					nestedBuildRownumGroup(select.getWhere().getExpGroup(), select
							.getTbNames(), targetRowNumGroup,aliasMap);
				}
				// else{
				// //其他的正常绑定变量或列名情况，忽略。
				// }
			}
		}
	}

	private static void putRownumColumnToRownumExpression(
			ExpressionGroup targetRowNumGroup, Expression exp, String colName) {
		if (colName.equalsIgnoreCase("rownum")) {
			// 如果是rownum,放入RowNum专用ExpGroup中.
			// 因为rownum是伪列，不会出现针对他的处理函数。
			targetRowNumGroup.addExpression(exp);
			((ComparableExpression)exp).setRownum(true);
		}
	}
}
