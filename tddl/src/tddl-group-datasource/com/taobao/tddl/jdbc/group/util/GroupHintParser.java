//Copyright(c) Taobao.com
package com.taobao.tddl.jdbc.group.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.util.TStringUtil;
import com.taobao.tddl.jdbc.group.GroupIndex;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2010-12-24ÉÏÎç10:32:16
 */
public class GroupHintParser {
	public static Log log = LogFactory.getLog(GroupHintParser.class);

	public static GroupIndex convertHint2Index(String sql) {
		String groupIndexHint = extractTDDLGroupHintString(sql);
		if (null != groupIndexHint && !groupIndexHint.equals("")) {
			String[] hintWithRetry = groupIndexHint.split(",");
			if (hintWithRetry.length == 1) {
				int index = GroupHintParser
						.getIndexFromHintPiece(hintWithRetry[0]);
				return new GroupIndex(index, false);
			} else if (hintWithRetry.length == 2) {
				int index = GroupHintParser
						.getIndexFromHintPiece(hintWithRetry[0]);
				boolean retry = GroupHintParser
						.getFailRetryFromHintPiece(hintWithRetry[1]);
				return new GroupIndex(index, retry);
			} else {
				throw new IllegalArgumentException(
						"the standard group hint is:'groupIndex:12[,failRetry:true]'"
								+ ",current hint is:" + groupIndexHint);
			}
		} else {
			return null;
		}
	}

	private static int getIndexFromHintPiece(String indexPiece) {
		String[] piece = indexPiece.split(":");
		if (piece[0].trim().toLowerCase().equals("groupindex")) {
			return Integer.valueOf(piece[1]);
		} else {
			throw new IllegalArgumentException(
					"the standard group hint is:'groupIndex:12[,failRetry:true]'"
							+ ",current index hint is:" + indexPiece);
		}
	}

	private static boolean getFailRetryFromHintPiece(String retryPiece) {
		String[] piece = retryPiece.split(":");
		if (piece[0].trim().toLowerCase().equals("failretry")) {
			return Boolean.valueOf(piece[1]);
		} else {
			throw new IllegalArgumentException(
					"the standard group hint is:'groupIndex:12[,failRetry:true]'"
							+ ",current retry hint is:" + retryPiece);
		}
	}

	private static String extractTDDLGroupHintString(String sql) {
		return TStringUtil.getBetween(sql.toLowerCase(), "/*+tddl_group({",
				"})*/");
	}

	public static String removeTddlGroupHint(String sql) {
		String tddlHint = extractTDDLGroupHintString(sql);
		if (null == tddlHint || "".equals(tddlHint)) {
			return sql;
		}

		sql = TStringUtil.removeBetweenWithSplitor(sql.toLowerCase(),
				"/*+tddl_group({", "})*/");
		return sql;
	}

	public static void main(String[] args) {
		String sql = "/*+TDDL_GROUP({groupIndex:12})*/select * from tab";

		System.out.println(convertHint2Index(sql));
		System.out.println(removeTddlGroupHint(sql));
	}
}
