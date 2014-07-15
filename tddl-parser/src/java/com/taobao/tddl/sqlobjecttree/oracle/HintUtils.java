package com.taobao.tddl.sqlobjecttree.oracle;

import java.util.List;
import java.util.Set;

import com.taobao.tddl.sqlobjecttree.Hint;

public class HintUtils {
	public static StringBuilder appendHint(Set<String> oraTabName, List<Object> list,
			StringBuilder sb,List<Hint> hints) {
		if (hints!=null&&hints.size() != 0) {
			sb.append("/*+ ");
			boolean firestElement = true;
			for (Hint hint : hints) {
				if (firestElement) {
					firestElement = false;
				} else {
					sb.append(" ");
				}
				sb = hint.regTableModifiable(oraTabName, list, sb);
			}
			sb.append(" */ ");
		}
		return sb;
	}

}
