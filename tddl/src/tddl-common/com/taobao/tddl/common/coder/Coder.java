package com.taobao.tddl.common.coder;

import java.util.List;

import com.taobao.tddl.common.SyncCommand;

/**
 * @author huali
 *
 * 数据库操作命令编解码器
 * 完成数据库操作命令列表到字符串的编码和解码过程
 */
public interface Coder {
	List<SyncCommand> decode(String content);
	
	String encode(List<SyncCommand> commands);
	
	String getId();
}
