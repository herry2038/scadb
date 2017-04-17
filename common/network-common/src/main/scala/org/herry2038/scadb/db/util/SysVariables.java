package org.herry2038.scadb.db.util;

import java.util.HashMap;
import java.util.Map;

public class SysVariables {
	private static Map<String,String> mapVariables ;
	static {
		mapVariables = new HashMap<String,String>() ;
		mapVariables.put("version", "distdb-0.0.1") ;
		mapVariables.put("version_comment", "distdb-0.0.1") ;
		mapVariables.put("session.autocommit", "true") ;
		mapVariables.put("version_compile_os", "Linux") ;
		mapVariables.put("hostname", "distdb") ;
		mapVariables.put("uptime", "100") ;
		mapVariables.put("autocommit","true") ;
		mapVariables.put("auto_increment_increment", "1") ;
		mapVariables.put("tx_isolation", "REPEATABLE-READ") ;
		mapVariables.put("lower_case_table_names", "0") ;
		mapVariables.put("max_allowed_packet","16777216") ;
		mapVariables.put("character_set_client","utf8") ;
		mapVariables.put("character_set_connection","utf8") ;
		mapVariables.put("character_set_results","utf8") ;
		mapVariables.put("character_set_server","utf8") ;
		mapVariables.put("interactive_timeout","1800") ;
		mapVariables.put("license","GPL") ;
		mapVariables.put("lower_case_table_names","0") ;
		mapVariables.put("max_allowed_packet","16777216") ;
		mapVariables.put("net_buffer_length","16384") ;
		mapVariables.put("net_write_timeout","60") ;
		mapVariables.put("query_cache_size","0") ;
		mapVariables.put("query_cache_type","ON") ;
		mapVariables.put("sql_mode","") ;
		mapVariables.put("system_time_zone","CST") ;
		mapVariables.put("time_zone","SYSTEM") ;
		mapVariables.put("wait_timeout","1800") ;
		mapVariables.put("init_connect","") ;
		//mapVariables.put("tx_read_only","OFF") ;
	}
	
	public static String getVariableValue(String variableName) {
		String value = mapVariables.get(variableName) ;
		if ( value == null )
			throw new RuntimeException("unknown system value!") ;
		return value ;
	}
}
