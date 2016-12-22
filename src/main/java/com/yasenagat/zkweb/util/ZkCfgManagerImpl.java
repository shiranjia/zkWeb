package com.yasenagat.zkweb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class ZkCfgManagerImpl implements ZkCfgManager {

	private static Logger log = LoggerFactory.getLogger(ZkCfgManagerImpl.class);

	// 指定JDBC串
	//private static JdbcConnectionPool cp = JdbcConnectionPool.create("jdbc:h2:~/test", "sa", "sa");
	private static Derby cp = new Derby();
	private static Connection conn = null;

	public ZkCfgManagerImpl() {
		try {
			getConnection();
			init();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}


	private Connection getConnection() throws SQLException {
		if (null == conn) {
			conn = cp.getConnection();
		}
		return conn;
	}

	private void closeConn() {
		if (null != conn) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean init() {
		try (Statement ps = getConnection().createStatement()){
			return ps.execute(ZkCfgManager.newInitSql);
		} catch (Exception e) {
			log.info("init zkCfg error : {}", e.getMessage());
		}
		return false;
	}

	public boolean add(String des, String connectStr, String sessionTimeOut) {
		try (Statement ps = getConnection().createStatement()){
			return ps.execute("INSERT INTO ZK VALUES('" + UUID.randomUUID().toString().replaceAll("-", "") + "','" + des +"', '" + connectStr +"','" + sessionTimeOut+"'");
		} catch (SQLException e) {
			e.printStackTrace();
			log.error("add zkCfg error : {}", e.getMessage());
		}
		return false;
	}

	public List<Map<String, Object>> query() {
		try (Statement ps = getConnection().createStatement();
			 ResultSet rs = ps.executeQuery("SELECT * FROM ZK")){
			List<Map<String, Object>> list = new ArrayList();
			ResultSetMetaData meta = rs.getMetaData();
			Map<String, Object> map;
			int cols = meta.getColumnCount();
			while (rs.next()) {
				map = new HashMap();
				for (int i = 0; i < cols; i++) {
					map.put(meta.getColumnName(i + 1), rs.getObject(i + 1));
				}
				list.add(map);
			}
			return list;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return new ArrayList();
	}

	public boolean update(String id, String des, String connectStr, String sessionTimeOut) {
		try (Statement ps = getConnection().createStatement()){
			return ps.execute("UPDATE ZK SET DES='" + des +"',CONNECTSTR='"+connectStr+"',SESSIONTIMEOUT='"+sessionTimeOut+"' WHERE ID='"+id+"'");
		} catch (Exception e) {
			e.printStackTrace();
			log.error("update id={} zkCfg error : {}", new Object[] { id, e.getMessage() });
		}
		return false;
	}

	public boolean delete(String id) {
		try (Statement ps = getConnection().createStatement()){
			return ps.execute("DELETE FROM ZK WHERE ID='"+id+"'");
		} catch (Exception e) {
			e.printStackTrace();
			log.error("delete id={} zkCfg error : {}", new Object[] { id, e.getMessage() });
		}
		return false;
	}

	public Map<String, Object> findById(String id) {
		try  (Statement ps = getConnection().createStatement();
			  ResultSet rs = ps.executeQuery("SELECT * FROM ZK WHERE ID = '" + id +"'")){
			Map<String, Object> map = new HashMap();
			ResultSetMetaData meta = rs.getMetaData();
			int cols = meta.getColumnCount();
			if (rs.next()) {
				for (int i = 0; i < cols; i++) {
					map.put(meta.getColumnName(i + 1).toLowerCase(), rs.getObject(i + 1));
				}
			}
			return map;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public List<Map<String, Object>> query(int page, int rows) {
		int start = (page-1) * rows;
		int end = page * rows;
		try (ResultSet rs = getConnection().createStatement()
				.executeQuery(
						"select * from (select row_number() over() as rownum, ZK.* from ZK) as tmp where rownum>="+start+" and rownum<="+end)){
			List<Map<String, Object>> list = new ArrayList();
			Map<String, Object> map;
			while (rs.next()) {
				map = new HashMap();
				for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
					map.put(rs.getMetaData().getColumnName(i + 1), rs.getObject(i + 1));
				}
				list.add(map);
			}
			return list;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return new ArrayList();
	}

	public boolean add(String id, String des, String connectStr, String sessionTimeOut) {
		try (Statement ps = getConnection().createStatement()){
			return ps.execute("INSERT INTO ZK VALUES('"+id+"','"+des+"','"+connectStr+"','"+sessionTimeOut+"')");
		} catch (SQLException e) {
			e.printStackTrace();
			log.error("add zkCfg error : {}", e.getMessage());
		}
		return false;
	}

	public int count() {
		try (ResultSet rs = getConnection().createStatement().executeQuery("SELECT count(id) FROM ZK")){
			if (rs.next()) {
				return rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			log.error("count zkCfg error : {}", e.getMessage());
		}
		return 0;
	}

}
