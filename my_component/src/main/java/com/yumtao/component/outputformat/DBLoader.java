package com.yumtao.component.outputformat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * @goal mysql连接工具类，获取对应数据放入到map中
 * @author yumTao
 *
 */
public class DBLoader {

	public static void dbLoader(Map<String, String> ruleMap) {
		Connection conn = null;
		Statement st = null;
		ResultSet res = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			// TODO 连接信息待修改
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mrDB", "root", "root");
			st = conn.createStatement();
			res = st.executeQuery("select url,content from urlcontent");
			while (res.next()) {
				ruleMap.put(res.getString(1), res.getString(2));
			}
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			try {
				if (res != null) {
					res.close();
				}
				if (st != null) {
					st.close();
				}
				if (conn != null) {
					conn.close();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		DBLoader db = new DBLoader();
		HashMap<String, String> map = new HashMap<String, String>();
		db.dbLoader(map);
		System.out.println(map);
	}
}
