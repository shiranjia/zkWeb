package com.yasenagat.zkweb.util;

import java.sql.*;

/**
 * Created by jiashiran on 2016/12/22.
 */
public class Derby{
    private static String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    private static String protocol = "jdbc:derby:";

    String dbName = "/export/db";

    public Derby(){
        try {
            Class.forName(driver).newInstance();
            System.out.println("Loaded the appropriate driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(protocol + dbName + ";create=true");
        return conn;
    }

    public static void main(String[] args) {
        Derby derby = new Derby();
        try (Connection con = derby.getConnection()){
            Statement sta = con.createStatement();
            //sta.execute("CREATE TABLE ZK(ID VARCHAR(100) PRIMARY KEY, DES VARCHAR(100), CONNECTSTR VARCHAR(100), SESSIONTIMEOUT VARCHAR(100))");
            sta.execute("DROP TABLE ZK");
            /*ResultSet re = sta.executeQuery("SHOW TABLES;");
            while (re.next()){
                System.out.println(re.getString(1));
            }*/
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
