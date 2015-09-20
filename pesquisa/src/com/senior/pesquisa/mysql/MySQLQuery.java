package com.senior.pesquisa.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

public class MySQLQuery {

	public static void main(String[] args) {
		String query1 = "select * from data where latitude=116.36838 and longitude=39.90484";
		String query2 = "select * from data where latitude = (select max(latitude) from data)";

		double seconds;

		for (int i = 0; i < 4; i++) {
			System.out.println("==== Execution " + (i + 1) + " ====");
			System.out.println("Executing query 1");
			seconds = query(query1);
			System.out.println(seconds + " secods\n");

			System.out.println("Executing query 2");
			seconds = query(query2);
			System.out.println(seconds + " secods\n");
		}
	}

	private static double query(String query) {
		double seconds = 0;

		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (InstantiationException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (IllegalAccessException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (ClassNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		Connection conn = null;
		Statement smt = null;

		try {
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost/traffic", "root", "123456");

			try {
				smt = conn.createStatement();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			Date datIni = new Date();
			smt.executeQuery(query);
			Date datFim = new Date();

			seconds = (double) (datFim.getTime() - datIni.getTime()) / 1000;

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				smt.close();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return seconds;
	}
}
