package com.senior.pesquisa.mysql;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.senior.pesquisa.common.DataReader;

public class MySQLMassiveDataLoader {

	private static List<String> dataLines = new ArrayList<>();
	private static List<String> insertLines = new ArrayList<>();

	public static void main(String[] args) {
		File filesFolder = new File("/home/anderson/nodejs/data");
		File[] listFiles = filesFolder.listFiles();

		int qtd = listFiles.length;

		Date initialDate = new Date();

		for (int i = 0; i < qtd; i++) {
			System.out.println("reading file " + i + "/" + qtd + " "
					+ listFiles[i]);

			dataLines.addAll(DataReader.getFileLines(listFiles[i]));

			System.out.println("preparing data");
			prepareData();

			System.out.println("inserting data");
			insertData();
			System.out.println();

			dataLines.clear();
			insertLines.clear();
		}

		Date finalDate = new Date();

		System.out.println("Initial Date " + initialDate);
		System.out.println("Final Date " + finalDate);

	}

	private static void prepareData() {
		int qtd = dataLines.size();

		String preInsert = "INSERT INTO data (taxi_id, timestamp, latitude, longitude) VALUES ";

		String pattern = "({0},''{1}'',{2},{3})";
		String splitted[];

		List<String> params = new ArrayList<>();

		for (int i = 0; i < qtd; i++) {
			if (params.size() == 1000) {
				insertLines.add(preInsert + String.join(",", params));
				params.clear();
			}

			splitted = dataLines.get(i).split(",");
			params.add(MessageFormat.format(pattern, splitted));
		}

		if (!params.isEmpty()) {
			insertLines.add(preInsert + String.join(",", params));
		}
	}

	private static void insertData() {

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

		int qtd = insertLines.size();

		try {
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost/traffic", "root", "123456");

			try {
				smt = conn.createStatement();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			for (int i = 0; i < qtd; i++) {
				try {
					smt.executeUpdate(insertLines.get(i));
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
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
	}
}
