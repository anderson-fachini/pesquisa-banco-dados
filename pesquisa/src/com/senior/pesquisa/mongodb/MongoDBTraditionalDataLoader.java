package com.senior.pesquisa.mongodb;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.senior.pesquisa.common.DataReader;

public class MongoDBTraditionalDataLoader {

	private static List<String> dataLines = new ArrayList<>();
	private static List<BasicDBObject> insertObjects = new ArrayList<>();

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

			// for (int j = 0; j < insertObjects.size(); j++) {
			// System.out.println(j + " " + insertObjects.get(j).size());
			// }

			System.out.println();

			dataLines.clear();
			insertObjects.clear();

			// break;
		}

		Date finalDate = new Date();

		System.out.println("Initial Date " + initialDate);
		System.out.println("Final Date " + finalDate);

	}

	private static void prepareData() {
		int qtd = dataLines.size();
		String splitted[];

		String format = "yyyy-MM-dd kk:mm:ss";
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern(format);
		Date date = null;

		for (int i = 0; i < qtd; i++) {
			splitted = dataLines.get(i).split(",");

			date = Date.from(LocalDateTime.parse(splitted[1], dtf)
					.atZone(ZoneId.of("UTC")).toInstant());

			insertObjects.add(//
					new BasicDBObject()
							//
							.append("taxi_id", Integer.parseInt(splitted[0]))
							.append("timestamp", date)
							.append("latitude", Double.parseDouble(splitted[2]))
							.append("longitude",
									Double.parseDouble(splitted[3])));
		}
	}

	private static void insertData() {
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		DB db = mongoClient.getDB("traffic");
		DBCollection coll = db.getCollection("data");

		int qtd = insertObjects.size();

		for (int i = 0; i < qtd; i++) {
			coll.insert(insertObjects.get(i));
		}

		mongoClient.close();
	}
}
