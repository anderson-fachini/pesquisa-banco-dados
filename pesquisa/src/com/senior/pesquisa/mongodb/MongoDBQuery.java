package com.senior.pesquisa.mongodb;

import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoDBQuery {

	public static void main(String[] args) {
		double seconds;

		for (int i = 0; i < 4; i++) {
			System.out.println("==== Execution " + (i + 1) + " ====");
			System.out.println("Executing query 1");
			seconds = query1();
			System.out.println(seconds + " secods\n");

			System.out.println("Executing query 2");
			seconds = query2();
			System.out.println(seconds + " secods\n");
		}
	}

	private static double query1() {
		BasicDBObject query = new BasicDBObject().append("latitude", 116.36838)
				.append("longitude", 39.90484);

		double seconds = 0;

		MongoClient mongoClient = new MongoClient("localhost", 27017);

		DB db = mongoClient.getDB("traffic");

		DBCollection coll = db.getCollection("data");

		Date datIni = new Date();
		coll.find(query);
		Date datFim = new Date();

		seconds = (double) (datFim.getTime() - datIni.getTime()) / 1000;

		return seconds;
	}

	private static double query2() {
		double seconds = 0;

		MongoClient mongoClient = new MongoClient("localhost", 27017);

		DB db = mongoClient.getDB("traffic");

		DBCollection coll = db.getCollection("data");

		Date datIni = new Date();

		DBObject sort = new BasicDBObject();
		sort.put("latitude", -1);

		DBCursor cursor = coll.find().sort(sort).limit(1);

		BasicDBObject query = new BasicDBObject().append("latitude", cursor
				.next().get("latitude"));

		coll.find(query);

		Date datFim = new Date();

		seconds = (double) (datFim.getTime() - datIni.getTime()) / 1000;

		return seconds;
	}
}
