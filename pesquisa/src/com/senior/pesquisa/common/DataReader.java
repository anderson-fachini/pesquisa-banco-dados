package com.senior.pesquisa.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataReader {

	public static List<String> getFileLines(File file) {
		List<String> lines = new ArrayList<>();

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
			String line = "";

			try {
				while ((line = br.readLine()) != null) {
					lines.add(line);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return lines;
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return lines;
	}
}
