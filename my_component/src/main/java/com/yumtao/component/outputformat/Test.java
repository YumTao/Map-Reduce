package com.yumtao.component.outputformat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;

public class Test {

	public static void main(String[] args) throws Exception {
		BufferedReader brBufferedReader = new BufferedReader(
				new FileReader(new File("C:\\Users\\56243\\Desktop\\test.txt")));
		String readLine = brBufferedReader.readLine();
		System.out.println(readLine);
		Arrays.asList(readLine.split("\t")).forEach(System.out::println);

		brBufferedReader.close();
	}
}
