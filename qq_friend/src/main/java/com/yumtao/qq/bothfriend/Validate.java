package com.yumtao.qq.bothfriend;

public class Validate {
	public static void main(String[] args) {
		int count = 0;
		int length = 14;
		for (int i = 0; i < length; i++) {
			for (int j = i + 1; j < length; j++) {
				count++;
			}
		}
		
		System.out.println(count);
	}
}
