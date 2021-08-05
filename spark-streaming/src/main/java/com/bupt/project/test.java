package com.bupt.project;

/**
 * @param
 * @author gml
 * @version 1.0
 * @date 2021/8/4 13:51
 * @return
 */
public class test {
	public static void main(String[] args) {
		int[] a = new int[]{1, 3, 5, 2, 4};
		int[] b = a;
		a[2] = b[1] - b[2];
		b[1] = a[0] + a[3];
		System.out.println(a[2] - a[1] + b[2]);
	}
	
}
