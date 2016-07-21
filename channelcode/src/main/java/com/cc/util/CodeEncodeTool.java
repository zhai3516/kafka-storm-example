package com.cc.util;

public class CodeEncodeTool {
	
	/**
	 *  class that provide static method to encoding strings that contain special chars
	 */
	
	static public Boolean isLimitChar(char c){
		Boolean isLimit = false;
		if (c == 45 | c == 46 | c == 95 | (c >= 48 & c <= 57) |
				(c >= 65 & c <= 90) | ( c >= 97 & c <= 122 ))
			isLimit = true;
		return isLimit;
	}
	
	static public String encoding(String s){
		StringBuilder temp = new StringBuilder();
		for(int i = 0;i < s.length();i ++){
			char c = s.charAt(i);
			if (!isLimitChar(c)){
				int asciiCode = c;
				temp.append('/');
				temp.append(asciiCode);
			}else{
				temp.append(c);
			}
		}
		return temp.toString();
	}
}
