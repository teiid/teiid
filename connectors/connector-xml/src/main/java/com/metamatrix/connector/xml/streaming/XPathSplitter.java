package com.metamatrix.connector.xml.streaming;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;



public class XPathSplitter {
	
	List<String> result;
	char[] pathChars;
	
	public List<String> split(String paths) throws InvalidPathException {
		if(!validate(paths)) {
			throw new InvalidPathException("This path is not valid: " + paths);
		}
		result = new ArrayList<String>();
		if(-1 == paths.indexOf('(') || -1 == paths.indexOf(')')) {
			result.add(paths);
		} else {
			pathChars = paths.toCharArray();
			split(pathChars.length -1, "");
		}
		return new ArrayList<String>(new HashSet<String>(result));
	}

	private int split(int start, String suffix) {
		boolean suffixSeek = true;
		int end = start;
		int index = start;
		while (index >= 0) {
			if(pathChars[index] == ')') {
				if(end - index != 0 && pathChars[end] !=  ')' && suffixSeek) {
					suffix = new String(pathChars, index +1, end - index).trim()
					+ suffix.trim();
				}
				suffixSeek = false;
				index = end = split(index-1, suffix);
			} else if(pathChars[index] == '(') {
				if(pathChars[end] !=  ')' && end - index > 1) {
					String path = new String(pathChars, index+1, end - index);
					if (null != suffix && !suffix.isEmpty()) {
						path = path.trim() + suffix.trim();
					}
					result.add(path);
				}
				//appendSuffix(suffix);
				return index-1;
			} else if(pathChars[index] == '|') {
				end = index -1;
			}
			index--;
		}
		return index;
	}

	private static boolean validate(String paths) {
		//TODO: add validation here
		return true;
	}
}
