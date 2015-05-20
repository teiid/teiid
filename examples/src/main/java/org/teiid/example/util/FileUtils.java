/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.example.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class FileUtils {
	
	/**
	 * This method used to read file content from current directory.
	 * @param dir is the directory under current directory.
	 * @param name is the file name under dir
	 * @return the file content
	 * @throws IOException 
	 */
	public static String readFileContent(String dir, String name) {

	    File file = readFile(dir, name);
		BufferedReader reader = null;
		try {
			
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			String line = null;
			StringBuilder sb = new StringBuilder();
			
			while( ( line = reader.readLine() ) != null ) {
			    sb.append( line );
			    sb.append("\n"); //$NON-NLS-1$
			}
			return sb.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if(null != reader)
					reader.close();
			} catch (IOException e) {
				
			}
		}
		
	}
	
	/**
	 * Get file path from current directory
	 * @param dir is base directory under current directory
	 * @param name is the file name
	 * @return
	 */
	public static String readFilePath(String dir, String name){

		File baseDir = new File(System.getProperty("user.dir")); //$NON-NLS-1$
		File fileDir = find(baseDir, dir);
		File fileItem = find(fileDir, name);
		if(fileItem != null) {
			return fileItem.getAbsolutePath();
		}
		return null;
	}
	
	/**
	 * Find file from current directory
	 * @param dir is base directory under current directory
	 * @param name is the file name
	 * @return
	 */
	public static File readFile(String dir, String name){

		File baseDir = new File(System.getProperty("user.dir")); //$NON-NLS-1$
		File fileDir = find(baseDir, dir);
		return find(fileDir, name);
	}
	
	private static File find(File baseDir, String dir) {

		for(File file : baseDir.listFiles()) {
			if(file.getName().equals(dir)) {
				return file;
			} else if(file.isDirectory()) {
				File result = find(file, dir);
				if(result != null && result.getName().equals(dir)) {
					return result;
				}
			}
		}
		
		return null;
	}

}
