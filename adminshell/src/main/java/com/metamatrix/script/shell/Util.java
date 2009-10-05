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

package com.metamatrix.script.shell;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.metamatrix.core.util.ObjectConverterUtil;
import com.metamatrix.jdbc.util.MMJDBCURL;

public class Util {

	public static byte[] readBinaryFile(String fileName) throws IOException {
	    InputStream is = null;
	    
	    if(fileName == null) {
	        throw new IOException("fileName is null");
	    }
	    try {
	        //try to load file from the classpath
	        is = Object.class.getResourceAsStream("/"+fileName);
	         
	        byte[] result;
	        if (is == null) {
	            //load from "hardcoded" path        
	            is = new FileInputStream(new File(fileName));
	        }
	    

	    }catch(Exception e) {
	         if (is == null) {
	         	try {
	            //load from "hardcoded" path        
	            	is = new FileInputStream(new File(fileName));
	            }catch(Exception e2) {
	                
	       			 e.printStackTrace(); 
	        		 return null;
	            }
	         } 

	    }
	    
		//convert to bytes
        byte[] result = ObjectConverterUtil.convertToByteArray(is);
        try {
        	is.close();
        }catch(Exception e3) {
        }    
        return result;
	}

	public static char[] readTextFile(String fileName) throws IOException {
	    if(fileName == null) {
	        throw new IOException("fileName is null");
	    }
	    char[] result = null;

	    try {
	      File file = new File(fileName);
	 
	    // changed to use the ObectConverterUtil, instead of the
	    // convertToCharArray() method because it doesn't completely
	    // convert the file, the XML reader throws a malform exception
	    // the test case for ServerAdminImpl also the ObjectConverterUtil
	    // that's why this was changed to use it
	      result = ObjectConverterUtil.convertFileToCharArray(file, null);
	    
	    }catch(Exception e) {
	        e.printStackTrace();
	    } 
	    return result;
	}

	public static void cleanUpDirectory(String dirName, String[] filesToKeep){
	    File dir = new File(dirName);
	    if (dir.exists()) {
	        File[] files = dir.listFiles();    
	        for (File f:files) {
                if (f.getName().endsWith(".deleted")) { 
                	continue;
                }       
                boolean delete = true;
	            for (String keep:filesToKeep) {            
	                if (f.getName().equalsIgnoreCase(keep)) {
	                	delete = false;
	                	break;
	                }
	            }
	            if (delete) f.delete();
	        }
	    }
	}
	
	public static char[] convertToCharArray(InputStream in) throws IOException {
		return ObjectConverterUtil.convertToCharArray(in, Integer.MAX_VALUE, null);
	}

	public static String extractVDBName(String url) {
	    MMJDBCURL mmurl = new MMJDBCURL(url);
	    return mmurl.getVDBName();
	}

	public static String extractHost(String url) {
		MMJDBCURL mmurl = new MMJDBCURL(url);
	    return mmurl.getConnectionURL();
	}

}
