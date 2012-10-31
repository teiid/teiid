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

package org.teiid.resource.adapter.google.common;

/**
 * 
 * 
 */
public class Util {
	
	/**
	 * Converts spreadsheet column name to position number.
	 * 
	 * @param id   Name of the column
	 * @return     Position of the column
	 */
    public static int convertColumnIDtoInt(String id) {        
        String normID=id.toUpperCase().trim();
        int result=0;
        for(int counter=0, i=normID.length()-1;i>=0;i--,counter++){
           int partial=(int)normID.charAt(i)-64;
           result=(int)(result+(partial*Math.pow(26,counter)));
        }
        return result;
    }
    
	/**
	 * Converts spreadsheet column position to String.
	 * 
	 * @param id   Position of the column
	 * @return     Name of the column
	 */
    public static String convertColumnIDtoString(int id) {        
        StringBuilder result=new StringBuilder();
        int mod;
        while(id>0){
            mod=(id%26);   
            if(mod==0){
              mod=26;
              id=id-1; 
            }
            result.append((char)(mod+64)); 
            id/=26;
        }
        return result.reverse().toString();
    }
    
}
