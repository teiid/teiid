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
package org.teiid.rhq.enterprise;

import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

public class AdminUtil {
	
	private final static String[] CLASSLOADERPREFIXVALUES = new String[] {
		"/jbedsp-teiid", "/teiid-client-", "/log4j-", "/commons-logging", "/teiid-common-core", "netty-" };  //$NON-NLS-1$,  //$NON-NLS-2$,  //$NON-NLS-3$,  //$NON-NLS-4$ 
	
	public static ClassLoader createClassloader(String installDir, ClassLoader parentClassLoader) {
		if (true) {
			return parentClassLoader;
		}
		        LinkedList p = new LinkedList();
        String[] classLoaderValues = getJarsForPrefixes(installDir + "/lib");  //$NON-NLS-1$
        String[] patches = getPatchJars(installDir + "/lib/patches");  //$NON-NLS-1$
        int cnt = classLoaderValues.length;
        if (patches != null) {        	
            p.addAll(Arrays.asList(patches));
            cnt += patches.length;
        }  
        
        p.add(installDir + "/lib/"); //$NON-NLS-1$
        cnt++;
        
        if (classLoaderValues != null){
        	p.addAll(Arrays.asList(classLoaderValues));
        }       

		URL[] urls = new URL[cnt];
		try {
            
          
			for (int i = 0; i < cnt; i++) {
                String f = (String) p.get(i);
				urls[i] = new File(f).toURL();
			}
            ClassLoader classLoader = new URLClassLoader(urls, parentClassLoader);
            return classLoader;
          
		} catch (Exception e) {
            e.printStackTrace();
			// TODO: handle exception 
		}
        
        return null;
	}
	
	private static String[] getJarsForPrefixes(String dir) {
        ArrayList<String> jarArrayList = new ArrayList<String>();
        File[] jarFiles = findAllFilesInDirectoryHavingExtension(dir, ".jar"); //$NON-NLS-1$
        if (jarFiles != null && jarFiles.length > 0) {
            for (int i = 0; i < jarFiles.length; i++) {
                try {
                	String fileName =  jarFiles[i].getCanonicalPath();
                	for (int j = 0; j < CLASSLOADERPREFIXVALUES.length; j++){
                		if (fileName.contains(CLASSLOADERPREFIXVALUES[j].toString())){
                			jarArrayList.add(fileName);
                			break;
                		}
                	}
                } catch (Exception e) {
                    //TODO
                }
            }
                
        }
        String[] jars = new String[jarArrayList.size()];
        int i = 0;
        Iterator<String> jarIter = jarArrayList.iterator();
        while(jarIter.hasNext()){
        	jars[i++] = jarIter.next();
        }
        
        return jars;
    }
    
    private static String[] getPatchJars(String dir) {
        String[] jars = null;
        File[] jarFiles = findAllFilesInDirectoryHavingExtension(dir, ".jar"); //$NON-NLS-1$
        if (jarFiles != null && jarFiles.length > 0) {
            jars = new String[jarFiles.length];
            for (int i = 0; i < jarFiles.length; i++) {
                try {
                	jars[i] =  jarFiles[i].getCanonicalPath();
                } catch (Exception e) {
                    //TODO
                }
            }
                
        }
        return jars;
    }
    
    /**
     * Returns a <code>File</code> array that will contain all the files that
     * exist in the directory that have the specified extension.
     * @return File[] of files having a certain extension
     */
    private static File[] findAllFilesInDirectoryHavingExtension(String dir, final String extension) {

      // Find all files in that directory that end in XML and attempt to
      // load them into the runtime metadata database.
      File modelsDirFile = new File(dir);
      FileFilter fileFilter = new FileFilter() {
          public boolean accept(File file) {
              if(file.isDirectory()) {
                  return false;
              }


              String fileName = file.getName();

              if (fileName==null || fileName.length()==0) {
                  return false;
              }

              // here we check to see if the file is an .xml file...
              int index = fileName.lastIndexOf("."); //$NON-NLS-1$

              if (index<0 || index==fileName.length()) {
                  return false;
              }

              if (fileName.substring(index, fileName.length()).equalsIgnoreCase(extension)) {
                  return true;
              }
              return false;
          }
      };

      File[] modelFiles = modelsDirFile.listFiles(fileFilter);

      return modelFiles;

  }    
	
	/**
	 * Return the tokens in a string in a list. This is particularly
	 * helpful if the tokens need to be processed in reverse order. In that case,
	 * a list iterator can be acquired from the list for reverse order traversal.
	 *
	 * @param str String to be tokenized
	 * @param delimiter Characters which are delimit tokens
	 * @return List of string tokens contained in the tokenized string
	 */
	public static List getTokens(String str, String delimiter) {
		ArrayList l = new ArrayList();
		StringTokenizer tokens = new StringTokenizer(str, delimiter);
		while(tokens.hasMoreTokens()) {
			l.add(tokens.nextToken());
		}
		return l;
    }
	
    /*
     * Replace all occurrences of the search string with the replace string
     * in the source string. If any of the strings is null or the search string
     * is zero length, the source string is returned.
     * @param source the source string whose contents will be altered
     * @param search the string to search for in source
     * @param replace the string to substitute for search if present
     * @return source string with *all* occurrences of the search string
     * replaced with the replace string
     */
    public static String replaceAll(String source, String search, String replace) {
        if (source != null && search != null && search.length() > 0 && replace != null) {
            int start = source.indexOf(search);
            if (start > -1) {
                StringBuffer newString = new StringBuffer(source);
                replaceAll(newString, search, replace);
                return newString.toString();
            }
        }
        return source;    
    }
    
    public static void replaceAll(StringBuffer source, String search, String replace) {
        if (source != null && search != null && search.length() > 0 && replace != null) {
            int start = source.toString().indexOf(search);
            while (start > -1) {
                int end = start + search.length();
                source.replace(start, end, replace);
                start = source.toString().indexOf(search, start + replace.length());
            }
        }
    }    
	
}
