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

package org.teiid.core.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.teiid.core.CorePlugin;
import org.teiid.core.TeiidRuntimeException;


public final class ApplicationInfo implements Serializable {
    
    public static final String APPLICATION_PRODUCT_INFORMATION       = "Product Information"; //$NON-NLS-1$

    public static final String APPLICATION_BUILD_NUMBER_PROPERTY       = "Build"; //$NON-NLS-1$
    
    private static final ApplicationInfo INSTANCE = new ApplicationInfo();

    private static final String LINE_SEPARATOR = "\n"; //$NON-NLS-1$

	private Properties props = new Properties();

    private ApplicationInfo() {
		InputStream is = this.getClass().getResourceAsStream("application.properties"); //$NON-NLS-1$
		try {
			try {
				props.load(is);
			} finally {
				is.close();
			}
		} catch (IOException e) {
			  throw new TeiidRuntimeException(CorePlugin.Event.TEIID10045, e);
		}
    }
    
    public String getReleaseNumber() {
		return props.getProperty("build.releaseNumber"); //$NON-NLS-1$
	}
    
	public int getMajorReleaseVersion() {
		String version = getReleaseNumber().substring(0, getReleaseNumber().indexOf('.'));
		return Integer.parseInt(version);
	}
	
    public int getMinorReleaseVersion() {
    	int majorIndex = getReleaseNumber().indexOf('.');
    	String version = getReleaseNumber().substring(majorIndex+1, getReleaseNumber().indexOf('.', majorIndex+1));
        return Integer.parseInt(version);
    }
	
	public String getBuildNumber() {
		return props.getProperty("build.number"); //$NON-NLS-1$
	}
	
	public String getUrl() {
		return props.getProperty("url"); //$NON-NLS-1$
	}
	
	public String getCopyright() {
		return props.getProperty("copyright"); //$NON-NLS-1$
	}
	
	public String getBuildDate() {
		return props.getProperty("build.date"); //$NON-NLS-1$
	}

    /**
     * Get the application information instance for this VM.
     * @return the singleton instance for this VM; never null
     */
    public static ApplicationInfo getInstance() {
        return INSTANCE;
    }
    
    private static String getClassPath() {
        return System.getProperty( "java.class.path" ); //$NON-NLS-1$
    }

    /**
     * The getClasspathInfo method is used to capture the current classpath 
     * information.  The initial intent is to write this information
     * to a file at VM startup time for debugging purposes and ensuring
     * patches are applied. 
     * @param outputstream
     * @since 4.2
     */
    public String getClasspathInfo() {
        String classPath = getClassPath(); 

        StringBuffer sb = new StringBuffer();
        sb.append("\nDate: " + DateFormat.getDateInstance().format(new Date()));//$NON-NLS-1$
        sb.append( LINE_SEPARATOR );        
        
        Map pathResults = new HashMap();
        List reversetList = new ArrayList();
        String separator = System.getProperty( "path.separator"  ); //$NON-NLS-1$

        StringTokenizer path = new StringTokenizer( classPath, separator );

        while(path.hasMoreTokens()){
            String pathElement = path.nextToken();
            File pathFile = new File( pathElement );
            if (pathFile.exists()) {
//                        if (resourceExistInClassPath(pathElement)) {
                pathResults.put(pathElement, Boolean.TRUE);
                    
            } else {
                pathResults.put(pathElement, Boolean.FALSE);
            }
            reversetList.add(pathElement);
        }       
        sb.append("Classpath Information" ); //$NON-NLS-1$
        sb.append( LINE_SEPARATOR );
        sb.append("CLASSPATH: "); //$NON-NLS-1$
        sb.append(classPath);
        sb.append( LINE_SEPARATOR );
        sb.append( LINE_SEPARATOR );
        
        Iterator iter = reversetList.iterator();
        sb.append("---- Classpath Entries ----"); //$NON-NLS-1$
         sb.append( LINE_SEPARATOR );       
        while ( iter.hasNext() ) {
            String epath = (String) iter.next();
            Boolean doesExist = (Boolean) pathResults.get(epath);
            sb.append(epath);
            if (!doesExist.booleanValue()) {
                sb.append(" (MISSING)");//$NON-NLS-1$
            }
            sb.append( LINE_SEPARATOR );
            
        }
        
        
        sb.append( LINE_SEPARATOR );
        sb.append("Note the (MISSING) at the end to designate that the classpath entry is missing");//$NON-NLS-1$
        
        
        try {
            // find all the patch readme files and print those out.
            Enumeration readmes =  ClassLoader.getSystemResources("patch_readme.txt");  //$NON-NLS-1$        
            sb.append( LINE_SEPARATOR );
            sb.append( LINE_SEPARATOR );

            sb.append("---- Patch Readme Entries----"); //$NON-NLS-1$
            sb.append( LINE_SEPARATOR );
            int cnt = 0;
            if (readmes != null) {
                
                while(readmes.hasMoreElements()) {
                    ++cnt;
                    URL url = (URL) readmes.nextElement();
                    sb.append("Patch " + url.getFile() + ":"); //$NON-NLS-1$ //$NON-NLS-2$
                    sb.append( LINE_SEPARATOR );
                    InputStream is = url.openStream();
                    byte[] data = ObjectConverterUtil.convertToByteArray(is);
                    sb.append(new String(data));
                    sb.append("-------------------------------------");//$NON-NLS-1$
                    sb.append( LINE_SEPARATOR );
                    is.close();

                }
            } 
            if (cnt == 0) {
                sb.append("no Patch Readme Entries found"); //$NON-NLS-1$                
            }
        } catch (IOException ioe) {
            
        }
        
        return sb.toString();
    }
    
}
