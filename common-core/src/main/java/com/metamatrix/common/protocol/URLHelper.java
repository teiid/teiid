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

package com.metamatrix.common.protocol;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.net.URLConnection;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;

import com.metamatrix.common.protocol.classpath.ClasspathURLConnection;
import com.metamatrix.common.protocol.mmfile.MMFileURLConnection;
import com.metamatrix.common.protocol.mmrofile.MMROFileURLConnection;
import com.metamatrix.core.io.FileUrl;

/**
 * Helper class to build the URL objects from the strings. Since as an
 * application we do not know if we are embedded or we are in our own server, we
 * can not install the "URLStreamHandlers" in the VM, as they can be only
 * installed once per VM, as an alternative, the stream handler must be
 * specified at the time URL it is constructed. This class will help us to this
 * code at one place.
 * 
 * Here inspect the given string and build the correct type of URL with correct
 * handler.
 * 
 * @since 4.4
 */
public class URLHelper {
	private static final String SEPARATOR = ":"; //$NON-NLS-1$

	static {
		// Very important that this property is set, so that loading of
		// custom extension module URLHandlers will work

		String propKey = "java.protocol.handler.pkgs"; //$NON-NLS-1$
		String directory = "com.metamatrix.common.protocol"; //$NON-NLS-1$

		String value = System.getProperty(propKey);
		if (value == null || value.trim().length() == 0) {
			System.setProperty(propKey, directory);
		} else if (value.indexOf(directory) < 0) {
			value = value + "|" + directory; //$NON-NLS-1$
			System.setProperty(propKey, value);
		}
	}

	/**
	 * Construct the URL based on the String
	 * 
	 * @param url
	 * @return
	 * @throws MalformedURLException
	 * @since 4.4
	 */
	public static URL buildURL(String url) throws MalformedURLException {

		if (url == null) {
			throw new MalformedURLException();
		}

		url = convertBackSlashes(url);

		if (url.startsWith(ClasspathURLConnection.PROTOCOL + SEPARATOR)) {
			String filename = url.substring((ClasspathURLConnection.PROTOCOL + SEPARATOR).length());
			return new URL(ClasspathURLConnection.PROTOCOL,"", -1, filename, new com.metamatrix.common.protocol.classpath.Handler()); //$NON-NLS-1$
		} else if (url.startsWith(MMFileURLConnection.PROTOCOL + SEPARATOR)) {
			String filename = url.substring((MMFileURLConnection.PROTOCOL + SEPARATOR).length());
			return new URL(MMFileURLConnection.PROTOCOL,"", -1, filename, new com.metamatrix.common.protocol.mmfile.Handler()); //$NON-NLS-1$            
		} else if (url.startsWith(MMROFileURLConnection.PROTOCOL + SEPARATOR)) {
            String filename = url.substring((MMROFileURLConnection.PROTOCOL + SEPARATOR).length());
            return new URL(MMROFileURLConnection.PROTOCOL,"", -1, filename, new com.metamatrix.common.protocol.mmrofile.Handler()); //$NON-NLS-1$            
        }
        else if (isFile(url)) {
			String filename = extractFileName(url);
			return new URL(MMFileURLConnection.PROTOCOL,"", -1, filename, new com.metamatrix.common.protocol.mmfile.Handler()); //$NON-NLS-1$                        
		}
		return new URL(url);
	}

	public static URL buildURL(URL context, String url)
			throws MalformedURLException {

		if (url == null) {
			throw new MalformedURLException();
		}

		if (context == null) {
			return buildURL(url);
		}

		url = convertBackSlashes(url);

		if (url.startsWith(ClasspathURLConnection.PROTOCOL + SEPARATOR)) {
			String filename = url.substring((ClasspathURLConnection.PROTOCOL + SEPARATOR).length());
			return new URL(ClasspathURLConnection.PROTOCOL,"", -1, filename, new com.metamatrix.common.protocol.classpath.Handler()); //$NON-NLS-1$
		} else if (url.startsWith(MMFileURLConnection.PROTOCOL + SEPARATOR)) {
			String filename = url.substring((MMFileURLConnection.PROTOCOL + SEPARATOR).length());
			return new URL(MMFileURLConnection.PROTOCOL,"", -1, filename, new com.metamatrix.common.protocol.mmfile.Handler()); //$NON-NLS-1$            
		} else if (url.startsWith(MMROFileURLConnection.PROTOCOL + SEPARATOR)) {
            String filename = url.substring((MMROFileURLConnection.PROTOCOL + SEPARATOR).length());
            return new URL(MMROFileURLConnection.PROTOCOL,"", -1, filename, new com.metamatrix.common.protocol.mmrofile.Handler()); //$NON-NLS-1$            
        } else if (isFile(url)) {
			String filename = extractFileName(url);
			String contextURL = context.toString();

			if (contextURL.startsWith(ClasspathURLConnection.PROTOCOL+ SEPARATOR)) {
				return new URL(context, filename,new com.metamatrix.common.protocol.classpath.Handler()); 
			} else if (contextURL.startsWith(MMFileURLConnection.PROTOCOL+ SEPARATOR)) {
				return new URL(context, filename,new com.metamatrix.common.protocol.mmfile.Handler()); 
			} else if (contextURL.startsWith(MMROFileURLConnection.PROTOCOL+ SEPARATOR)) {
				return new URL(URLHelper.buildURL(context.getPath()), filename,new com.metamatrix.common.protocol.mmfile.Handler());
			}
		}
		return new URL(context, url);

	}

	public static URL buildURL(URL url) {
		try {
			return buildURL(url.toExternalForm());
		} catch (MalformedURLException e) {
			// since it came as url it should not have any issues with this
		}
		return null;
	}

    /**
     * Determines whether a URL object resolves to a valid url. This will work
     * for any protocol (file, HTTP, etc.).
     * 
     * @param url
     * @param userName
     * @param password
     * @param verifyHostname whether to verify hostname for HTTPS connection
     * @return resolved boolean
     * @throws MalformedURLException, IOException 
     * @since 5.1
     */
    public static boolean resolveUrl(final URL url, final String userName, final String password, boolean verifyHostname) throws MalformedURLException, IOException{
        if(userName != null && userName.length() != 0 && password != null) {
            Authenticator.setDefault( new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(userName, password.toCharArray());
                }
            });
        }
        return resolveUrl(url, verifyHostname);
    }
    
    /**
     * Determines whether a URL object resolves to a valid url. This will work
     * for any protocol (file, HTTP, etc.).
     * 
     * @param url
     * @return resolved boolean
     * @throws MalformedURLException, IOException 
     * @since 5.1
     */
    public static boolean resolveUrl(final URL url) throws MalformedURLException, IOException{
        return resolveUrl(url, true);
    }
    
	/**
	 * Determines whether a URL object resolves to a valid url. This will work
	 * for any protocol (file, HTTP, etc.).
	 * 
	 * @param url
     * @param verifyHostname whether to verify hostname for HTTPS connection
	 * @return resolved boolean
	 * @throws MalformedURLException, IOException 
	 * @since 5.5
	 */
	public static boolean resolveUrl(final URL url, boolean verifyHostname) throws MalformedURLException, IOException{
		boolean resolved = true;
		if (url==null){
			return resolved;
		}
		String nextLine;
		URLConnection urlConn = null;
		InputStreamReader inStream = null;
		BufferedReader buff = null;
        // Add a time-out here....
        long timeOut = 30000;
        
        long startTime = System.currentTimeMillis();
        long deltaTime = 0;
		try {
			urlConn = url.openConnection();
            if(!verifyHostname && urlConn instanceof HttpsURLConnection) {
                ((HttpsURLConnection)urlConn).setHostnameVerifier(new HostnameVerifier() {
                    public boolean verify(String arg, SSLSession session) {
                        return true;
                    }
                });
            }
			inStream = new InputStreamReader(urlConn.getInputStream());
			buff = new BufferedReader(inStream);
			boolean keepReading = true;
			// Read and print the lines from index.html
			while (keepReading ) {
				nextLine = buff.readLine();
				if (nextLine != null) {
					
				} else {
					break;
				}
                deltaTime  = System.currentTimeMillis() - startTime;
                if( deltaTime > timeOut ) {
                    keepReading = false;
                    resolved = false;
                }
			}
		} finally {
				if (inStream != null){
					inStream.close();
				}
		}
		
		return resolved;
	}
    
	/**
     * Download the content from the given URL and save it into the specified file.
     * @param url URL of the file to be saved
     * @param filePath the full path of the file name 
     * @param userName user name if authentication is required
     * @param password password if authentication is required
     * @param verifyHostname whether to verify hostname for HTTPS connection
     * @return the file created
     * @throws MalformedURLException
     * @throws IOException
     * @since 5.5
	 */
    public static File createFileFromUrl(final URL url, 
                                         final String filePath, final String userName, final String password, boolean verifyHostname) throws MalformedURLException, IOException{
        if(userName != null && userName.length() != 0 && password != null) {
            Authenticator.setDefault( new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(userName, password.toCharArray());
                }
            });
        }
        File file = null;
        String tempDir = System.getProperty("java.io.tmpdir");//$NON-NLS-1$
        if(filePath.indexOf("/") != -1 || filePath.indexOf("\\") != -1 ) {//$NON-NLS-1$//$NON-NLS-2$
            
            int lastPart = filePath.lastIndexOf("/");//$NON-NLS-1$
            if(lastPart == -1) {
                lastPart = filePath.lastIndexOf("\\");//$NON-NLS-1$
            }
            String relativeDir = filePath.substring(0, lastPart);
            File dir = new File(new File(tempDir), relativeDir);
            if(!dir.exists()) {
                dir.mkdirs();
            }
            file = new File(dir, filePath.substring(lastPart + 1));
        }else{
            file = new File(new File(tempDir), filePath);
        }
        return createFileFromUrlInternal(url, new FileUrl(file.toURI()), verifyHostname);
    }

	/**
	 * Build a {@link java.io.File} from a {@link java.net.URL} object.
	 *   
	 * @param url
	 * @param fileNamePrefix
	 * @param fileNameSuffix
	 * @return File
	 * @since 5.1
	 */
	public static File createFileFromUrl(final URL url, 
										 final String fileNamePrefix, 
										 final String fileNameSuffix) throws MalformedURLException, IOException{
		
        return createFileFromUrlInternal(url, FileUrl.createTempFile(fileNamePrefix,fileNameSuffix), true);
	}
	
    private static File createFileFromUrlInternal(final URL url, final File file, boolean verifyHostname) throws MalformedURLException, IOException{
        String nextLine;
        URLConnection urlConn = null;
        InputStreamReader inStream = null;
        BufferedReader buff = null;
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            file.deleteOnExit();
            ((FileUrl)file).setOriginalUrlString(url.toString());
            fw = new FileWriter(file);
            bw = new BufferedWriter(fw);
            urlConn = url.openConnection();
            if(!verifyHostname && urlConn instanceof HttpsURLConnection) {
                ((HttpsURLConnection)urlConn).setHostnameVerifier(new HostnameVerifier() {
                    public boolean verify(String arg, SSLSession session) {
                        return true;
                    }
                });
            }
            inStream = new InputStreamReader(urlConn.getInputStream());
            buff = new BufferedReader(inStream);

            // Read and print the lines from index.html
            while (true) {
                nextLine = buff.readLine();
                if (nextLine != null) {
                    bw.write(nextLine);
                } else {
                    break;
                }
            }
        } finally {
            if (inStream!=null){
                inStream.close();
            }
            if (bw!=null){
                    bw.close();
            }
        }
        
        return file;
    }
    
	static boolean isFile(String file) {
		return extractFileName(file) != null;
	}

	static String extractFileName(String file) {

		if (file.matches("^(\\w){2,}:.*")) { //$NON-NLS-1$ 
			// Handles URLs - No conversion nessasary
			// http://lib/foo.txt - currently do not support, converts to local
			// host with absolute path
			// file://lib/foo.txt
			// file:///c:/lib/foo.txt
			return null;
		} else if (file.matches("^\\/.*")) { //$NON-NLS-1$
			// Handles absolute paths- it can be file or URL depending upon
			// context Conversion needed
			// /lib/foo.txt
			return file;
		} else if (file.matches("^\\w:[\\\\,\\/].*")) { //$NON-NLS-1$
			// Handles windows absolute path - no conversion needed
			// c:\\lib\\foo.txt
			// c:/lib.foo.txt
			file = file.replaceAll("\\\\", "\\/"); //$NON-NLS-1$ //$NON-NLS-2$
			return "/" + file; //$NON-NLS-1$
		} else if (file.matches("^(\\.)+\\/.*|^\\w+\\/.*|^\\w+.*")) { //$NON-NLS-1$
			// Handles relative paths - these can be urls or files - conversion
			// nessasary
			// ./lib/foo.txt
			// ../lib/foo.txt
			// lib/foo.txt
			return file;
		}
		return null;
	}

	static String convertBackSlashes(String str) {
		return str.replaceAll("\\\\", "/"); //$NON-NLS-1$ //$NON-NLS-2$
	}
}
