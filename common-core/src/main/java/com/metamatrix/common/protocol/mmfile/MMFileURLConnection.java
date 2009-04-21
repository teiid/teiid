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

package com.metamatrix.common.protocol.mmfile;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilePermission;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.Permission;
import java.util.Arrays;
import java.util.Comparator;
import java.util.StringTokenizer;

import com.metamatrix.common.protocol.MMURLConnection;
import com.metamatrix.common.protocol.URLHelper;

/** 
 * Metamatrix's own implementation of the "file:" URL handler. The reason for
 * a different handler is to support the "output stream" as the sun supplied one
 * does not handle writing to it.
 *  
 *  Strings are not externalized because of the fact that we have huge dependencies 
 *  with our plugin stuff to eclipse.
 *  
 * @since 4.4
 */
public class MMFileURLConnection extends MMURLConnection {
	public static String PROTOCOL = "mmfile"; //$NON-NLS-1$
    File file;
    File deleted;
    public static String DELETED = ".deleted"; //$NON-NLS-1$
    boolean readOnly = false;
    
    /**
     * ctor 
     * @param u - URL to open the connection to
     */
    public MMFileURLConnection(URL u) throws MalformedURLException, IOException {
        this(u, false);
    }
    
    public MMFileURLConnection(URL u, boolean readOnly) throws MalformedURLException, IOException {
        super(u);

        String path = url.getPath();
        file = new File(path.replace('/', File.separatorChar).replace('|', ':'));
        deleted = new File(file.getAbsolutePath()+DELETED);
        doOutput = false;        
        this.readOnly = readOnly;
    }    

    /**
     * Returns the underlying file for this connection.
     */
    public File getFile() {
        return file;
    }

    /**
     * Marks that connected
     */        
    public void connect() throws IOException {
        connected = true;        
        
        if (action.equals(READ) || action.equals(LIST)) {
            if (!file.exists()) {
                throw new FileNotFoundException(file.getPath());
            }
            
            // we know original file exists, but check if there is any .deleted file
            // which also says that this file is no longer accessable.            
            if (deleted.exists()) {
                throw new FileNotFoundException(file.getPath());
            }

            doOutput = false;
            doInput = true;
        }
        else if (action.equals(WRITE)) {
            if (!this.readOnly) {
                if (!file.exists() || (file.exists() && deleted.exists())) {
                    // if there is deleted file remove it first.
                    deleted.delete();
                    
                    // now write the file.
                    File parent = file.getParentFile();
                    if (parent != null && !parent.exists()) {
                        parent.mkdirs();
                    }
                }
            }
            doOutput = true;
            doInput = false;            
        }
        else if (action.equals(DELETE)) {
            if (!this.readOnly) {
                if (file.exists()) {
                    file.delete();
                    
                    if (file.exists()) {
                        try {
                            FileWriter fw = new FileWriter(deleted); 
                            fw.write("failed to delete file:\""+url+"\"; this is marker to note this file has been deleted"); //$NON-NLS-1$ //$NON-NLS-2$
                            fw.close();
                        } catch (IOException e) {
                            throw new IOException("failed to delete file:"+url); //$NON-NLS-1$
                        }                    
                    }
                }
            }
        }
    }

    /**
     * @see java.net.URLConnection#getInputStream()
     */
    public InputStream getInputStream() throws IOException {
        if (!connected) {
            connect();
        }
        
        // If the action was to read 
        if (action.equals(LIST)) {
            // Construct a filter;
            FileFilter fileFilter = new FileFilter() {
                String filter = props.getProperty("filter"); //$NON-NLS-1$                
                public boolean accept(File pathname) {
                    StringTokenizer st = new  StringTokenizer(filter, ","); //$NON-NLS-1$
                    while (st.hasMoreTokens()) {
                        String token = st.nextToken().trim().toLowerCase();
                        if (pathname.getPath().toLowerCase().endsWith(token)) {
                            return true;
                        }
                    }
                    return false;
                }                
            };

            File[] matchedFiles = file.listFiles(fileFilter);
            String[] urls = new String[matchedFiles.length];
            
            String sort = props.getProperty(FILE_LIST_SORT, DATE);
            if (sort.equals(DATE)) { 
            	Arrays.sort(matchedFiles, new Comparator<File>() {
        			//## JDBC4.0-begin ##
        			@Override
        			//## JDBC4.0-end ##
					public int compare(File o1, File o2) { 
						return Long.valueOf(o2.lastModified()).compareTo(o1.lastModified()); // latest first.
					}
            	});
            } else if (sort.equals(ALPHA)) {
            	Arrays.sort(matchedFiles, new Comparator<File>() {
        			//## JDBC4.0-begin ##
        			@Override
        			//## JDBC4.0-end ##
					public int compare(File o1, File o2) {
						return o1.getName().compareTo(o2.getName()); 
					}
            	});
            	
            } else if (sort.equals(REVERSEALPHA)) { 
            	Arrays.sort(matchedFiles, new Comparator<File>() {
        			//## JDBC4.0-begin ##
        			@Override
        			//## JDBC4.0-end ##
					public int compare(File o1, File o2) { 
						return o2.getName().compareTo(o1.getName());
					}
            	});            	
            }
            
            for (int i = 0; i < matchedFiles.length; i++) {
                urls[i] = URLHelper.buildURL(url, matchedFiles[i].getName()).toString();
            }
            
            // Build input stream from the object
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeObject(urls);
            oos.close();
            byte[] content = out.toByteArray();
            out.close();
            return new ByteArrayInputStream(content);
        }
        
        // make sure we only return the stream for non-deleted files
        if (!deleted.exists() && file.exists()) {
            return new FileInputStream(file);
        }
        return null;
    }

    /**  
     * @see java.net.URLConnection#getOutputStream()
     */
    public OutputStream getOutputStream() throws IOException {
        if (!connected) {
            connect();
        }
        if (action.equals(WRITE)) {
            
            // make sure we are not read only, if it is serve dummy stream
            if (this.readOnly) {
                return new NullOutputStream();
            }

            // this not readonly, so go ahead write..
            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                // Check for write access
                FilePermission p = new FilePermission(file.getPath(), "write"); //$NON-NLS-1$
                sm.checkPermission(p);
            }
            return new FileOutputStream(file);
        }
        throw new IOException("Writing to the file \""+url+"\" is not allowed"); //$NON-NLS-1$ //$NON-NLS-2$        
    }

    /**
     * Provides support for returning the value for the
     * <tt>last-modified</tt> header.
     */
    public String getHeaderField(final String name) {
        String headerField = null;
        if (name.equalsIgnoreCase("last-modified")) //$NON-NLS-1$
            headerField = String.valueOf(getLastModified());
        else if (name.equalsIgnoreCase("content-length")) //$NON-NLS-1$
            headerField = String.valueOf(file.length());
        else if (name.equalsIgnoreCase("content-type")) { //$NON-NLS-1$
            headerField = getFileNameMap().getContentTypeFor(file.getName());
            if (headerField == null) {
                try {
                    InputStream is = getInputStream();
                    BufferedInputStream bis = new BufferedInputStream(is);
                    headerField = URLConnection.guessContentTypeFromStream(bis);
                    bis.close();
                } catch (IOException e) {
                }
            }
        } else if (name.equalsIgnoreCase(DATE)) 
            headerField = String.valueOf(file.lastModified());
        else {
            // This always returns null currently
            headerField = super.getHeaderField(name);
        }
        return headerField;
    }

    /** 
     * Return a permission for reading of the file
     */
    public Permission getPermission() throws IOException {
        return new FilePermission(file.getPath(), "read"); //$NON-NLS-1$
    }

    /**
     * Returns the last modified time of the underlying file.
     */
    public long getLastModified() {
        return file.lastModified();
    }
    
    /**
     * a no land output stream.. 
     */
    static class NullOutputStream extends OutputStream{
        public void write(int b) throws IOException {
            // ha ha I do nothing..
        }        
    }    
}