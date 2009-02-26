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
package com.metamatrix.common.protocol.jar;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLConnection;
import java.security.cert.Certificate;
import java.util.HashMap;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.metamatrix.common.protocol.MetaMatrixURLStreamHandlerFactory;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.common.protocol.mmfile.MMFileURLConnection;


/** 
 * Jar protocol connection class. The class extends the JDK provided JarURLConnection factory.
 * However Jar protocol can not understand the teiid specific protocols like mmfile, mmrofile etc.
 * The issue is if you can register application specific protocols during start-up 
 * using system properties the bulit in JAR protocol understands those protocols. 
 * However in the Teiid model this need to work by just dropping in in any 
 * container. This handler will be called for any class loaders registered 
 * with {@link MetaMatrixURLStreamHandlerFactory}, so that they understand the application specific protocols.   
 */
public class JarURLConnection extends java.net.JarURLConnection {
    
    public static String PROTOCOL = "jar"; //$NON-NLS-1$
    public static URL DUMMY_URL = null;
    URL jarFileURL = null;
    String jarEntry = null;
    private static HashMap jarMap = new HashMap();
    
    static {
        try {            
            DUMMY_URL = new URL(PROTOCOL, "", -1, "file:/dummy.foo!/"); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (MalformedURLException e) {
            // ignore
        }
    }
    
    /** 
     * @param url
     * @throws MalformedURLException
     * @since 4.3
     */
    protected JarURLConnection(URL url) throws MalformedURLException {
        // This super class is trying to construct a new URL() with out using the
        // URLHelper, and thus not finding the custom protocol handlers and exiting 
        // before rest of the construction, by throwing exception. this trick is to
        // avoid it. All the public class methods have been overridden in this class
        // such that all the base classes's functionality is duplicated, and base class
        // is just for marker interface (which JVM should have made as interface)
        super(DUMMY_URL);       

        // This is one line for which this whole jar protocol handler has been 
        // created.
        String path = url.getPath();
        int index = path.indexOf("!/"); //$NON-NLS-1$
        if (index != -1) {
            // This is where the jar file can be found
            jarFileURL = URLHelper.buildURL(path.substring(0, index));            
        }
        
        // this what somebody may be looking for as resource
        if (path.length() > (index+2)) {
            jarEntry = path.substring(index+2);
        }
    }

    /**  
     * @see java.net.JarURLConnection#getJarFileURL()
     */
    public URL getJarFileURL() {
        return jarFileURL;
    }
    
    /** 
     * @see java.net.URLConnection#connect()
     * @since 4.3
     */
    public void connect() throws IOException {
        connected = true;
    }

       
    public synchronized JarFile getJarFile() throws java.io.IOException {
        if (!connected) {
            connect();
        }

        // make sure do input not set to false
        if (!doInput) {
            throw new ProtocolException("Can not open JarFile if doInput is false"); //$NON-NLS-1$
        }
        
        JarFile jarFile = null;
        
        // get the Jar file URL
        URL jarFileURL = getJarFileURL();
        String protocol = jarFileURL.getProtocol();
        
        // if we are using the file based protocol then use the same
        // file as the jar file; otherwise persist to temp loacation and use it.        
        if ((protocol.equals("file") || protocol.equals(MMFileURLConnection.PROTOCOL)) ) { //$NON-NLS-1$
            jarFile = new JarFile(jarFileURL.getPath());
        } 
        else {
            jarFile = (JarFile)jarMap.get(jarFileURL);
            if (jarFile == null) {                
                URLConnection urlconn = jarFileURL.openConnection();
                InputStream is = urlconn.getInputStream();
                
                // create temporary directory for metamatrix
                File tempDirectory = new File(System.getProperty("mm.io.tmpdir")); //$NON-NLS-1$ 
                if (!tempDirectory.exists()) {
                    tempDirectory.mkdirs();
                }
                
                // stream the data in and create a temp file
                byte[] buf = new byte[4 * 1024];  
                File f = File.createTempFile("dqp", ".jar", tempDirectory); //$NON-NLS-1$ //$NON-NLS-2$
                FileOutputStream fos = new FileOutputStream(f);
                int len = 0;
                while ((len = is.read(buf)) != -1) {
                    fos.write(buf, 0, len);
                }
                fos.close();
                is.close();

                // Using the temp file create the jar file
                jarFile = new JarFile(f, true, ZipFile.OPEN_READ);                
                jarMap.put(jarFileURL, jarFile);
                f.deleteOnExit();
            }            
        }
        return jarFile;
    }

    /** 
     * @see java.net.URLConnection#getInputStream()
     * @since 4.3
     */
    public InputStream getInputStream() throws IOException {
        if (!connected) {
            connect();
        }

        // make sure we have valid flags
        if (!doInput) {
            throw new ProtocolException("Can not open InputStream if doInput is set to false"); //$NON-NLS-1$
        }

        // make sure we user requested for valid entry
        if (jarEntry == null || jarEntry.length() == 0) {
            throw new IOException("Entry name requested in the "+url+ " is wrong"); //$NON-NLS-1$ //$NON-NLS-2$
        }
        
        // now get the jar file, and look for the entry and return
        // stream to the resource.
        JarFile jarFile = getJarFile();
        ZipEntry entry = jarFile.getEntry(jarEntry);
        if (entry != null) {
            return jarFile.getInputStream(entry);
        }        
        return null;
    }
    
    
    public int getContentLength() {
        if (!connected) {
            return -1;
        }
        
        try {
            JarFile jarFile = getJarFile();
            ZipEntry entry = jarFile.getEntry(jarEntry);
            if (entry != null) {
                return (int)entry.getSize();
            }
        } catch (IOException e) {
            //eat it.
        }
        
        return -1;
    }

    /** 
     * @see java.net.JarURLConnection#getAttributes()
     * @since 4.3
     */
    public Attributes getAttributes() throws IOException {
        JarEntry e = getJarEntry();
        return e != null ? e.getAttributes() : null;
    }

    /** 
     * @see java.net.JarURLConnection#getCertificates()
     * @since 4.3
     */
    public Certificate[] getCertificates() throws IOException {
        JarEntry e = getJarEntry();
        return e != null ? e.getCertificates() : null;
    }

    /** 
     * @see java.net.JarURLConnection#getEntryName()
     * @since 4.3
     */
    public String getEntryName() {
        return jarEntry;
    }

    /** 
     * @see java.net.JarURLConnection#getJarEntry()
     * @since 4.3
     */
    public JarEntry getJarEntry() throws IOException {
        return getJarFile().getJarEntry(jarEntry);
    }

    /** 
     * @see java.net.JarURLConnection#getMainAttributes()
     * @since 4.3
     */
    public Attributes getMainAttributes() throws IOException {
        Manifest man = getManifest();
        return man != null ? man.getMainAttributes() : null;
    }

    /** 
     * @see java.net.JarURLConnection#getManifest()
     * @since 4.3
     */
    public Manifest getManifest() throws IOException {
        return getJarFile().getManifest();
    }    
}
