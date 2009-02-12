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

package com.metamatrix.common.protocol.classpath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.net.UnknownServiceException;
import java.util.ArrayList;
import java.util.Enumeration;

import com.metamatrix.common.protocol.MMURLConnection;


/** 
 * Connection object to a resource in a Classpath.
 * can not use the PluginUtill logging beacuse their dependencies 
 * @since 4.4
 */
public class ClasspathURLConnection extends MMURLConnection  {

    public static String PROTOCOL = "classpath"; //$NON-NLS-1$
    
    /**
     * ctor 
     * @param u - URL to open the connection to
     */
    public ClasspathURLConnection(URL u) {
        super(u);
    }
    
    /**
     * Connect to the supplied URL 
     * @see java.net.URLConnection#connect()
     */
    public void connect() throws IOException {
        connected = true;
        
        if (action.equals(READ) || action.equals(LIST)) {
            // Check to make sure the resource exists.
            InputStream in = getResourceAsStream(url);
            if (in == null) {
                throw new FileNotFoundException(url.toString());
            }            
            in.close();
            
            doOutput = false;
            doInput = true;
        }
        else if (action.equals(WRITE)) {
            doOutput = true;
            doInput = false;
            String msg = "classpath protocol does not support write. Failed to write to:"+url; //$NON-NLS-1$
            throw new UnknownServiceException(msg); 
        }
        else if (action.equals(DELETE)) {
            String msg = "classpath protocol does not support delete. Failed to delete:"+url; //$NON-NLS-1$
            throw new UnknownServiceException(msg); 
        }        
    }
    
    /**  
     * @see java.net.URLConnection#getInputStream()
     */
    public InputStream getInputStream() throws IOException {
        if (!connected) {
            connect();
        }
      
        // Check if this is request to get list from directory
        if (action.equals(LIST)) {      
            
            ArrayList foundResouces = new ArrayList();
            Enumeration e = Thread.currentThread().getContextClassLoader().getResources(url.getPath());
            while(e.hasMoreElements()) {
                URL u = (URL)e.nextElement();
                foundResouces.add(u.toString());
            }
            // Build input stream from the object
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeObject(foundResouces.toArray(new String[foundResouces.size()]));
            oos.close();
            byte[] content = out.toByteArray();
            out.close();
            return new ByteArrayInputStream(content);            
        }
        
        // This must be read then, see if we can find the resource and
        // return the stream to it.                
        InputStream in = null;        
        in = getResourceAsStream(url);                
        if (in != null) {
            return in;
        }

        String msg = "Resource not found for reading:"+url; //$NON-NLS-1$
        throw new IOException(msg); 
    }

    /** 
     * By using the different look up mechnisms look for the resource
     * @param path
     * @return inputstream if the resource found, null otherwise
     */
    private InputStream getResourceAsStream(URL pathUrl) {
        InputStream in;
        
        String path = pathUrl.getPath();
        if (path.startsWith("/")) { //$NON-NLS-1$
            path = path.substring(1);
        }
        
        // First look in the thread's context class loader for the resource
        in = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);

        // then look for the resource in the this classe's class loader
        if (in == null) {
            in = this.getClass().getClassLoader().getResourceAsStream(path);
        }
        
        // then look in the system class loader
        if (in == null) {
            in = ClassLoader.getSystemResourceAsStream(path);
        }                        
        return in;
    }
        
}
