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

package com.metamatrix.common.extensionmodule.protocol.extensionjar;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.Permission;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.extensionmodule.protocol.URLFactory;

/**
 * This URLConnection actually gets the byte[] from an ExtensionModuleManager
 */
public class ExtensionJarURLConnection extends java.net.URLConnection {

//    private URLConnection resourceURLConnection;
    private ExtensionModuleManager manager;

    public ExtensionJarURLConnection(URL url, ExtensionModuleManager manager) throws MalformedURLException, IOException {
        super(url);
        this.manager = manager;
        
        // Defect 13370: jars were being cached in JVM and code was not replaced after jar was replaced in
        // extension module.  setDefaultUseCaches(false) fixes this problem.          
        setDefaultUseCaches(false);
    }

    public Permission getPermission()
        throws IOException{
        return new java.security.AllPermission();
    }

    public void connect()
        throws IOException{
        if(!super.connected)
        {
//            resourceURLConnection =
                getURL().openConnection();
            super.connected = true;
        }
    }

    public InputStream getInputStream() throws IOException {
        connect();
        return new BufferedInputStream(new ByteArrayInputStream( getBytes() ));
    }

    private byte[] getBytes() throws IOException {
        try {
            return this.manager.getSource(URLFactory.getFileName(getURL().getFile()));
        } catch (ExtensionModuleNotFoundException e) {
            throw new IOException(e.getMessage());

        } catch (MetaMatrixComponentException e) {
            throw new IOException(e.getMessage());
        }
    }

    public int getContentLength() {
        try {
            return getBytes().length;
        } catch (IOException ioe) {
            return -1;
        }
    }

}
