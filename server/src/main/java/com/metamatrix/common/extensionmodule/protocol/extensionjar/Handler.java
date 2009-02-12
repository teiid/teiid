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

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.common.extensionmodule.protocol.URLFactory;

/**
 * A URLStreamHandler to make connections to the MetaMatrix Extension
 * Module Store(s) to retrieve class or resources.  An instance of this
 * will be instantiated reflexively by the classloader framework, as 
 * such it must retrieve it's ExtensionModuleManager instance from the
 * ExtensionModuleManager.getInstance() method.
 */
public class Handler extends URLStreamHandler {

    private ExtensionModuleManager manager;

    public Handler(){
        this.manager = ExtensionModuleManager.getInstance();
    }

    protected URLConnection openConnection(URL url)
        throws IOException{
        return new ExtensionJarURLConnection(url, manager);
    }

    protected void parseURL(URL url, String s, int i, int j){
        setURL(url, URLFactory.MM_JAR_PROTOCOL, "", -1, null, null, URLFactory.getFileName(s), "", ""); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
}
