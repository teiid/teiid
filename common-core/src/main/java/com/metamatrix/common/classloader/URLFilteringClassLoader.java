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

package com.metamatrix.common.classloader;

import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;
import java.util.ArrayList;
import java.util.HashSet;

import com.metamatrix.common.protocol.classpath.ClasspathURLConnection;
import com.metamatrix.common.protocol.mmfile.MMFileURLConnection;
import com.metamatrix.common.protocol.mmrofile.MMROFileURLConnection;


/** 
 * @since 4.3
 */
public class URLFilteringClassLoader extends URLClassLoader {

    private static HashSet<String> excludeProtocols = null;
    
    static {
        excludeProtocols = new HashSet<String>(); 
        excludeProtocols.add("extensionjar"); //$NON-NLS-1$
        excludeProtocols.add(ClasspathURLConnection.PROTOCOL);
        excludeProtocols.add(MMFileURLConnection.PROTOCOL);
        excludeProtocols.add(MMROFileURLConnection.PROTOCOL);        
    }
    
    /** 
     * @param urls
     * @param parent
     * @since 4.3
     */
    public URLFilteringClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }
    
    /** 
     * @param urls
     * @since 4.3
     */
    public URLFilteringClassLoader(URL[] urls) {
        super(urls);
    }

    public URLFilteringClassLoader(URL[] urls, ClassLoader parent, URLStreamHandlerFactory factory) {
        super(urls, parent, factory);
    }    
    
    public URL[] getURLs() {
        /* Description from defect 17763 (John Doyle) - extensionjar protocol causes exception in JBOSS
         * The Librados Connector sets the com.metamatrix.common.classloaderNonDelegatingClassLoader as the
         * context class loader.  This is requires so that InitialContextFactory class can be found. 
         * However the JBOSS server receives the list of URLs contained in this classloader and tries to creat
         * a URL object for each of the jars.  An exception is throw because extensionjar is not a valid protocol.
         * The librados connector cannot work until this is fixed.
         * 
         * I have a fix coded and dev tested that resolves the problem.  It overrides the getURLs call that
         * JBOSS is using to filter out our extensionjar URLs.  Interestingly, WebLogic does not use this
         * function, so I think JBOSS may be doing something wrong here.
         */
        ArrayList<URL> temp = new ArrayList<URL>();
        URL[] all = super.getURLs();
        for (int i = 0; i < all.length; i++) {
            String protocol = all[i].getProtocol();
            if (!excludeProtocols.contains(protocol)) {
                temp.add(all[i]);
            }            
        }
        return temp.toArray(new URL[temp.size()]);
    }
    
    @Override
    public void addURL(URL url) {
    	super.addURL(url);
    }

}
