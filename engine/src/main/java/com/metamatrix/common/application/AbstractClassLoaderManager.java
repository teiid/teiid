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

package com.metamatrix.common.application;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import com.metamatrix.common.classloader.PostDelegatingClassLoader;
import com.metamatrix.common.classloader.URLFilteringClassLoader;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.protocol.MetaMatrixURLStreamHandlerFactory;
import com.metamatrix.dqp.util.LogConstants;

public abstract class AbstractClassLoaderManager implements ClassLoaderManager {
	
	private ClassLoader parentClassLoader;
    private URLFilteringClassLoader commonExtensionClassloader;
    private Map<String, PostDelegatingClassLoader> postdelegationClassLoaderCache = new HashMap<String, PostDelegatingClassLoader>();
    private boolean usePostDelegationCache;
    private MetaMatrixURLStreamHandlerFactory factory;
    private Object lock = new Object();
    
    public AbstractClassLoaderManager(ClassLoader parentClassLoader, boolean usePostDelegationCache, boolean useStreamHandler) {
    	this.usePostDelegationCache = usePostDelegationCache;
    	this.parentClassLoader = parentClassLoader;
    	if (useStreamHandler) {
    		factory = new MetaMatrixURLStreamHandlerFactory();
    	}
    }
    
	public ClassLoader getCommonClassLoader(String urls) {
		synchronized (lock) {
			if (this.commonExtensionClassloader == null) {
	            // since we are using the extensions, get the common extension path 
				this.commonExtensionClassloader = new URLFilteringClassLoader(parseURLs(getCommonExtensionClassPath()), parentClassLoader, factory);
			}
			if (urls != null && urls.trim().length() > 0) {
	            for (URL url : parseURLs(urls)) {
	            	this.commonExtensionClassloader.addURL(url);
				}
			}
			return this.commonExtensionClassloader;
		}
	}
	
	public ClassLoader getPostDelegationClassLoader(String urls) {
		synchronized (lock) {
			PostDelegatingClassLoader cl = this.postdelegationClassLoaderCache.get(urls);
	    	if (cl == null) {
	    		if (urls != null && urls.trim().length() > 0) {
		            cl = new PostDelegatingClassLoader(parseURLs(urls), getCommonClassLoader(null), factory);
		            if (usePostDelegationCache) {
		            	this.postdelegationClassLoaderCache.put(urls, cl);
		            }
	    		}
	    		if (cl == null) {
	    			return getCommonClassLoader(null);
	    		}
	    	}
	    	return cl;
		}
	}

	public void clearCache() {
		synchronized (lock) {
			this.commonExtensionClassloader = null;
			this.postdelegationClassLoaderCache.clear();
		}
	}
		
	public URL[] parseURLs(String delimitedUrls) {
		StringTokenizer toke = new StringTokenizer(delimitedUrls, ";"); //$NON-NLS-1$
        List<URL> urls = new ArrayList<URL>(toke.countTokens());
        while (toke.hasMoreElements()) {
            String urlString = toke.nextToken();
	        try {
				URL url = parseURL(urlString);
				if (url != null) {
					urls.add(url);
				}
			} catch (MalformedURLException e) {
				LogManager.logError(LogConstants.CTX_EXTENSION_SOURCE, "Invalid extension classpath entry " + urlString); //$NON-NLS-1$
			}
        }
                
        return urls.toArray(new URL[urls.size()]);	
	}
	
	public abstract URL parseURL(String url) throws MalformedURLException;
	
	public abstract String getCommonExtensionClassPath();
		
}
