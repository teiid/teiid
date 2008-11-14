/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;
import java.util.StringTokenizer;



/** 
 * Base URL Connection Class
 * @since 4.4
 */
public abstract class MMURLConnection extends URLConnection {
   protected static final String READ = "read";      //$NON-NLS-1$
   protected static final String WRITE = "write";    //$NON-NLS-1$
   protected static final String LIST = "list";      //$NON-NLS-1$
   protected static final String DELETE = "delete";  //$NON-NLS-1$
    
   protected String action = READ;
   protected Properties props = null;
    
    public MMURLConnection(URL url) {
        super(url);
        
        // If the URL has any query part parse it and handle it.
        String query = url.getQuery();
        if (query != null) {
            StringTokenizer st = new StringTokenizer(query, "&"); //$NON-NLS-1$
            while (st.hasMoreTokens()) {
                String option = st.nextToken();
                int index = option.indexOf('=');
                if (index != -1) {
                    String task = option.substring(0, index);
                    String value =  option.substring(index+1);
                    if (task.equals("action")) {       //$NON-NLS-1$
                        action = value;
                    }
                    else {
                        if (props == null) {
                            props = new Properties();
                        }
                        props.setProperty(task, value);
                    }
                }                
            }
        }        
    }         
}
