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


package com.metamatrix.connector.xml.base;


import java.util.Date;

import com.metamatrix.connector.xml.cache.DocumentCache;

public class XMLDocument implements DocumentCache.EventSinkFactory, java.io.Serializable {

	
    private Object m_contextRoot;
    private Date m_timeCreated;
    private transient FileLifeManager[] m_externalFiles;
    
    public XMLDocument() {        
	}

    static class CacheEventSink implements DocumentCache.EventSink
    {
        private FileLifeManager[] m_externalFiles;
        CacheEventSink(FileLifeManager[] externalFiles)
        {
        	m_externalFiles = externalFiles;
        }

        public void onDelete()
        {
            // We don't actually need to do anything here.
            // The event sink merely needed to retain references to the FileLifeManagers,
            // to prevent the files being deleted. We delete the references here,
            // although we don't actually ned to, because the entire event sink
            // object is going to become unreachable.
            m_externalFiles = new FileLifeManager[0];
        }
        
        public void onRestoreFromFile(Object o)
        {
        	XMLDocument doc = (XMLDocument)o;
            doc.m_externalFiles = m_externalFiles;
        }
    }

    public DocumentCache.EventSink getEventSink()
    {
        return new CacheEventSink(m_externalFiles);
    }
    
    // The contextRoot must be an object the the xpath processor can use
    // as a context root, such as Element or Document
    public XMLDocument(Object contextRoot, FileLifeManager[] externalFiles)
    {
    	setContextRoot(contextRoot);
        setExternalFiles(externalFiles);
    }


    // The contextRoot must be an object the the xpath processor can use
    // as a context root, such as Element or Document
	public void setContextRoot(Object contextRoot) {
		m_contextRoot = contextRoot;
	}

	public Object getContextRoot() {
		return m_contextRoot;
	}


    public void setExternalFiles(FileLifeManager[] externalFiles) {
        m_externalFiles = externalFiles;
    }


    public FileLifeManager[] getExternalFiles() {
        return m_externalFiles;
    }
}
