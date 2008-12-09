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


package com.metamatrix.connector.xml.file;

import java.util.Properties;

import com.metamatrix.connector.xml.CachingConnector;
import com.metamatrix.connector.xml.DocumentProducer;
import com.metamatrix.connector.xml.XMLExecution;
import com.metamatrix.connector.xml.base.XMLConnectionImpl;
import com.metamatrix.connector.xml.base.XMLConnectorStateImpl;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;


public class FileConnectorState extends XMLConnectorStateImpl {

	
	private String m_fileName;
	private String m_directoryPath;
	public static final String FILE_NAME = "FileName"; //$NON-NLS-1$
	public static final String DIRECTORY_PATH = "FilePath"; //$NON-NLS-1$
	
	public FileConnectorState() {
		super();
		setFileName(""); //$NON-NLS-1$
		setDirectoryPath(""); //$NON-NLS-1$
	}
	
	public Properties getState() {
		Properties props = super.getState();
		props.setProperty(FILE_NAME, getFileName());
		props.setProperty(DIRECTORY_PATH, getDirectoryPath());
		return props;		
	}
	
	public void setState(ConnectorEnvironment env) throws ConnectorException {
        super.setState(env);
        setFileName(env.getProperties().getProperty(FILE_NAME));
		setDirectoryPath(env.getProperties().getProperty(DIRECTORY_PATH));
	}

	/**
	 * @param m_fileName The m_fileName to set.
	 */
	public final void setFileName(String fileName) {
		if (fileName != null) {
            m_fileName = fileName;
        } else {
        	m_fileName = "";
        }
	}

	/**
	 * @return Returns the m_fileName.
	 */
	public final String getFileName() {
		return m_fileName;
	}

	/**
	 * @param m_directoryPath The m_directoryPath to set.
	 */
	public final void setDirectoryPath(String directoryPath) {
		if (directoryPath != null) {
             m_directoryPath = directoryPath;         
        } else {
        	m_directoryPath = "";
        }
   
	}

	/**
	 * @return Returns the m_directoryPath.
	 */
	public final String getDirectoryPath() {
		return m_directoryPath;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.XMLConnectorState#getExecutioner(com.metamatrix.connector.xml.ExecutionInfo)
	 */
	public DocumentProducer makeExecutor(XMLExecution execution) throws ConnectorException {
		return new FileExecutor(this, execution);
	}

	public Connection getConnection(CachingConnector connector, SecurityContext context, ConnectorEnvironment environment) throws ConnectorException {
		return new XMLConnectionImpl(connector, context, environment);
	}
}
