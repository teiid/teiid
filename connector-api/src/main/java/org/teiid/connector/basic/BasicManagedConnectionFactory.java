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
package org.teiid.connector.basic;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterAssociation;
import javax.resource.spi.ValidatingManagedConnectionFactory;
import javax.security.auth.Subject;

import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.language.LanguageFactory;

import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.ReflectionHelper;

public class BasicManagedConnectionFactory implements ManagedConnectionFactory, ResourceAdapterAssociation, ConnectorEnvironment, ValidatingManagedConnectionFactory {

	private static final long serialVersionUID = -7302713800883776790L;
	private static final TypeFacility TYPE_FACILITY = new TypeFacilityImpl();
	private ConnectorLogger logger = new DefaultConnectorLogger();
	private PrintWriter log;
	private BasicResourceAdapter ra;
	
	// Properties set by ra.xml 
	private String connectorClass;
	private String capabilitiesClass;
	private boolean immutable = false;
	private boolean exceptionOnMaxRows = false;
	private int maxResultRows = -1;
	private boolean xaCapable;
	
	private String overrideCapabilitiesFile;
	
	// derived
	private Properties overrideCapabilities;
	
	@Override
	public Object createConnectionFactory() throws ResourceException {
		return new ResourceException("Resource Adapter does not currently support running in a non-managed environment.");
	}

	@Override
	public Object createConnectionFactory(ConnectionManager arg0) throws ResourceException {
		try {
			Object o = ReflectionHelper.create(this.connectorClass, null, Thread.currentThread().getContextClassLoader());
			if(!(o instanceof Connector)) {
				throw new ConnectorException("Invalid Connector class specified="+this.connectorClass);
			}
			Connector connector = (Connector)o;
			connector.initialize(this);
			return new WrappedConnector(connector, arg0, this);
		} catch (MetaMatrixCoreException e) {
			throw new ResourceException(e);
		} 
	}

	@Override
	public ManagedConnection createManagedConnection(Subject arg0, ConnectionRequestInfo arg1) throws ResourceException {
		return new BasicManagedConnection(this);
	}

	@Override
	public PrintWriter getLogWriter() throws ResourceException {
		return this.log;
	}

	@Override
	public ManagedConnection matchManagedConnections(Set arg0, Subject arg1, ConnectionRequestInfo arg2) throws ResourceException {
		// rameshTODO: to manage per-user based pools the information in the request info needs to be used to filter.
		return (ManagedConnection)arg0.iterator().next();
	}

	@Override
	public void setLogWriter(PrintWriter arg0) throws ResourceException {
		this.log = arg0;
	}

	@Override
	public ResourceAdapter getResourceAdapter() {
		return this.ra;
	}

	@Override
	public void setResourceAdapter(ResourceAdapter arg0) throws ResourceException {
		this.ra = (BasicResourceAdapter)arg0;
	}
	
	public void setConnectorClass(String arg0) {
		this.connectorClass = arg0;
	}
	
	public void setCapabilitiesClass(String arg0) {
		this.capabilitiesClass = arg0;
	}

	public void setImmutable(Boolean arg0) {
		this.immutable = arg0.booleanValue();
	}	
	
	public void setExceptionOnMaxRows(Boolean arg0) {
		this.exceptionOnMaxRows = arg0.booleanValue();
	}
	
	public void setMaxResultRows(Integer arg0) {
		this.maxResultRows = arg0.intValue();
	}
	
	public String getCapabilitiesClass() {
		return capabilitiesClass;
	}

	public boolean isImmutable() {
		return immutable;
	}

	public boolean isExceptionOnMaxRows() {
		return exceptionOnMaxRows;
	}

	public int getMaxResultRows() {
		return maxResultRows;
	}
	
	public boolean isXaCapable() {
		return xaCapable;
	}

	public void setXaCapable(Boolean arg0) {
		this.xaCapable = arg0.booleanValue();
	}	
	
	@Override
	public LanguageFactory getLanguageFactory() {
		return LanguageFactory.INSTANCE;
	}

	@Override
	public ConnectorLogger getLogger() {
		return logger;
	}

	@Override
	public TypeFacility getTypeFacility() {
		return TYPE_FACILITY;
	}

	@Override
	public Properties getOverrideCapabilities() throws ConnectorException {
		if (this.overrideCapabilities == null && this.overrideCapabilitiesFile != null) {
			try {
				this.overrideCapabilities = new Properties();
				this.overrideCapabilities.loadFromXML(this.getClass().getResourceAsStream(this.overrideCapabilitiesFile));
			} catch (IOException e) {
				throw new ConnectorException(e);
			}
		}
		return this.overrideCapabilities;
	}
	
	public void setOverrideCapabilitiesFile(String propsFile) {
		this.overrideCapabilitiesFile = propsFile;
	}
	
    public static <T> T getInstance(Class<T> expectedType, String className, Collection ctorObjs, Class defaultClass) throws ConnectorException {
    	try {
	    	if (className == null) {
	    		if (defaultClass == null) {
	    			throw new ConnectorException("Neither class name or default class specified to create an instance");
	    		}
	    		return expectedType.cast(defaultClass.newInstance());
	    	}
	    	return expectedType.cast(ReflectionHelper.create(className, ctorObjs, Thread.currentThread().getContextClassLoader()));
		} catch (MetaMatrixCoreException e) {
			throw new ConnectorException(e);
		} catch (IllegalAccessException e) {
			throw new ConnectorException(e);
		} catch(InstantiationException e) {
			throw new ConnectorException(e);
		}    	
    }

	@Override
	public Set<BasicManagedConnection> getInvalidConnections(Set arg0) throws ResourceException {
		HashSet<BasicManagedConnection> result = new HashSet<BasicManagedConnection>();
		for (Object object : arg0) {
			if (object instanceof BasicManagedConnection) {
				BasicManagedConnection bmc = (BasicManagedConnection)object;
				if (!bmc.isValid()) {
					result.add(bmc);
				}
			}
		}
		return result;
	}	
}
