package org.teiid.resource.adapter.custom.spi;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.ResourceException;
import javax.resource.cci.ConnectionFactory;
import javax.resource.cci.ConnectionSpec;
import javax.resource.cci.RecordFactory;
import javax.resource.cci.ResourceAdapterMetaData;

public abstract class BasicConnectionFactory implements ConnectionFactory {
	private static final long serialVersionUID = 2900581028589520388L;
	private Reference reference;
	
	@Override
	public abstract BasicConnection getConnection() throws ResourceException;
	
	@Override
	public BasicConnection getConnection(ConnectionSpec arg0) throws ResourceException {
		throw new ResourceException("This operation not supported"); //$NON-NLS-1$;
	}

	@Override
	public ResourceAdapterMetaData getMetaData() throws ResourceException {
		throw new ResourceException("This operation not supported"); //$NON-NLS-1$;
	}

	@Override
	public RecordFactory getRecordFactory() throws ResourceException {
		throw new ResourceException("This operation not supported"); //$NON-NLS-1$
	}

	@Override
	public void setReference(Reference arg0) {
		this.reference = arg0; 
	}

	@Override
	public Reference getReference() throws NamingException {
		return this.reference;
	}
}
