package org.teiid.example;

import javax.resource.ResourceException;
import javax.resource.cci.*;

public abstract class AbstractConnection implements Connection {

	@Override
	public Interaction createInteraction() throws ResourceException {
		return null;
	}

	@Override
	public LocalTransaction getLocalTransaction() throws ResourceException {
		return null;
	}

	@Override
	public ConnectionMetaData getMetaData() throws ResourceException {
		return null;
	}

	@Override
	public ResultSetInfo getResultSetInfo() throws ResourceException {
		return null;
	}
}
