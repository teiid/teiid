package org.teiid.connector.api;

import org.teiid.connector.metadata.runtime.MetadataFactory;

public interface MetadataProvider {

	void getConnectorMetadata(MetadataFactory metadataFactory) throws ConnectorException;
	
}
