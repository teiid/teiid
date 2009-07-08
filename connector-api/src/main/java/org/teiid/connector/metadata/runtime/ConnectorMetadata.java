package org.teiid.connector.metadata.runtime;

import java.io.Serializable;
import java.util.Collection;

public interface ConnectorMetadata extends Serializable {

	ModelRecordImpl getModel();
	
	Collection<TableRecordImpl> getTables();
	
	Collection<ProcedureRecordImpl> getProcedures();
	
	//costing
	
}
