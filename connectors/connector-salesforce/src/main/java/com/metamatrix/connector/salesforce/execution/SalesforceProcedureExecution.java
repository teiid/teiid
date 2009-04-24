package com.metamatrix.connector.salesforce.execution;

import java.util.List;

import org.teiid.connector.api.ConnectorException;

public interface SalesforceProcedureExecution {

	static final int OBJECT = 0;
	static final int STARTDATE = 1;
	static final int ENDDATE = 2;
	static final int LATESTDATECOVERED = 3;

	List<?> getOutputParameterValues();

	List<?> next();

	void cancel();

	void close();

	void execute(ProcedureExecutionParent procedureExecutionParent) throws ConnectorException;

}
