package org.teiid.translator.salesforce.execution;

import java.util.List;

import org.teiid.translator.TranslatorException;

public interface SalesforceProcedureExecution {

	static final int OBJECT = 0;
	static final int STARTDATE = 1;
	static final int ENDDATE = 2;
	static final int LATESTDATECOVERED = 3;

	List<?> getOutputParameterValues();

	List<?> next();

	void cancel();

	void close();

	void execute(ProcedureExecutionParent procedureExecutionParent) throws TranslatorException;

}
