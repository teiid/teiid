package com.metamatrix.connector.salesforce.execution;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.IParameter;
import org.teiid.connector.language.IProcedure;

/**
 * 
 * The structure of the getDeleted procedure is:
 * Salesforce object type: String: IN param
 * startDate: datatime: IN param
 * enddate: datetime: IN param
 * earliestDateAvailable: datetime: OUT param
 * latestDateCovered: datetime: OUT param
 * getUpdatedResult: resultset: OUT param 
 *
 */

public class GetDeletedExecutionImpl implements SalesforceProcedureExecution {

	private static final int EARLIESTDATEAVAILABLE = 4;
	private static final int RESULT = 5;
	
	private ProcedureExecutionParent parent;

	private DeletedResult deletedResult;
	private int resultIndex = 0;
	
	public GetDeletedExecutionImpl(
			ProcedureExecutionParent procedureExecutionParent) {
		this.parent = procedureExecutionParent;
	}

	@Override
	public void cancel() {
		// nothing to do here
		
	}

	@Override
	public void close() {
		// nothing to do here
		
	}

	@Override
	public void execute(ProcedureExecutionParent procedureExecutionParent) throws ConnectorException {
		IProcedure command = parent.getCommand();
		List<IParameter> params = command.getParameters();
		
		IParameter object = (IParameter) params.get(OBJECT);
		String objectName = (String) object.getValue();
		
		IParameter start = (IParameter) params.get(STARTDATE);
		Timestamp startTime = (Timestamp) start.getValue();
		Calendar startCalendar = GregorianCalendar.getInstance();
		startCalendar.setTime(startTime);
		
		IParameter end = (IParameter) params.get(ENDDATE);
		Timestamp endTime = (Timestamp) end.getValue();
		Calendar endCalendar = GregorianCalendar.getInstance();
		endCalendar.setTime(endTime);
		
		deletedResult = parent.getConnection().getDeleted(objectName, startCalendar, endCalendar);	
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<?> getOutputParameterValues() {
		List result = new ArrayList();
		result.add(deletedResult.getLatestDateCovered());
		result.add(deletedResult.getEarliestDateAvailable());
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<?> next() {
		List result = null;
		if(deletedResult.getResultRecords() != null && resultIndex < deletedResult.getResultRecords().size()){
			result = new ArrayList(2);
			result.add(deletedResult.getResultRecords().get(resultIndex).getID());
			result.add(deletedResult.getResultRecords().get(resultIndex).getDeletedDate());
			resultIndex++;
		}
		return result;
	}

}
