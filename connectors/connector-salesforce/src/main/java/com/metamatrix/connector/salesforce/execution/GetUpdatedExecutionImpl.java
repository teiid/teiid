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
 * The structure of the getUpdated procedure is:
 * Salesforce object type: String: IN param
 * startDate: datatime: IN param
 * enddate: datetime: IN param
 * latestDateCovered: datetime: OUT param
 * getUpdatedResult: resultset: OUT param 
 *
 */

public class GetUpdatedExecutionImpl implements SalesforceProcedureExecution {

	private static final int RESULT = 4;
	
	private ProcedureExecutionParent parent;
	private UpdatedResult updatedResult;
	private int idIndex = 0;

	public GetUpdatedExecutionImpl(
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
		
		updatedResult = parent.getConnection().getUpdated(objectName, startCalendar, endCalendar);
	}

	@Override
	public List<?> getOutputParameterValues() {
		List result = new ArrayList(1);
		result.add(updatedResult.getLatestDateCovered());
		return result;
	}

	@Override
	public List<?> next() {
		List result = null;
		if(updatedResult.getIDs() != null && idIndex < updatedResult.getIDs().length){
			result = new ArrayList(1);
			result.add(updatedResult.getIDs()[idIndex]);
			idIndex++;
		}
		return result;
	}

}
