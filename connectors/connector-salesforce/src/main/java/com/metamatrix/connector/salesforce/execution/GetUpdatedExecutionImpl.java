package com.metamatrix.connector.salesforce.execution;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.Argument;
import org.teiid.connector.language.Call;

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
	DatatypeFactory factory;

	public GetUpdatedExecutionImpl(
			ProcedureExecutionParent procedureExecutionParent) throws ConnectorException {
		this.parent = procedureExecutionParent;
		try {
			factory = DatatypeFactory.newInstance();
		} catch (DatatypeConfigurationException e) {
			throw new ConnectorException(e.getMessage());
		}
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
		Call command = parent.getCommand();
		List<Argument> params = command.getArguments();
		
		Argument object = params.get(OBJECT);
		String objectName = (String) object.getArgumentValue().getValue();
		
		Argument start = params.get(STARTDATE);
		Timestamp startTime = (Timestamp) start.getArgumentValue().getValue();
		GregorianCalendar tempCalendar = (GregorianCalendar) GregorianCalendar.getInstance();
		tempCalendar.setTime(startTime);
		XMLGregorianCalendar startCalendar = factory.newXMLGregorianCalendar(tempCalendar);
		
		Argument end = params.get(ENDDATE);
		Timestamp endTime = (Timestamp) end.getArgumentValue().getValue();
		tempCalendar = (GregorianCalendar) GregorianCalendar.getInstance();
		tempCalendar.setTime(endTime);
		XMLGregorianCalendar endCalendar = factory.newXMLGregorianCalendar(tempCalendar);
		
		updatedResult = parent.getConnection().getUpdated(objectName, startCalendar, endCalendar);
	}

	@Override
	public List<Calendar> getOutputParameterValues() {
		List<Calendar> result = new ArrayList<Calendar>(1);
		result.add(updatedResult.getLatestDateCovered());
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List next() {
		List result = null;
		if(updatedResult.getIDs() != null && idIndex < updatedResult.getIDs().size()){
			result = new ArrayList(1);
			result.add(updatedResult.getIDs().get(idIndex));
			idIndex++;
		}
		return result;
	}

}
