package org.teiid.translator.salesforce.execution;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.translator.ConnectorException;

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
	DatatypeFactory factory;
	
	public GetDeletedExecutionImpl(
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
