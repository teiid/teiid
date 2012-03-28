/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.translator.salesforce.execution;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

import javax.resource.ResourceException;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.translator.TranslatorException;

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

	private ProcedureExecutionParent parent;

	private DeletedResult deletedResult;
	private int resultIndex = 0;
	DatatypeFactory factory;
	
	public GetDeletedExecutionImpl(
			ProcedureExecutionParent procedureExecutionParent) throws TranslatorException {
		this.parent = procedureExecutionParent;
		try {
			factory = DatatypeFactory.newInstance();
		} catch (DatatypeConfigurationException e) {
			throw new TranslatorException(e.getMessage());
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
	public void execute(ProcedureExecutionParent procedureExecutionParent) throws TranslatorException {
		try {
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
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}	
	}

	@Override
	public List<?> getOutputParameterValues() {
		List<Timestamp> result = new ArrayList<Timestamp>();
		result.add(new Timestamp(deletedResult.getLatestDateCovered().getTimeInMillis()));
		result.add(new Timestamp(deletedResult.getEarliestDateAvailable().getTimeInMillis()));
		return result;
	}

	@Override
	public List<?> next() {
		List<Object> result = null;
		if(deletedResult.getResultRecords() != null && resultIndex < deletedResult.getResultRecords().size()){
			result = new ArrayList<Object>(2);
			result.add(deletedResult.getResultRecords().get(resultIndex).getID());
			result.add(new Timestamp(deletedResult.getResultRecords().get(resultIndex).getDeletedDate().getTimeInMillis()));
			resultIndex++;
		}
		return result;
	}

}
