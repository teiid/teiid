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

package com.metamatrix.connector.xml;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ResultSetExecution;

/**
 * An XMLExecution is responsible for responding to a Query.  Depending upon
 * the query and the source of the XML, this can sometimes require multiple
 * trips to the source system.  
 * 
 * For example, as SOAP service that converts temperature:
 * 
 * int convertToFahrenheit(int degreesCelsius)
 * 
 * might be modeled as a Table called TempConversion with a column for celsius
 * of type int and a column for fahreneheit also of type int.
 * 
 * when queried like this:
 * 
 * 
 * SELECT fahrenheit FROM TempConversion WHERE celsius IN (40, 20)
 * 
 * The XMLExecution has to make two calls to the service to create the correct
 * result set.  The multiple calls are abstracted within the ResultProducer.
 * 
 */
public interface XMLExecution extends ResultSetExecution {
	/**
	 * Gets all the ResultProducers for a single query.
	 * This could be any number or results and is implementation dependent.
	 * @return
	 * @throws ConnectorException 
	 */
	public ResultProducer  getStreamProducer() throws ConnectorException;
}
