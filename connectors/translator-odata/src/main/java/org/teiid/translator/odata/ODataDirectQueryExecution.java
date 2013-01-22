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
package org.teiid.translator.odata;

import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.*;

public class ODataDirectQueryExecution implements ProcedureExecution {

	public ODataDirectQueryExecution(List<Argument> arguments, Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			WSConnection connection, String nativeQuery) {
		
		// rameshTODO Auto-generated constructor stub
	}

	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		// rameshTODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// rameshTODO Auto-generated method stub

	}

	@Override
	public void cancel() throws TranslatorException {
		// rameshTODO Auto-generated method stub

	}

	@Override
	public void execute() throws TranslatorException {
		// rameshTODO Auto-generated method stub

	}

	@Override
	public List<?> getOutputParameterValues() throws TranslatorException {
		// rameshTODO Auto-generated method stub
		return null;
	}

}
