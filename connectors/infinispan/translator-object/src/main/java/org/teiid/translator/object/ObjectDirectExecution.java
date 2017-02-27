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

/*
 */

package org.teiid.translator.object;

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;


public class ObjectDirectExecution extends ObjectBaseExecution implements ProcedureExecution {

	private List<Argument> arguments;
    protected int updateCount = 0;
 
    public ObjectDirectExecution(List<Argument> arguments, Command command, ObjectConnection connection, ExecutionContext context, ObjectExecutionFactory env)  {
        super(connection, context, env);
        this.arguments = arguments;
              
    }
    
    @Override
    public void execute() throws TranslatorException {
       	DDLHandler omlc = connection.getDDLHandler();
    	
		String sourceSQL = (String) this.arguments.get(0).getArgumentValue().getValue();
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Source-specific command: ", sourceSQL); //$NON-NLS-1$

        omlc.handleDDL(sourceSQL, this.connection);
    	updateCount = 1;
    }

	@Override
	public List<?> next() throws TranslatorException,
			DataNotAvailableException {
		if (updateCount == -1) {
			return null;
		}
    	ArrayList<Object[]> row = new ArrayList<Object[]>(1);
        try {   	
        		List<Object> vals = new ArrayList<Object>(1);
        		vals.add(new Integer(updateCount));
                row.add(vals.toArray(new Object[vals.size()]));
                
                updateCount = -1;
        } catch (Exception e) {
            throw new TranslatorException(e, ObjectPlugin.Util.getString("ObjectTranslator.Unexpected_exception_translating_results___8", e.getMessage())); //$NON-NLS-1$
        }
        return row;
    }
    
	@Override
	public List<?> getOutputParameterValues()  {
		return null;  //could support as an array of output values via given that the native procedure returns an array value
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.Execution#close()
	 */
	@Override
	public void close() {
		super.close();
		arguments = null;
	}

	
 }
