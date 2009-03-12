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

package com.metamatrix.connector.jdbc.oracle.spatial;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.jdbc.oracle.OracleSQLTranslator;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.language.ICriteria;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.ISelect;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.visitor.util.CollectorVisitor;


public class OracleSpatialSQLTranslator extends OracleSQLTranslator {

	@Override
	public void initialize(ConnectorEnvironment env) throws ConnectorException {
        Iterator<String> iter = OracleSpatialFunctions.relateFunctions.iterator();
        while (iter.hasNext()) {
            registerFunctionModifier(iter.next(), new RelateFunctionModifier());
        }
        iter = OracleSpatialFunctions.nearestNeighborFunctions.iterator();
        while (iter.hasNext()) {
            registerFunctionModifier(iter.next(), new NearestNeighborFunctionModifier());
        }
        iter = OracleSpatialFunctions.filterFunctions.iterator();
        while (iter.hasNext()) {
            registerFunctionModifier(iter.next(), new FilterFunctionModifier());
        }
        iter = OracleSpatialFunctions.withinDistanceFunctions.iterator();
        while (iter.hasNext()) {
            registerFunctionModifier(iter.next(), new WithinDistanceFunctionModifier());
        }
        iter = OracleSpatialFunctions.nnDistanceFunctions.iterator();
        while (iter.hasNext()) {
            registerFunctionModifier(iter.next(), new NnDistanceFunctionModifier());
        }
    }

    /**
     * This method is overridden to modify the incoming command to add the hint to the ISelect in the command.
     */
	@Override
	public ICommand modifyCommand(ICommand command, ExecutionContext context) throws ConnectorException {
        if (command instanceof IQuery) {
            IQuery query = (IQuery)command;
            
            ISelect select = ((IQuery)command).getSelect();
            List<ISelectSymbol> symbols = select.getSelectSymbols();
            
            Collection<IFunction> functions = CollectorVisitor.collectObjects(IFunction.class, select);
            for (IFunction function : functions) {
				if (function.getName().equalsIgnoreCase("SDO_NN_DISTANCE")) {//$NON-NLS-1$
                    ICriteria criteria = query.getWhere();
                    if(criteria == null || criteria.toString().indexOf("SDO_NN") == -1){ //$NON-NLS-1$
                	    throw(new ConnectorException(
                	    	Messages.getString("OracleSpatialSQLTranslator.SDO_NN_DEPENDENCY_ERROR"))); //$NON-NLS-1$
                	}
                    break;
				}
			}
            
            for (int i = 0; i < symbols.size(); i++) {
            	ISelectSymbol symbol = symbols.get(i);
            	if (symbol.getExpression().getType().equals(Object.class)) {
                    String outName = symbol.getOutputName();
                    int lIndx = outName.lastIndexOf("."); //$NON-NLS-1$
                    symbol.setOutputName(outName.substring(lIndx + 1));
                    symbol.setExpression(getLanguageFactory().createLiteral(null, TypeFacility.RUNTIME_TYPES.OBJECT));
                    symbol.setAlias(true);
                }
            }
        }

        return command;
    }

    /**
     * This method figures out what the hint is by looking at the query and returns it.
     * @param query
     * 
     * @return The hint or null for no hint
     */
	@Override
	public String getSourceComment(ExecutionContext context, ICommand command) {
		String comment = super.getSourceComment(context, command);
		
		if (command instanceof IQuery) {
	        //
	        // This simple algorithm determines the hint which will be added to the
	        // query.
	        // Right now, we look through all functions passed in the query
	        // (returned as a collection)
	        // Then we check if any of those functions contain the strings 'sdo' and
	        // 'relate'
	        // If so, the ORDERED hint is added, if not, it isn't
	        Collection<IFunction> col = CollectorVisitor.collectObjects(IFunction.class, command);
	        for (IFunction func : col) {
	            String funcName = func.getName().toUpperCase();
	            int indx1 = funcName.indexOf("SDO"); //$NON-NLS-1$
	            int indx2 = funcName.indexOf("RELATE"); //$NON-NLS-1$
	            if (indx1 >= 0 && indx2 > indx1)
	                return comment + " /* + ORDERED */"; //$NON-NLS-1$
	        }
		}
        return comment;
    }
	
	@Override
	public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
		return OracleSpatialCapabilities.class;
	}

}