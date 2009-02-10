/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import java.util.Map;

import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.connector.jdbc.oracle.OracleSQLTranslator;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.ICriteria;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.IQuery;
import com.metamatrix.connector.language.ISelect;
import com.metamatrix.connector.visitor.util.CollectorVisitor;

public class OracleSpatialSQLTranslator extends OracleSQLTranslator {

    /**
     * This method is overridden to find function modifiers that modify functions in the incoming command.
     */
    public Map getFunctionModifiers() {
        Map modifiers = super.getFunctionModifiers();
        Iterator iter = OracleSpatialFunctions.relateFunctions.iterator();
        while (iter.hasNext()) {
            modifiers.put(iter.next(), new RelateFunctionModifier());
        }
        iter = OracleSpatialFunctions.nearestNeighborFunctions.iterator();
        while (iter.hasNext()) {
            modifiers.put(iter.next(), new NearestNeighborFunctionModifier());
        }
        iter = OracleSpatialFunctions.filterFunctions.iterator();
        while (iter.hasNext()) {
            modifiers.put(iter.next(), new FilterFunctionModifier());
        }
        iter = OracleSpatialFunctions.withinDistanceFunctions.iterator();
        while (iter.hasNext()) {
            modifiers.put(iter.next(), new WithinDistanceFunctionModifier());
        }
        iter = OracleSpatialFunctions.nnDistanceFunctions.iterator();
        while (iter.hasNext()) {
            modifiers.put(iter.next(), new NnDistanceFunctionModifier());
        }
        return modifiers;
    }

    /**
     * This method is overridden to modify the incoming command to add the hint to the ISelect in the command.
     */
public ICommand modifyCommand(ICommand command, ExecutionContext context) throws ConnectorException {
        if (command instanceof IQuery) {
            IQuery query = (IQuery)command;
            String hint = getHint(query);
            
            ISelect select = ((IQuery)command).getSelect();
            List symbols = select.getSelectSymbols();
            if(symbols.toString().indexOf("SDO_NN_DISTANCE") != -1){ //$NON-NLS-1$
                ICriteria criteria = query.getWhere();
                if(criteria != null && criteria.toString().indexOf("SDO_NN") == -1){ //$NON-NLS-1$
            	  	    throw(new ConnectorException(
            	    	Messages.getString("OracleSpatialSQLTranslator.SDO_NN_DEPENDENCY_ERROR"))); //$NON-NLS-1$
            	}    
            }
            /*Iterator iter = symbols.iterator();
            while(iter.hasNext()){
                if(((ISelectSymbol)iter.next()).getExpression().compareToIgnoreCase("SDO_NN_DISTANCE") != 0){ //$NON-NLS-1$
                    ICriteria criteria = query.getWhere();
                    if(criteria != null && criteria.toString().compareToIgnoreCase("SDO_NN") != 0){ //$NON-NLS-1$
                	        break;
                	} else{
                	    throw(new ConnectorException(
                	    	Messages.getString("OracleSpatialSQLTranslator.SDO_NN_DEPENDENCY_ERROR"))); //$NON-NLS-1$
                	}
                }   	
            }
            */
            if (hint != null) {
                SpatialSelectProxy proxy = new SpatialSelectProxy(select, hint);
                query.setSelect(proxy);
            }
        }

        return command;
    }
    /**
     * This method is overridden to use a special variant of the SQL conversion visitor so that the hint can be inserted into the
     * SQL.
     */
    public SQLConversionVisitor getTranslationVisitor() {
        OracleSpatialConversionVisitor visitor = new OracleSpatialConversionVisitor();
        visitor.setRuntimeMetadata(getRuntimeMetadata());
        visitor.setFunctionModifiers(getFunctionModifiers());
        visitor.setProperties(getConnectorEnvironment().getProperties());
        visitor.setLanguageFactory(getConnectorEnvironment().getLanguageFactory());
        return visitor; 
    }

    /**
     * This method figures out what the hint is by looking at the query and returns it.
     * 
     * @param query
     * @return The hint or null for no hint
     */
    private String getHint(IQuery query) {
        //
        // This simple algorithm determines the hint which will be added to the
        // query.
        // Right now, we look through all functions passed in the query
        // (returned as a collection)
        // Then we check if any of those functions contain the strings 'sdo' and
        // 'relate'
        // If so, the ORDERED hint is added, if not, it isn't
        Class iFunction = null;
        try {
            iFunction = Class.forName("com.metamatrix.data.language.IFunction"); //$NON-NLS-1$
        } catch (ClassNotFoundException cex) {
            System.err.println("BML: IFunction Class Missing"); //$NON-NLS-1$
        }
        Collection col = CollectorVisitor.collectObjects(iFunction, query);
        Iterator it = col.iterator();
        while (it.hasNext()) {
            IFunction func = (IFunction)it.next();
            String funcName = func.getName().toUpperCase();
            int indx1 = funcName.indexOf("SDO"); //$NON-NLS-1$
            int indx2 = funcName.indexOf("RELATE"); //$NON-NLS-1$
            if (indx1 >= 0 && indx2 > indx1)
                return "/* + ORDERED */"; //$NON-NLS-1$
        }
        return " "; //$NON-NLS-1$
    }

}