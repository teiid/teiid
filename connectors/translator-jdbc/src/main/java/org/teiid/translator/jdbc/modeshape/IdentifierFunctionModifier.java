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

package org.teiid.translator.jdbc.modeshape;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.NamedTable;
import org.teiid.language.SQLConstants;
import org.teiid.metadata.Column;
import org.teiid.translator.jdbc.FunctionModifier;


/**
 * Function to translate ColumnReferences to selector names
 * @since 7.1
 */
public class IdentifierFunctionModifier extends FunctionModifier {

    public List<?> translate(Function function) {
    	
    	List<Object> objs = new ArrayList<Object>();
    	
    	List<Expression> parms = function.getParameters();
    	
    	objs.add(function.getName().substring(function.getName().indexOf('_') + 1)); 
    	objs.add(SQLConstants.Tokens.LPAREN);
    	
    	for (Iterator<Expression> iter = parms.iterator(); iter.hasNext();) 
    	{
    		Expression expr = iter.next();
    		if (expr instanceof ColumnReference) {
    			boolean dotAll = false;
    			boolean useSelector = false;
    			ColumnReference cr = (ColumnReference)expr;
    			Column c = cr.getMetadataObject();
    			if (c != null) {
    				if ("\"mode:properties\"".equalsIgnoreCase(c.getNameInSource())) { //$NON-NLS-1$
    					dotAll = true;
    					useSelector = true;
    				} else if ("\"jcr:path\"".equalsIgnoreCase(c.getNameInSource())) { //$NON-NLS-1$
    					useSelector = true;
    				}
    			}
    			if (useSelector) {
		    		NamedTable nt = ((ColumnReference)expr).getTable();
		    		if (nt.getCorrelationName() != null) {
		    			objs.add(nt.getCorrelationName());
		    		} else {
		    			objs.add(nt);
		    		}
    			} else {
    				objs.add(expr);
    			}
	    		if (dotAll) {
	    			objs.add(".*"); //$NON-NLS-1$
	    		}
    		} else {
    			objs.add(expr);
    		}
    		if (iter.hasNext()) {
    			objs.add(", "); //$NON-NLS-1$
    		}
     	}

    	objs.add(SQLConstants.Tokens.RPAREN);
        return objs; 
    }

}
