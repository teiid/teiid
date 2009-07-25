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

package org.teiid.connector.jdbc.translator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.connector.api.SourceSystemFunctions;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.visitor.util.SQLReservedWords;


/**
 * Convert the YEAR/MONTH/DAY etc. function into an equivalent Extract function.  
 * Format: EXTRACT(YEAR from Element) or EXTRACT(YEAR from DATE '2004-03-03')
 */
public class ExtractFunctionModifier extends BasicFunctionModifier {
	public static final String YEAR = "YEAR"; //$NON-NLS-1$
	public static final String QUARTER = "QUARTER"; //$NON-NLS-1$
	public static final String MONTH = "MONTH"; //$NON-NLS-1$
	public static final String DAYOFYEAR = "DOY"; //$NON-NLS-1$
	public static final String DAY = "DAY"; //$NON-NLS-1$
	public static final String WEEK = "WEEK"; //$NON-NLS-1$
	public static final String DAYOFWEEK = "DOW"; //$NON-NLS-1$
	public static final String HOUR = "HOUR"; //$NON-NLS-1$
	public static final String MINUTE = "MINUTE"; //$NON-NLS-1$
	public static final String SECOND = "SECOND"; //$NON-NLS-1$
	public static final String MILLISECONDS = "MILLISECONDS"; //$NON-NLS-1$
	
	private static Map<String, String> FUNCTION_PART_MAP = new HashMap<String, String>();
	
	static {
		FUNCTION_PART_MAP.put(SourceSystemFunctions.WEEK, WEEK);
		FUNCTION_PART_MAP.put(SourceSystemFunctions.DAYOFWEEK, DAYOFWEEK);
		FUNCTION_PART_MAP.put(SourceSystemFunctions.DAYOFYEAR, DAYOFYEAR);
		FUNCTION_PART_MAP.put(SourceSystemFunctions.YEAR, YEAR);
		FUNCTION_PART_MAP.put(SourceSystemFunctions.QUARTER, QUARTER);
		FUNCTION_PART_MAP.put(SourceSystemFunctions.MONTH, MONTH);
		FUNCTION_PART_MAP.put(SourceSystemFunctions.DAYOFMONTH, DAY);
		FUNCTION_PART_MAP.put(SourceSystemFunctions.HOUR, HOUR);
		FUNCTION_PART_MAP.put(SourceSystemFunctions.MINUTE, MINUTE);
		FUNCTION_PART_MAP.put(SourceSystemFunctions.SECOND, SECOND);
	}
	
    public ExtractFunctionModifier() {
    }
    
    public List<?> translate(IFunction function) {
        List<IExpression> args = function.getParameters();
        List<Object> objs = new ArrayList<Object>();
        objs.add("EXTRACT("); //$NON-NLS-1$
        objs.add(FUNCTION_PART_MAP.get(function.getName().toLowerCase()));
        objs.add(SQLReservedWords.SPACE);
        objs.add(SQLReservedWords.FROM); 
        objs.add(SQLReservedWords.SPACE);               
        objs.add(args.get(0));
        objs.add(SQLReservedWords.RPAREN);
        if (function.getName().toLowerCase().equals(SourceSystemFunctions.DAYOFWEEK)) {
        	objs.add(0, SQLReservedWords.LPAREN);
        	objs.add(" + 1)"); //$NON-NLS-1$
        }
        return objs;
    }    
}
