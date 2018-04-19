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

package org.teiid.translator.jdbc.hsql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.translator.jdbc.ExtractFunctionModifier;
import org.teiid.translator.jdbc.FunctionModifier;

public class AddDiffModifier extends FunctionModifier {

	private static Map<String, String> INTERVAL_MAP = new HashMap<String, String>();
	
	static {
		INTERVAL_MAP.put(NonReserved.SQL_TSI_DAY, ExtractFunctionModifier.DAY);
		INTERVAL_MAP.put(NonReserved.SQL_TSI_HOUR, ExtractFunctionModifier.HOUR);
		INTERVAL_MAP.put(NonReserved.SQL_TSI_MINUTE, ExtractFunctionModifier.MINUTE);
		INTERVAL_MAP.put(NonReserved.SQL_TSI_MONTH, ExtractFunctionModifier.MONTH);
		INTERVAL_MAP.put(NonReserved.SQL_TSI_SECOND, ExtractFunctionModifier.SECOND);
		INTERVAL_MAP.put(NonReserved.SQL_TSI_YEAR, ExtractFunctionModifier.YEAR);
	}
	
	private boolean add;
	private boolean supportsQuarter;
	private boolean literalPart = true;

	public AddDiffModifier(boolean add, LanguageFactory factory) {
		this.add = add;
	}
	
	public AddDiffModifier supportsQuarter(boolean b) {
		this.supportsQuarter = b;
		return this;
	}
	
	public AddDiffModifier literalPart(boolean b) {
        this.literalPart = b;
        return this;
    }
	
	@Override
	public List<?> translate(Function function) {
	    ArrayList<Object> result = new ArrayList<Object>();
		if (add) {
			result.add("dateadd("); //$NON-NLS-1$
		} else {
		    result.add("datediff("); //$NON-NLS-1$
		}
		for (int i = 0; i < function.getParameters().size(); i++) {
		    if (i > 0) {
		        result.add(", "); //$NON-NLS-1$
		    }
		    result.add(function.getParameters().get(i));
		}
		result.add(")"); //$NON-NLS-1$
		Literal intervalType = (Literal)function.getParameters().get(0);
		String interval = ((String)intervalType.getValue()).toUpperCase();
		String newInterval = INTERVAL_MAP.get(interval);
		if (newInterval != null) {
			intervalType.setValue(newInterval);
		} else if (supportsQuarter && interval.equals(NonReserved.SQL_TSI_QUARTER)) {
			intervalType.setValue("QUARTER"); //$NON-NLS-1$
		} else if (add) {
			if (interval.equals(NonReserved.SQL_TSI_FRAC_SECOND)) {
				intervalType.setValue("MILLISECOND"); //$NON-NLS-1$
				result.add(4, " / 1000000"); //$NON-NLS-1$
			} else if (interval.equals(NonReserved.SQL_TSI_QUARTER)) {
				intervalType.setValue(ExtractFunctionModifier.DAY);
				result.add(4, " * 91"); //$NON-NLS-1$
			} else {
				intervalType.setValue(ExtractFunctionModifier.DAY);
				result.add(4, " * 7"); //$NON-NLS-1$
			}
		} else if (interval.equals(NonReserved.SQL_TSI_FRAC_SECOND)) {
			intervalType.setValue("MILLISECOND"); //$NON-NLS-1$
			result.add(" * 1000000"); //$NON-NLS-1$
		} else if (interval.equals(NonReserved.SQL_TSI_QUARTER)) {
			intervalType.setValue(ExtractFunctionModifier.DAY);
			result.add(" / 91"); //$NON-NLS-1$  
		} else {
    		intervalType.setValue(ExtractFunctionModifier.DAY);
    		result.add(" / 7"); //$NON-NLS-1$
		}
		if (!literalPart) {
		    result.set(1, intervalType.getValue());
		}
		return result;
	}
	
}