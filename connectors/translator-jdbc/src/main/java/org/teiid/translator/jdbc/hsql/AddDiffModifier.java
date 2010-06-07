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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.translator.TypeFacility;
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
	private LanguageFactory factory;

	public AddDiffModifier(boolean add, LanguageFactory factory) {
		this.add = add;
		this.factory = factory;
	}
	
	@Override
	public List<?> translate(Function function) {
		if (add) {
			function.setName("dateadd"); //$NON-NLS-1$
		} else {
			function.setName("datediff"); //$NON-NLS-1$
		}
		Literal intervalType = (Literal)function.getParameters().get(0);
		String interval = ((String)intervalType.getValue()).toUpperCase();
		String newInterval = INTERVAL_MAP.get(interval);
		if (newInterval != null) {
			intervalType.setValue(newInterval);
			return null;
		}
		if (add) {
			if (interval.equals(NonReserved.SQL_TSI_FRAC_SECOND)) {
				intervalType.setValue("MILLISECOND"); //$NON-NLS-1$
				Expression[] args = new Expression[] {function.getParameters().get(1), factory.createLiteral(1000000, TypeFacility.RUNTIME_TYPES.INTEGER)};
				function.getParameters().set(1, factory.createFunction("/", args, TypeFacility.RUNTIME_TYPES.INTEGER)); //$NON-NLS-1$
			} else if (interval.equals(NonReserved.SQL_TSI_QUARTER)) {
				intervalType.setValue(ExtractFunctionModifier.DAY);
				Expression[] args = new Expression[] {function.getParameters().get(1), factory.createLiteral(91, TypeFacility.RUNTIME_TYPES.INTEGER)};
				function.getParameters().set(1, factory.createFunction("*", args, TypeFacility.RUNTIME_TYPES.INTEGER)); //$NON-NLS-1$
			} else {
				intervalType.setValue(ExtractFunctionModifier.DAY);
				Expression[] args = new Expression[] {function.getParameters().get(1), factory.createLiteral(7, TypeFacility.RUNTIME_TYPES.INTEGER)};
				function.getParameters().set(1, factory.createFunction("*", args, TypeFacility.RUNTIME_TYPES.INTEGER)); //$NON-NLS-1$
			}
			return null;
		} 
		if (interval.equals(NonReserved.SQL_TSI_FRAC_SECOND)) {
			intervalType.setValue("MILLISECOND"); //$NON-NLS-1$
			return Arrays.asList(function, " * 1000000"); //$NON-NLS-1$
		} else if (interval.equals(NonReserved.SQL_TSI_QUARTER)) {
			intervalType.setValue(ExtractFunctionModifier.DAY);
			return Arrays.asList(function, " / 91"); //$NON-NLS-1$  
		} 
		intervalType.setValue(ExtractFunctionModifier.DAY);
		return Arrays.asList(function, " / 7"); //$NON-NLS-1$  
	}
	
}