/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
	private boolean supportsQuarter;

	public AddDiffModifier(boolean add, LanguageFactory factory) {
		this.add = add;
		this.factory = factory;
	}
	
	public AddDiffModifier supportsQuarter(boolean b) {
		this.supportsQuarter = b;
		return this;
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
		if (supportsQuarter && interval.equals(NonReserved.SQL_TSI_QUARTER)) {
			intervalType.setValue("QUARTER"); //$NON-NLS-1$
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