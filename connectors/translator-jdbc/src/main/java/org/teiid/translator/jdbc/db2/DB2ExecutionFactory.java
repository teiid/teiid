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

package org.teiid.translator.jdbc.db2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Literal;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.FunctionModifier;

@Translator(name="db2", description="A translator for IBM DB2 Database")
public class DB2ExecutionFactory extends BaseDB2ExecutionFactory {
	
	public static final String EIGHT_0 = "8.0"; //$NON-NLS-1$
	public static final String NINE_1 = "9.1"; //$NON-NLS-1$
	
	public DB2ExecutionFactory() {
		setDatabaseVersion(EIGHT_0);
	}
	
	@Override
	public List<String> getSupportedFunctions() {
		List<String> supportedFunctions = new ArrayList<String>();
		supportedFunctions.addAll(super.getSupportedFunctions());
		supportedFunctions.add("ABS"); //$NON-NLS-1$
		supportedFunctions.add("ACOS"); //$NON-NLS-1$
		supportedFunctions.add("ASIN"); //$NON-NLS-1$
		supportedFunctions.add("ATAN"); //$NON-NLS-1$
		supportedFunctions.add("ATAN2"); //$NON-NLS-1$
		supportedFunctions.add("CEILING"); //$NON-NLS-1$
		supportedFunctions.add("COS"); //$NON-NLS-1$
		supportedFunctions.add("COT"); //$NON-NLS-1$
		supportedFunctions.add("DEGREES"); //$NON-NLS-1$
		supportedFunctions.add("EXP"); //$NON-NLS-1$
		supportedFunctions.add("FLOOR"); //$NON-NLS-1$
		supportedFunctions.add("LOG"); //$NON-NLS-1$
		supportedFunctions.add("LOG10"); //$NON-NLS-1$
		supportedFunctions.add("MOD"); //$NON-NLS-1$
		supportedFunctions.add("POWER"); //$NON-NLS-1$
		supportedFunctions.add("RADIANS"); //$NON-NLS-1$
		supportedFunctions.add("SIGN"); //$NON-NLS-1$
		supportedFunctions.add("SIN"); //$NON-NLS-1$
		supportedFunctions.add("SQRT"); //$NON-NLS-1$
		supportedFunctions.add("TAN"); //$NON-NLS-1$
		//supportedFunctions.add("ASCII"); //$NON-NLS-1$
		supportedFunctions.add("CHAR"); //$NON-NLS-1$
		supportedFunctions.add("CHR"); //$NON-NLS-1$
		supportedFunctions.add("CONCAT"); //$NON-NLS-1$
		supportedFunctions.add("||"); //$NON-NLS-1$
		//supportedFunctions.add("INITCAP"); //$NON-NLS-1$
		supportedFunctions.add("LCASE"); //$NON-NLS-1$
		supportedFunctions.add("LENGTH"); //$NON-NLS-1$
		supportedFunctions.add("LEFT"); //$NON-NLS-1$
		supportedFunctions.add("LOCATE"); //$NON-NLS-1$
		supportedFunctions.add("LOWER"); //$NON-NLS-1$
		//supportedFunctions.add("LPAD"); //$NON-NLS-1$
		supportedFunctions.add("LTRIM"); //$NON-NLS-1$
		supportedFunctions.add("RAND"); //$NON-NLS-1$
		supportedFunctions.add("REPLACE"); //$NON-NLS-1$
		//supportedFunctions.add("RPAD"); //$NON-NLS-1$
		supportedFunctions.add("RIGHT"); //$NON-NLS-1$
		supportedFunctions.add("RTRIM"); //$NON-NLS-1$
		supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
		supportedFunctions.add(SourceSystemFunctions.TRIM);
		//supportedFunctions.add("TRANSLATE"); //$NON-NLS-1$
		supportedFunctions.add("UCASE"); //$NON-NLS-1$
		supportedFunctions.add("UPPER"); //$NON-NLS-1$
		supportedFunctions.add("HOUR"); //$NON-NLS-1$
		supportedFunctions.add("MONTH"); //$NON-NLS-1$
		supportedFunctions.add("MONTHNAME"); //$NON-NLS-1$
		supportedFunctions.add("YEAR"); //$NON-NLS-1$
		supportedFunctions.add("DAY"); //$NON-NLS-1$
		supportedFunctions.add("DAYNAME"); //$NON-NLS-1$
		supportedFunctions.add("DAYOFMONTH"); //$NON-NLS-1$
		supportedFunctions.add("DAYOFWEEK"); //$NON-NLS-1$
		supportedFunctions.add("DAYOFYEAR"); //$NON-NLS-1$
		supportedFunctions.add("QUARTER"); //$NON-NLS-1$
		supportedFunctions.add("MINUTE"); //$NON-NLS-1$
		supportedFunctions.add("SECOND"); //$NON-NLS-1$
		supportedFunctions.add("QUARTER"); //$NON-NLS-1$
		supportedFunctions.add("WEEK"); //$NON-NLS-1$
		supportedFunctions.add("CAST"); //$NON-NLS-1$
		supportedFunctions.add("CONVERT"); //$NON-NLS-1$
		supportedFunctions.add("IFNULL"); //$NON-NLS-1$
		supportedFunctions.add("NVL"); //$NON-NLS-1$ 
		supportedFunctions.add("COALESCE"); //$NON-NLS-1$
		return supportedFunctions;
	}

	@Override
	public boolean supportsFunctionsInGroupBy() {
		return true;
	}

	@Override
	public boolean supportsAggregatesEnhancedNumeric() {
		return true;
	}
	
	@Override
	public boolean supportsCommonTableExpressions() {
		return true;
	}
	
	@Override
	public boolean supportsRowLimit() {
		return true;
	}
	
	@Override
	public boolean supportsElementaryOlapOperations() {
		return getDatabaseVersion().compareTo(NINE_1) >= 0;
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();
		registerFunctionModifier(SourceSystemFunctions.TRIM, new FunctionModifier() {
			
			@Override
			public List<?> translate(Function function) {
				List<Expression> p = function.getParameters();
				return Arrays.asList("STRIP(", p.get(2), ", ", ((Literal)p.get(0)).getValue(), ", ", p.get(1), ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			}
		});
	}
	
}
