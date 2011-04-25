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

import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Join;
import org.teiid.language.LanguageFactory;
import org.teiid.language.LanguageObject;
import org.teiid.language.Limit;
import org.teiid.language.Literal;
import org.teiid.language.Comparison.Operator;
import org.teiid.language.Join.JoinType;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.LocateFunctionModifier;
import org.teiid.translator.jdbc.ModFunctionModifier;

@Translator(name="db2", description="A translator for IBM DB2 Database")
public class DB2ExecutionFactory extends JDBCExecutionFactory {

	private final class NullHandlingFormatModifier extends
			ConvertModifier.FormatModifier {
		private NullHandlingFormatModifier(String alias) {
			super(alias);
		}

		@Override
		public List<?> translate(Function function) {
			Expression arg = function.getParameters().get(0);
			if (arg instanceof Literal && ((Literal)arg).getValue() == null) {
				((Literal)function.getParameters().get(1)).setValue(this.alias);
				return null;
			}
			return super.translate(function);
		}
	}

	@Override
	public void start() throws TranslatorException {
		super.start();
        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("chr")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new AliasModifier("day")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("coalesce")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new LocateFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new SubstringFunctionModifier());  

        registerFunctionModifier(SourceSystemFunctions.MOD, new ModFunctionModifier("MOD", getLanguageFactory()));  //$NON-NLS-1$
        
        //add in type conversion
        ConvertModifier convertModifier = new ConvertModifier();
    	convertModifier.addTypeMapping("real", FunctionModifier.FLOAT); //$NON-NLS-1$
    	convertModifier.addTypeMapping("numeric(31,0)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$
    	convertModifier.addTypeMapping("numeric(31,12)", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
    	convertModifier.addTypeMapping("char(1)", FunctionModifier.CHAR); //$NON-NLS-1$
    	convertModifier.addTypeMapping("blob", FunctionModifier.BLOB, FunctionModifier.OBJECT); //$NON-NLS-1$
    	convertModifier.addTypeMapping("clob", FunctionModifier.CLOB, FunctionModifier.XML); //$NON-NLS-1$
    	convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.TIMESTAMP, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList("timestamp('1970-01-01', ", function.getParameters().get(0), ")"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		});
    	convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.TIMESTAMP, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList("timestamp(",function.getParameters().get(0), ", '00:00:00')"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		});
    	//the next convert is not strictly necessary for db2, but it also works for derby
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.FLOAT, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList("cast(double(", function.getParameters().get(0), ") as real)"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		});
    	convertModifier.addTypeConversion(new NullHandlingFormatModifier("char"), FunctionModifier.STRING); //$NON-NLS-1$
    	convertModifier.addTypeConversion(new NullHandlingFormatModifier("smallint"), FunctionModifier.BYTE, FunctionModifier.SHORT); //$NON-NLS-1$
    	convertModifier.addTypeConversion(new NullHandlingFormatModifier("integer"), FunctionModifier.INTEGER); //$NON-NLS-1$
    	convertModifier.addTypeConversion(new NullHandlingFormatModifier("bigint"), FunctionModifier.LONG); //$NON-NLS-1$
    	convertModifier.addTypeConversion(new NullHandlingFormatModifier("double"), FunctionModifier.DOUBLE); //$NON-NLS-1$
    	convertModifier.addTypeConversion(new NullHandlingFormatModifier("date"), FunctionModifier.DATE); //$NON-NLS-1$
    	convertModifier.addTypeConversion(new NullHandlingFormatModifier("time"), FunctionModifier.TIME); //$NON-NLS-1$
    	convertModifier.addTypeConversion(new NullHandlingFormatModifier("timestamp"), FunctionModifier.TIMESTAMP); //$NON-NLS-1$
    	convertModifier.addNumericBooleanConversions();
    	registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);
    }
		
	@SuppressWarnings("unchecked")
	@Override
	public List<?> translateLimit(Limit limit, ExecutionContext context) {
		return Arrays.asList("FETCH FIRST ", limit.getRowLimit(), " ROWS ONLY"); //$NON-NLS-1$ //$NON-NLS-2$ 
	}
	
	@Override
	public List<?> translate(LanguageObject obj, ExecutionContext context) {
		//DB2 doesn't support cross join
		convertCrossJoinToInner(obj, getLanguageFactory());
		//DB2 needs projected nulls wrapped in casts
		if (obj instanceof DerivedColumn) {
			DerivedColumn selectSymbol = (DerivedColumn)obj;
			if (selectSymbol.getExpression() instanceof Literal) {
				Literal literal = (Literal)selectSymbol.getExpression();
				if (literal.getValue() == null) {
					String type = TypeFacility.RUNTIME_NAMES.INTEGER;
					if (literal.getType() != TypeFacility.RUNTIME_TYPES.NULL) {
						type = TypeFacility.getDataTypeName(literal.getType());
					}
					selectSymbol.setExpression(ConvertModifier.createConvertFunction(getLanguageFactory(), literal, type));
				}
			}
		}
		return super.translate(obj, context);
	}

	public static void convertCrossJoinToInner(LanguageObject obj, LanguageFactory lf) {
		if (obj instanceof Join) {
			Join join = (Join)obj;
			if (join.getJoinType() == JoinType.CROSS_JOIN) {
				Literal one = lf.createLiteral(1, TypeFacility.RUNTIME_TYPES.INTEGER);
				join.setCondition(lf.createCompareCriteria(Operator.EQ, one, one));
				join.setJoinType(JoinType.INNER_JOIN);
			}
		}
	}
	
	@Override
	public NullOrder getDefaultNullOrder() {
		return NullOrder.HIGH;
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
	public boolean supportsInlineViews() {
		return true;
	}

	@Override
	public boolean supportsFunctionsInGroupBy() {
		return true;
	}

	@Override
	public boolean supportsRowLimit() {
		return true;
	}

	@Override
	public boolean supportsExcept() {
		return true;
	}

	@Override
	public boolean supportsIntersect() {
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
}
