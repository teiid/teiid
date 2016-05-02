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

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Comparison.Operator;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Join;
import org.teiid.language.Join.JoinType;
import org.teiid.language.LanguageFactory;
import org.teiid.language.LanguageObject;
import org.teiid.language.Limit;
import org.teiid.language.Literal;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.LocateFunctionModifier;
import org.teiid.translator.jdbc.ModFunctionModifier;

public class BaseDB2ExecutionFactory extends JDBCExecutionFactory {

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
	public boolean supportsInlineViews() {
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
	public boolean supportsSubqueryInOn() {
		return false;
	}
	
	@Override
	public Object retrieveValue(ResultSet results, int columnIndex,
			Class<?> expectedType) throws SQLException {
		if (expectedType == TypeFacility.RUNTIME_TYPES.XML || expectedType == TypeFacility.RUNTIME_TYPES.CLOB) {
			return results.getString(columnIndex);
		} else if (expectedType == TypeFacility.RUNTIME_TYPES.BLOB ){
			return results.getBytes(columnIndex);
		}
		return super.retrieveValue(results, columnIndex, expectedType);
	}
	
	@Override
	public Object retrieveValue(CallableStatement results, int parameterIndex,
			Class<?> expectedType) throws SQLException {
		if (expectedType == TypeFacility.RUNTIME_TYPES.XML || expectedType == TypeFacility.RUNTIME_TYPES.CLOB) {
			return results.getString(parameterIndex);
		} else if (expectedType == TypeFacility.RUNTIME_TYPES.BLOB){
			return results.getBytes(parameterIndex);
		}
		return super.retrieveValue(results, parameterIndex, expectedType);
	}
}
