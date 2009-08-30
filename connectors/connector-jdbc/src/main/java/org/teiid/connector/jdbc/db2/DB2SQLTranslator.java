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

package org.teiid.connector.jdbc.db2;

import java.util.Arrays;
import java.util.List;

import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.SourceSystemFunctions;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.jdbc.translator.AliasModifier;
import org.teiid.connector.jdbc.translator.ConvertModifier;
import org.teiid.connector.jdbc.translator.FunctionModifier;
import org.teiid.connector.jdbc.translator.LocateFunctionModifier;
import org.teiid.connector.jdbc.translator.ModFunctionModifier;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.IJoin;
import org.teiid.connector.language.ILanguageObject;
import org.teiid.connector.language.ILimit;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.language.ICompareCriteria.Operator;
import org.teiid.connector.language.IJoin.JoinType;

public class DB2SQLTranslator extends Translator {

	private final class NullHandlingFormatModifier extends
			ConvertModifier.FormatModifier {
		private NullHandlingFormatModifier(String alias) {
			super(alias);
		}

		@Override
		public List<?> translate(IFunction function) {
			IExpression arg = function.getParameters().get(0);
			if (arg instanceof ILiteral && ((ILiteral)arg).getValue() == null) {
				((ILiteral)function.getParameters().get(1)).setValue(this.alias);
				return null;
			}
			return super.translate(function);
		}
	}

	@Override
	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		super.initialize(env);
        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("chr")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new AliasModifier("day")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("coalesce")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new LocateFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("substr")); //$NON-NLS-1$ 

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
			public List<?> translate(IFunction function) {
				return Arrays.asList("timestamp('1970-01-01', ", function.getParameters().get(0), ")"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		});
    	convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.TIMESTAMP, new FunctionModifier() {
			@Override
			public List<?> translate(IFunction function) {
				return Arrays.asList("timestamp(",function.getParameters().get(0), ", '00:00:00')"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		});
    	//the next convert is not strictly necessary for db2, but it also works for derby
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.FLOAT, new FunctionModifier() {
			@Override
			public List<?> translate(IFunction function) {
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
	public List<?> translateLimit(ILimit limit, ExecutionContext context) {
		return Arrays.asList("FETCH FIRST ", limit.getRowLimit(), " ROWS ONLY"); //$NON-NLS-1$ //$NON-NLS-2$ 
	}
	
	@Override
	public List<?> translate(ILanguageObject obj, ExecutionContext context) {
		//DB2 doesn't support cross join
		if (obj instanceof IJoin) {
			IJoin join = (IJoin)obj;
			if (join.getJoinType() == JoinType.CROSS_JOIN) {
				ILiteral one = getLanguageFactory().createLiteral(1, TypeFacility.RUNTIME_TYPES.INTEGER);
				join.getCriteria().add(getLanguageFactory().createCompareCriteria(Operator.EQ, one, one));
				join.setJoinType(JoinType.INNER_JOIN);
			}
		}
		//DB2 needs projected nulls wrapped in casts
		if (obj instanceof ISelectSymbol) {
			ISelectSymbol selectSymbol = (ISelectSymbol)obj;
			if (selectSymbol.getExpression() instanceof ILiteral) {
				ILiteral literal = (ILiteral)selectSymbol.getExpression();
				if (literal.getValue() == null) {
					selectSymbol.setExpression(ConvertModifier.createConvertFunction(getLanguageFactory(), literal, TypeFacility.getDataTypeName(literal.getType())));
				}
			}
		}
		return super.translate(obj, context);
	}
	
	@Override
	public String getDefaultConnectionTestQuery() {
		return "Select 'x' from sysibm.systables where 1 = 2"; //$NON-NLS-1$
	}
	
	@Override
	public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
		return DB2Capabilities.class;
	}
    
	@Override
	public NullOrder getDefaultNullOrder() {
		return NullOrder.HIGH;
	}
}
