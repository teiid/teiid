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

package org.teiid.translator.jdbc.teradata;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.*;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.language.Literal;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.SQLConversionVisitor;



/** 
 * Teradata database Release 12
 */
@Translator(name="teradata", description="A translator for Teradata Database")
public class TeradataExecutionFactory extends JDBCExecutionFactory {

	public static String TERADATA = "teradata"; //$NON-NLS-1$
	protected ConvertModifier convert = new ConvertModifier();
	
    public TeradataExecutionFactory() {
    	setSupportsOuterJoins(false);
    }
    
	@Override
	public void start() throws TranslatorException {
		super.start();
		convert.addTypeMapping("byteint", FunctionModifier.BYTE, FunctionModifier.SHORT, FunctionModifier.BOOLEAN); //$NON-NLS-1$
		convert.addTypeMapping("double precision", FunctionModifier.DOUBLE); //$NON-NLS-1$
		convert.addTypeMapping("numeric(18,0)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$
		convert.addTypeMapping("char(1)", FunctionModifier.CHAR); //$NON-NLS-1$

    	convert.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.TIME, new CastModifier("TIME")); //$NON-NLS-1$
    	convert.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.DATE,  new CastModifier("DATE")); //$NON-NLS-1$ 
    	convert.addConvert(FunctionModifier.TIME, FunctionModifier.TIMESTAMP, new CastModifier("TIMESTAMP")); //$NON-NLS-1$
    	convert.addConvert(FunctionModifier.DATE, FunctionModifier.TIMESTAMP,  new CastModifier("TIMESTAMP")); //$NON-NLS-1$

    	convert.addConvert(FunctionModifier.STRING, FunctionModifier.INTEGER, new CastModifier("integer")); //$NON-NLS-1$
    	convert.addConvert(FunctionModifier.STRING, FunctionModifier.BIGDECIMAL, new CastModifier("decimal(37,5)"));//$NON-NLS-1$
    	convert.addConvert(FunctionModifier.STRING, FunctionModifier.BIGINTEGER, new CastModifier("numeric(18,0)"));//$NON-NLS-1$
    	convert.addConvert(FunctionModifier.STRING, FunctionModifier.FLOAT, new CastModifier("float"));//$NON-NLS-1$
    	convert.addConvert(FunctionModifier.STRING, FunctionModifier.BOOLEAN, new CastModifier("byteint"));//$NON-NLS-1$
    	convert.addConvert(FunctionModifier.STRING, FunctionModifier.LONG, new CastModifier("numeric(18,0)"));//$NON-NLS-1$
    	convert.addConvert(FunctionModifier.STRING, FunctionModifier.SHORT, new CastModifier("smallint"));//$NON-NLS-1$
    	convert.addConvert(FunctionModifier.STRING, FunctionModifier.DOUBLE, new CastModifier("double precision"));//$NON-NLS-1$
    	convert.addConvert(FunctionModifier.STRING, FunctionModifier.BYTE, new CastModifier("byteint")); //$NON-NLS-1$
    	
    	convert.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING,  new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList("cast(cast(", function.getParameters().get(0), " AS FORMAT 'Y4-MM-DDBHH:MI:SSDS(6)') AS VARCHAR(26))"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}); 
    	convert.addConvert(FunctionModifier.TIME, FunctionModifier.STRING,   new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList("cast(cast(", function.getParameters().get(0), " AS FORMAT 'HH:MI:SS') AS VARCHAR(9))"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}); 
    	convert.addConvert(FunctionModifier.DATE, FunctionModifier.STRING,  new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList("cast(cast(", function.getParameters().get(0), " AS FORMAT 'YYYY-MM-DD') AS VARCHAR(11))"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}); 
    	
    	convert.addTypeMapping("varchar(4000)", FunctionModifier.STRING); //$NON-NLS-1$
    	convert.addNumericBooleanConversions();
		
		registerFunctionModifier(SourceSystemFunctions.CONVERT, convert);
		registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new SubstrModifier(this.convert)); 
		registerFunctionModifier(SourceSystemFunctions.RAND, new AliasModifier("random")); //$NON-NLS-1$				
		registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier("LN")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.LCASE, new StringOnlyModifier("LOWER", this.convert)); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.UCASE, new StringOnlyModifier("UPPER", this.convert)); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.LENGTH, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				ArrayList target = new ArrayList();
				target.add("character_length("); //$NON-NLS-1$
				target.addAll(expressionToString(function.getParameters().get(0), convert));
				target.add(")"); //$NON-NLS-1$
				return target;
			}
		});
		registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("CURRENT_DATE")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.CURTIME, new AliasModifier("CURRENT_TIME")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.YEAR, new ExtractModifier("YEAR")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.MONTH, new ExtractModifier("MONTH")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new ExtractModifier("DAY")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.HOUR, new ExtractModifier("HOUR")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.MINUTE, new ExtractModifier("MINUTE")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.SECOND, new ExtractModifier("SECOND")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.LOCATE, new LocateModifier(this.convert));
        registerFunctionModifier(SourceSystemFunctions.LEFT, new LeftOrRightFunctionModifier(getLanguageFactory(), this.convert));
        registerFunctionModifier(SourceSystemFunctions.RIGHT, new LeftOrRightFunctionModifier(getLanguageFactory(), this.convert));
        registerFunctionModifier(SourceSystemFunctions.COT, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				function.setName(SourceSystemFunctions.TAN);
				return Arrays.asList(getLanguageFactory().createFunction(SourceSystemFunctions.DIVIDE_OP, new Expression[] {new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER), function}, TypeFacility.RUNTIME_TYPES.DOUBLE));
			}
		});        
        registerFunctionModifier(SourceSystemFunctions.LTRIM, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				ArrayList target = new ArrayList();
				target.add("TRIM(LEADING FROM ");//$NON-NLS-1$
				target.addAll(expressionToString(function.getParameters().get(0), convert));
				target.add(")"); //$NON-NLS-1$				
				return target; 
			}
		}); 
        registerFunctionModifier(SourceSystemFunctions.RTRIM, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				ArrayList target = new ArrayList();
				target.add("TRIM(TRAILING FROM ");//$NON-NLS-1$
				target.addAll(expressionToString(function.getParameters().get(0), convert));
				target.add(")"); //$NON-NLS-1$
				return target; 
			}
		}); 
        registerFunctionModifier(SourceSystemFunctions.MOD, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList(function.getParameters().get(0), " MOD ", function.getParameters().get(1)); //$NON-NLS-1$
			}
		});  
        
        addPushDownFunction(TERADATA, "COSH", FLOAT, FLOAT); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "TANH", FLOAT, FLOAT); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "ACOSH", FLOAT, FLOAT); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "ASINH", FLOAT, FLOAT); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "ATANH", FLOAT, FLOAT); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "CHAR2HEXINT", STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "INDEX", INTEGER, STRING, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "BYTES", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "OCTET_LENGTH", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "HASHAMP", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "HASHBAKAMP", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "HASHBUCKET", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "HASHROW", INTEGER, STRING); //$NON-NLS-1$
        addPushDownFunction(TERADATA, "NULLIFZERO", BIG_DECIMAL, BIG_DECIMAL); //$NON-NLS-1$        
        addPushDownFunction(TERADATA, "ZEROIFNULL", BIG_DECIMAL, BIG_DECIMAL); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.COT, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				function.setName(SourceSystemFunctions.TAN);
				return Arrays.asList(getLanguageFactory().createFunction(SourceSystemFunctions.DIVIDE_OP, new Expression[] {new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER), function}, TypeFacility.RUNTIME_TYPES.DOUBLE));
			}
		});        
        registerFunctionModifier(SourceSystemFunctions.LTRIM, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList("TRIM(LEADING FROM ", function.getParameters().get(0), ")"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}); 
        registerFunctionModifier(SourceSystemFunctions.RTRIM, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList("TRIM(TRAILING FROM ", function.getParameters().get(0), ")"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}); 
        registerFunctionModifier(SourceSystemFunctions.MOD, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList(function.getParameters().get(0), " MOD ", function.getParameters().get(1)); //$NON-NLS-1$
			}
		});        
	}

	@Override
    public SQLConversionVisitor getSQLConversionVisitor() {
    	return new TeradataSQLConversionVisitor(this);
    }
	
	
    @Override
    public List getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.ACOS);
        supportedFunctions.add(SourceSystemFunctions.ASIN);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        supportedFunctions.add(SourceSystemFunctions.ATAN2);
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
        supportedFunctions.add(SourceSystemFunctions.COS);
        supportedFunctions.add(SourceSystemFunctions.COT);
        supportedFunctions.add(SourceSystemFunctions.CONVERT);
		supportedFunctions.add(SourceSystemFunctions.CURDATE);
		supportedFunctions.add(SourceSystemFunctions.CURTIME); 
		supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.LEFT);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.NULLIF);
        supportedFunctions.add(SourceSystemFunctions.RAND);
        supportedFunctions.add(SourceSystemFunctions.RIGHT);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        supportedFunctions.add(SourceSystemFunctions.SIN);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.TAN);
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        supportedFunctions.add(SourceSystemFunctions.YEAR);

        return supportedFunctions;
    }
    
    @Override
    public String translateLiteralDate(Date dateValue) {
        return "cast('" + formatDateValue(dateValue) + "' AS DATE FORMAT 'yyyy-mm-dd')"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
        return "cast('" + formatDateValue(timeValue) + "' AS TIME(0) FORMAT 'hh:mi:ss')"; //$NON-NLS-1$//$NON-NLS-2$
    }
    
    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "cast('" + formatDateValue(timestampValue) + "' AS TIMESTAMP(6))"; //$NON-NLS-1$//$NON-NLS-2$ 
    }	
    
    // Teradata also supports MINUS & ALL set operators
    // more aggregates available

    @Override
    public boolean supportsScalarSubqueries() {
        return false;
    }
    
    @Override
    public boolean supportsUnions() {
    	return true;
    }

    @Override
    public boolean supportsIntersect() {
    	return true;
    }

    @Override
    public boolean supportsExcept() {
    	return true;
    }    
    
    @Override
    public boolean supportsInlineViews() {
        return true;
    }  
    
    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
    	return true;
    }
    
    @Override
    public boolean supportsCommonTableExpressions() {
    	return false;
    }    
    
    @Override
    public NullOrder getDefaultNullOrder() {
    	return NullOrder.FIRST;
    }
    
    @Override
    public boolean supportsSetQueryOrderBy() {
    	return false;
    }
    
    public static class LocateModifier extends FunctionModifier {
    	ConvertModifier convertModifier;
    	
    	public LocateModifier(ConvertModifier convertModifier) {
    		this.convertModifier = convertModifier;
    	}
		
    	@Override
		public List<?> translate(Function function) {
    		ArrayList target = new ArrayList();
    		Expression expr1 =  function.getParameters().get(0);
    		Expression expr2 =  function.getParameters().get(1);
    		if (function.getParameters().size() > 2) {
    			Expression expr3 =  function.getParameters().get(2);
	    		target.add("position("); //$NON-NLS-1$
	    		target.addAll(expressionToString(expr1, this.convertModifier));
	    		target.add( " in "); //$NON-NLS-1$
	    		target.add("substr("); //$NON-NLS-1$
	    		target.addAll(expressionToString(expr2, this.convertModifier));
	    		target.add(","); //$NON-NLS-1$
	    		target.add(expr3);
	    		target.add("))"); //$NON-NLS-1$	    		
    		}
    		else {
	    		target.add("position("); //$NON-NLS-1$
	    		target.addAll(expressionToString(expr1, this.convertModifier));
	    		target.add( " in "); //$NON-NLS-1$
	    		target.addAll(expressionToString(expr2, this.convertModifier));
	    		target.add(")"); //$NON-NLS-1$
    		}
    		return target;
		}
	}
    
    private static List<?> expressionToString(Expression expr, ConvertModifier modifier) {
    	Class tgtType = expr.getType();
		if (tgtType.equals(String.class) && ((expr instanceof Literal) || expr instanceof ColumnReference)) {
			return Arrays.asList(expr);  
		}
		else if (tgtType.equals(String.class) && (expr instanceof Function)) {
			
			Function func = (Function)expr;
			while(true) {
				Expression arg1 = func.getParameters().get(0);
				if ((arg1 instanceof Function) && ((Function)arg1).getName().equals("convert")) { //$NON-NLS-1$
					func = (Function)arg1;
				}
				else {
					break;
				}
			}
			Expression arg1 = func.getParameters().get(0);
			if (arg1 instanceof ColumnReference) {
				ColumnReference ref = (ColumnReference)func.getParameters().get(0);
				if(Number.class.isAssignableFrom(ref.getType())) {
					ArrayList target = new ArrayList();
					target.add("cast("); //$NON-NLS-1$
					target.add(func.getParameters().get(0));
					target.add(" AS varchar(100))"); //$NON-NLS-1$
					return target;
				}
				else if (String.class.isAssignableFrom(ref.getType())) {
					return Arrays.asList(ref);
				}
			}
			return modifier.translate(func);	
		} 
		return Arrays.asList("cast(" , expr, " AS varchar(100))"); //$NON-NLS-1$ //$NON-NLS-2$ 
    }

	public static class ExtractModifier extends FunctionModifier {
    	private String type;
    	public ExtractModifier(String type) {
    		this.type = type;
    	}
		@Override
		public List<?> translate(Function function) {
			return Arrays.asList("extract(",this.type," from ",function.getParameters().get(0) ,")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 				
		}
	}
    
    public static class CastModifier extends FunctionModifier {
    	private String target;
    	public CastModifier(String target) {
    		this.target = target;
    	}
		@Override
		public List<?> translate(Function function) {
			return Arrays.asList("cast(", function.getParameters().get(0), " AS "+this.target+")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		}
	}
    
    public static class StringOnlyModifier extends FunctionModifier {
    	String funcName;
    	ConvertModifier convertModifier;
    	public StringOnlyModifier(String name, ConvertModifier converModifier) {
    		this.funcName = name;
    		this.convertModifier = converModifier;
    	}
    	@Override
		public List<?> translate(Function function) {
			Expression expr = function.getParameters().get(0);
			ArrayList target = new ArrayList();
			target.add(this.funcName);
			target.add("("); //$NON-NLS-1$
			target.addAll(expressionToString(expr, this.convertModifier));
			target.add(")"); //$NON-NLS-1$
			return target;
		}
	}
    
    public static class SubstrModifier extends FunctionModifier {
    	ConvertModifier convertModifier;
    	public SubstrModifier(ConvertModifier converModifier) {
    		this.convertModifier = converModifier;
    	}
    	@Override
		public List<?> translate(Function function) {
			Expression expr = function.getParameters().get(0);
			ArrayList target = new ArrayList();
			target.add("substr("); //$NON-NLS-1$
			target.addAll(expressionToString(expr, this.convertModifier));
			target.add(","); //$NON-NLS-1$
			target.add(function.getParameters().get(1)); 
			if (function.getParameters().size() > 2 ) {
				target.add(","); //$NON-NLS-1$
				target.add(function.getParameters().get(2)); 
			}
			target.add(")"); //$NON-NLS-1$
			return target;
		}
	}    
    
    public static class LeftOrRightFunctionModifier extends FunctionModifier {
        private LanguageFactory langFactory;
        ConvertModifier convertModifier;
        
        public LeftOrRightFunctionModifier(LanguageFactory langFactory, ConvertModifier converModifier) {
            this.langFactory = langFactory;
            this.convertModifier = converModifier;
        }
        
        @Override
        public List<?> translate(Function function) {
            List<Expression> args = function.getParameters();
            ArrayList target = new ArrayList();
            if (function.getName().equalsIgnoreCase("left")) { //$NON-NLS-1$
            	//substr(string, 1, length)
            	target.add("substr("); //$NON-NLS-1$
            	target.addAll(expressionToString(args.get(0), this.convertModifier));
            	target.add(","); //$NON-NLS-1$
            	target.add(langFactory.createLiteral(Integer.valueOf(1), TypeFacility.RUNTIME_TYPES.INTEGER));
            	target.add(","); //$NON-NLS-1$
            	target.add(args.get(1));
            	target.add(")"); //$NON-NLS-1$
            } else if (function.getName().equalsIgnoreCase("right")) { //$NON-NLS-1$
            	//substr(case_size, character_length(case_size) -4) 
            	target.add("substr("); //$NON-NLS-1$
            	target.addAll(expressionToString(args.get(0), this.convertModifier));
            	
            	target.add(",(character_length("); //$NON-NLS-1$
            	target.addAll(expressionToString(args.get(0), this.convertModifier));
            	target.add(")-"); //$NON-NLS-1$
            	target.add(args.get(1));
            	target.add("+1))"); //$NON-NLS-1$ // offset for 1 based index
            }
            return target;
        }
    }    
    
    public static class UpperOrLowerModifier extends FunctionModifier {
    	String funcName;
    	ConvertModifier convertModifier;
    	public UpperOrLowerModifier(String name, ConvertModifier converModifier) {
    		this.funcName = name;
    		this.convertModifier = converModifier;
    	}
    	@Override
		public List<?> translate(Function function) {
			Expression expr = function.getParameters().get(0);
			ArrayList target = new ArrayList();
			target.add(this.funcName);
			target.add("("); //$NON-NLS-1$
			target.addAll(expressionToString(expr, this.convertModifier));
			target.add(")"); //$NON-NLS-1$
			return target;
		}
	}    
}
