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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageObject;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;



/** 
 * Translator class for accessing the ModeShape JCR repository.  
 */
@Translator(name="modeshape", description="A translator for open source Modeshape JCA repository")
public class ModeShapeExecutionFactory extends JDBCExecutionFactory {
	
	public ModeShapeExecutionFactory() {
		setDatabaseVersion("2.0"); //$NON-NLS-1$
	}
	
    @Override
    public void start() throws TranslatorException {
        super.start();
        
        registerFunctionModifier("PATH", new FunctionModifier() { //$NON-NLS-1$
            
            @Override
            public List<?> translate(Function function) {
           	List<Object> objs = new ArrayList<Object>();

        	List<Expression> parms = function.getParameters();
        	
        	for (Expression s : parms) 
        	{
        	    String v = s.toString();
        	    v.replace('\'', ' ');
        	    objs.add(v);
         	}

                return objs; 
            	}
        }   );
        	


           
        //add in type conversion
        ConvertModifier convertModifier = new ConvertModifier();
   	 
        
        convertModifier.addTypeMapping("boolean", FunctionModifier.BOOLEAN); //$NON-NLS-1$
    	convertModifier.addTypeMapping("smallint", FunctionModifier.BYTE, FunctionModifier.SHORT); //$NON-NLS-1$
    	convertModifier.addTypeMapping("integer", FunctionModifier.INTEGER); //$NON-NLS-1$
    	convertModifier.addTypeMapping("bigint", FunctionModifier.LONG); //$NON-NLS-1$
    	convertModifier.addTypeMapping("real", FunctionModifier.FLOAT); //$NON-NLS-1$
    	convertModifier.addTypeMapping("float8", FunctionModifier.DOUBLE); //$NON-NLS-1$
    	convertModifier.addTypeMapping("numeric(38)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$
    	convertModifier.addTypeMapping("decimal", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
    	convertModifier.addTypeMapping("char(1)", FunctionModifier.CHAR); //$NON-NLS-1$
    	convertModifier.addTypeMapping("varchar(4000)", FunctionModifier.STRING); //$NON-NLS-1$
    	convertModifier.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
    	convertModifier.addTypeMapping("time", FunctionModifier.TIME); //$NON-NLS-1$
    	convertModifier.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
    	convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.TIMESTAMP, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList(function.getParameters().get(0), " + TIMESTAMP '1970-01-01'"); //$NON-NLS-1$
			}
		});
    	convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.TIME, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList("cast(date_trunc('second', ", function.getParameters().get(0), ") AS time)"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		});
    	convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", "YYYY-MM-DD")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", "HH24:MI:SS")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", "YYYY-MM-DD HH24:MI:SS.UF")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.STRING, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				Expression stringValue = function.getParameters().get(0);
				return Arrays.asList("CASE WHEN ", stringValue, " THEN 'true' WHEN not(", stringValue, ") THEN 'false' END"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			}
		});
    	convertModifier.addSourceConversion(new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				((Literal)function.getParameters().get(1)).setValue("integer"); //$NON-NLS-1$
				return null;
			}
		}, FunctionModifier.BOOLEAN);
     }    
    

	@Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {

	if (obj instanceof NamedTable) {

	    NamedTable nt = (NamedTable) obj;
	    List<String> ntlist = new ArrayList<String>(1);

	    ntlist.add("[" + nt.getMetadataObject().getNameInSource() + "]"); //$NON-NLS-1$ //$NON-NLS-2$
	    return ntlist;
	}

	if (obj instanceof ColumnReference) {
	    ColumnReference elem = (ColumnReference) obj;
	    List<String> ntlist = new ArrayList<String>(1);
	    ntlist.add("[" + elem.getMetadataObject().getNameInSource() + "]"); //$NON-NLS-1$ //$NON-NLS-2$
	    return ntlist;

	}

	return super.translate(obj, context);
    }
    
    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "TRUE"; //$NON-NLS-1$
        }
        return "FALSE"; //$NON-NLS-1$
    }
    
    @Override
    public String translateLiteralDate(Date dateValue) {
        return "DATE '" + formatDateValue(dateValue) + "'"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
        return "TIME '" + formatDateValue(timeValue) + "'"; //$NON-NLS-1$//$NON-NLS-2$
    }
    
    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "TIMESTAMP '" + formatDateValue(timestampValue) + "'"; //$NON-NLS-1$//$NON-NLS-2$ 
    }
    
    @Override
    public int getTimestampNanoPrecision() {
    	return 6;
    }
    
    @Override
    public List<String> getSupportedFunctions() {
	List<String> supportedFunctions = new ArrayList<String>();
	supportedFunctions.addAll(super.getSupportedFunctions());
	supportedFunctions.add("PATH"); //$NON-NLS-1$
	supportedFunctions.add("NAME"); //$NON-NLS-1$
	supportedFunctions.add("ISCHILDNODE"); //$NON-NLS-1$
	
	return supportedFunctions;

    }
        
    @Override
    public boolean useBindVariables() {
		return false;
	}
    
}
