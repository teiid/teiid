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

package org.teiid.translator.jdbc.mysql;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.GeometryType;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.language.Command;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.QueryExpression;
import org.teiid.language.SQLConstants;
import org.teiid.language.Select;
import org.teiid.language.SetQuery;
import org.teiid.metadata.Table;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.JDBCMetdataProcessor;
import org.teiid.translator.jdbc.LocateFunctionModifier;


/** 
 * @since 4.3
 */
@Translator(name="mysql", description="A translator for open source MySQL Database, used with any version lower than 5")
public class MySQLExecutionFactory extends JDBCExecutionFactory {
	
	public MySQLExecutionFactory() {
		setSupportsFullOuterJoins(false);
	}
	
	/**
	 * Adds support for the 2 argument form of padding
	 */
	private final class PadFunctionModifier extends FunctionModifier {
		@Override
		public List<?> translate(Function function) {
			if (function.getParameters().size() == 2) {
				function.getParameters().add(getLanguageFactory().createLiteral(" ", TypeFacility.RUNTIME_TYPES.STRING)); //$NON-NLS-1$
			}
			return null;
		}
	}

	@Override
    public void start() throws TranslatorException {
        super.start();
        registerFunctionModifier(SourceSystemFunctions.BITAND, new BitFunctionModifier("&", getLanguageFactory())); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITNOT, new BitFunctionModifier("~", getLanguageFactory())); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITOR, new BitFunctionModifier("|", getLanguageFactory())); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITXOR, new BitFunctionModifier("^", getLanguageFactory())); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new LocateFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.LPAD, new PadFunctionModifier());
        registerFunctionModifier(SourceSystemFunctions.RPAD, new PadFunctionModifier());
        //WEEKINYEAR assumes 4.1.1
        registerFunctionModifier(SourceSystemFunctions.WEEK, new AliasModifier("WEEKOFYEAR")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ST_ASBINARY, new AliasModifier("AsWKB")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ST_ASTEXT, new AliasModifier("AsWKT")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ST_GEOMFROMWKB, new AliasModifier("GeomFromWKB")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.ST_GEOMFROMTEXT, new AliasModifier("GeomFromText")); //$NON-NLS-1$

        //add in type conversion
        ConvertModifier convertModifier = new ConvertModifier();
        convertModifier.addTypeMapping("signed", FunctionModifier.BOOLEAN, FunctionModifier.BYTE, FunctionModifier.SHORT, FunctionModifier.INTEGER, FunctionModifier.LONG); //$NON-NLS-1$
    	//char(n) assume 4.1 or later
    	convertModifier.addTypeMapping("char(1)", FunctionModifier.CHAR); //$NON-NLS-1$
    	convertModifier.addTypeMapping("char", FunctionModifier.STRING); //$NON-NLS-1$
    	convertModifier.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
    	convertModifier.addTypeMapping("time", FunctionModifier.TIME); //$NON-NLS-1$
    	convertModifier.addTypeMapping("datetime", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.DATE, new ConvertModifier.FormatModifier("DATE")); //$NON-NLS-1$
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIME, new ConvertModifier.FormatModifier("TIME")); //$NON-NLS-1$
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIMESTAMP, new ConvertModifier.FormatModifier("TIMESTAMP")); //$NON-NLS-1$
    	convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.STRING, new ConvertModifier.FormatModifier("date_format", "%Y-%m-%d")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.STRING, new ConvertModifier.FormatModifier("date_format", "%H:%i:%S")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING, new ConvertModifier.FormatModifier("date_format", "%Y-%m-%d %H:%i:%S.%f")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addTypeConversion(new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList("(", function.getParameters().get(0), " + 0.0)"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}, FunctionModifier.BIGDECIMAL, FunctionModifier.BIGINTEGER, FunctionModifier.FLOAT, FunctionModifier.DOUBLE);
    	convertModifier.addNumericBooleanConversions();
    	convertModifier.setWideningNumericImplicit(true);
    	registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);
    }  
	
	@Override
    public String translateLiteralDate(Date dateValue) {
        return "DATE('" + formatDateValue(dateValue) + "')";  //$NON-NLS-1$//$NON-NLS-2$
    }

	@Override
    public String translateLiteralTime(Time timeValue) {
        return "TIME('" + formatDateValue(timeValue) + "')";  //$NON-NLS-1$//$NON-NLS-2$
    }

	@Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "{ts '" + formatDateValue(timestampValue) + "'}";  //$NON-NLS-1$//$NON-NLS-2$
    }
	
	@Override
	public boolean useParensForSetQueries() {
		return true;
	}
	
	@Override
	public int getTimestampNanoPrecision() {
		return 0;
	}
	
	@Override
    public boolean useParensForJoins() {
    	return true;
    }
    
    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.ABS); 
        supportedFunctions.add(SourceSystemFunctions.ACOS); 
        supportedFunctions.add(SourceSystemFunctions.ASIN);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        supportedFunctions.add(SourceSystemFunctions.ATAN2);
        supportedFunctions.add(SourceSystemFunctions.BITAND);
        supportedFunctions.add(SourceSystemFunctions.BITNOT);
        supportedFunctions.add(SourceSystemFunctions.BITOR);
        supportedFunctions.add(SourceSystemFunctions.BITXOR);
        supportedFunctions.add(SourceSystemFunctions.CEILING);
        supportedFunctions.add(SourceSystemFunctions.COS);
        supportedFunctions.add(SourceSystemFunctions.COT);
        supportedFunctions.add(SourceSystemFunctions.DEGREES);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.LOG10);
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.PI);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.RADIANS);
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.SIGN);
        supportedFunctions.add(SourceSystemFunctions.SIN);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.TAN);

        supportedFunctions.add(SourceSystemFunctions.ASCII);
        supportedFunctions.add(SourceSystemFunctions.CHAR);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add(SourceSystemFunctions.INSERT);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LEFT);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.REPEAT);
        supportedFunctions.add(SourceSystemFunctions.REPLACE);
        supportedFunctions.add(SourceSystemFunctions.RIGHT);
        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        
        // These are executed within the server and never pushed down
//        supportedFunctions.add("CURDATE"); //$NON-NLS-1$
//        supportedFunctions.add("CURTIME"); //$NON-NLS-1$
//        supportedFunctions.add("NOW"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.DAYNAME);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.DAYOFWEEK);
        supportedFunctions.add(SourceSystemFunctions.DAYOFYEAR);
        
        // These should not be pushed down since the grammar for string conversion is different
//        supportedFunctions.add("FORMATDATE"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATTIME"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATTIMESTAMP"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.MONTHNAME);
        
        // These should not be pushed down since the grammar for string conversion is different
//        supportedFunctions.add("PARSEDATE"); //$NON-NLS-1$
//        supportedFunctions.add("PARSETIME"); //$NON-NLS-1$
//        supportedFunctions.add("PARSETIMESTAMP"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.QUARTER);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
//        supportedFunctions.add(SourceSystemFunctions.TIMESTAMPADD);
//        supportedFunctions.add(SourceSystemFunctions.TIMESTAMPDIFF);
        supportedFunctions.add(SourceSystemFunctions.WEEK);
        supportedFunctions.add(SourceSystemFunctions.YEAR);

        supportedFunctions.add(SourceSystemFunctions.CONVERT);
        supportedFunctions.add(SourceSystemFunctions.IFNULL);
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
        
        supportedFunctions.add(SourceSystemFunctions.ST_ASBINARY);
        supportedFunctions.add(SourceSystemFunctions.ST_ASTEXT);
        supportedFunctions.add(SourceSystemFunctions.ST_GEOMFROMWKB);
        supportedFunctions.add(SourceSystemFunctions.ST_GEOMFROMTEXT);
        
//        supportedFunctions.add("GREATEST"); //$NON-NLS-1$
//        supportedFunctions.add("ISNULL"); //$NON-NLS-1$
//        supportedFunctions.add("LEAST"); //$NON-NLS-1$
//        supportedFunctions.add("STRCMP"); // String-specific //$NON-NLS-1$
//        
//        // String
//        supportedFunctions.add("BIN"); //$NON-NLS-1$
//        supportedFunctions.add("BIT_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("CHAR_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("CHARACTER_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("COMPRESS"); //$NON-NLS-1$
//        supportedFunctions.add("CONCAT_WS"); //$NON-NLS-1$
//        supportedFunctions.add("CONV"); //$NON-NLS-1$
//        supportedFunctions.add("ELT"); //$NON-NLS-1$
//        supportedFunctions.add("EXPORT_SET"); //$NON-NLS-1$
//        supportedFunctions.add("FIELD"); //$NON-NLS-1$
//        supportedFunctions.add("FIND_IN_SET"); //$NON-NLS-1$
//        supportedFunctions.add("FORMAT"); //$NON-NLS-1$
//        supportedFunctions.add("HEX"); //$NON-NLS-1$
//        supportedFunctions.add("INSTR"); //$NON-NLS-1$
//        supportedFunctions.add("LOAD_FILE"); //$NON-NLS-1$
//        supportedFunctions.add("MAKE_SET"); //$NON-NLS-1$
//        supportedFunctions.add("MID"); //$NON-NLS-1$
//        supportedFunctions.add("OCT"); //$NON-NLS-1$
//        supportedFunctions.add("OCTET_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("ORD"); //$NON-NLS-1$
//        supportedFunctions.add("QUOTE"); //$NON-NLS-1$
//        supportedFunctions.add("REVERSE"); //$NON-NLS-1$
//        supportedFunctions.add("SOUNDEX"); //$NON-NLS-1$
//        supportedFunctions.add("SPACE"); //$NON-NLS-1$
//        supportedFunctions.add("SUBSTR"); //$NON-NLS-1$
//        supportedFunctions.add("SUBSTRING_INDEX"); //$NON-NLS-1$
//        supportedFunctions.add("TRIM"); //$NON-NLS-1$
//        supportedFunctions.add("UNCOMPRESS"); //$NON-NLS-1$
//        supportedFunctions.add("UNHEX"); //$NON-NLS-1$
//        
//        // Math
//        supportedFunctions.add("CEIL"); //$NON-NLS-1$
//        supportedFunctions.add("CRC32"); //$NON-NLS-1$
//          // DIV is an operator equivalent to '/'
//        supportedFunctions.add("DIV"); //$NON-NLS-1$
//        supportedFunctions.add("FORMAT"); //$NON-NLS-1$
//        supportedFunctions.add("LN"); //$NON-NLS-1$
//        supportedFunctions.add("LOG2"); //$NON-NLS-1$
//        supportedFunctions.add("POW"); //$NON-NLS-1$
//        supportedFunctions.add("RAND"); //$NON-NLS-1$
//        supportedFunctions.add("TRUNCATE"); //$NON-NLS-1$
//        
//        // Date / Time
//        supportedFunctions.add("ADDDATE"); //$NON-NLS-1$
//        supportedFunctions.add("ADDTIME"); //$NON-NLS-1$
//        supportedFunctions.add("CONVERT_TZ"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_DATE"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_TIME"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_TIMESTAMP"); //$NON-NLS-1$
//        supportedFunctions.add("DATE"); //$NON-NLS-1$
//        supportedFunctions.add("DATEDIFF"); //$NON-NLS-1$
////        supportedFunctions.add("DATE_ADD");
////        supportedFunctions.add("DATE_SUB");
//        supportedFunctions.add("DATE_FORMAT"); //$NON-NLS-1$
//        supportedFunctions.add("DAY"); //$NON-NLS-1$
////        supportedFunctions.add("EXTRACT");
//        supportedFunctions.add("FROM_DAYS"); //$NON-NLS-1$
//        supportedFunctions.add("FROM_UNIXTIME"); //$NON-NLS-1$
//        supportedFunctions.add("GET_FORMAT"); //$NON-NLS-1$
//        supportedFunctions.add("LAST_DAY"); //$NON-NLS-1$
//        supportedFunctions.add("LOCALTIME"); //$NON-NLS-1$
//        supportedFunctions.add("LOCALTIMESTAMP"); //$NON-NLS-1$
//        supportedFunctions.add("MAKEDATE"); //$NON-NLS-1$
//        supportedFunctions.add("MAKETIME"); //$NON-NLS-1$
//        supportedFunctions.add("MICROSECOND"); //$NON-NLS-1$
//        supportedFunctions.add("PERIOD_ADD"); //$NON-NLS-1$
//        supportedFunctions.add("PERIOD_DIFF"); //$NON-NLS-1$
//        supportedFunctions.add("SEC_TO_TIME"); //$NON-NLS-1$
//        supportedFunctions.add("STR_TO_DATE"); //$NON-NLS-1$
//        supportedFunctions.add("SUBDATE"); //$NON-NLS-1$
//        supportedFunctions.add("SUBTIME"); //$NON-NLS-1$
//        supportedFunctions.add("SYSDATE"); //$NON-NLS-1$
//        supportedFunctions.add("TIME"); //$NON-NLS-1$
//        supportedFunctions.add("TIMEDIFF"); //$NON-NLS-1$
//        supportedFunctions.add("TIMESTAMP"); //$NON-NLS-1$
//        supportedFunctions.add("TIME_FORMAT"); //$NON-NLS-1$
//        supportedFunctions.add("TIME_TO_SEC"); //$NON-NLS-1$
//        supportedFunctions.add("TO_DAYS"); //$NON-NLS-1$
//        supportedFunctions.add("UNIX_TIMESTAMP"); //$NON-NLS-1$
//        supportedFunctions.add("UTC_DATE"); //$NON-NLS-1$
//        supportedFunctions.add("UTC_TIME"); //$NON-NLS-1$
//        supportedFunctions.add("UTC_TIMESTAMP"); //$NON-NLS-1$
//        supportedFunctions.add("WEEKDAY"); //$NON-NLS-1$
//        supportedFunctions.add("WEEKOFYEAR"); //$NON-NLS-1$
//        supportedFunctions.add("YEARWEEK"); //$NON-NLS-1$
//        
//        // Bit
//        supportedFunctions.add("|"); //$NON-NLS-1$
//        supportedFunctions.add("&"); //$NON-NLS-1$
//        supportedFunctions.add("^"); //$NON-NLS-1$
//        supportedFunctions.add("<<"); //$NON-NLS-1$
//        supportedFunctions.add(">>"); //$NON-NLS-1$
//        supportedFunctions.add("~"); //$NON-NLS-1$
//        supportedFunctions.add("BIT_COUNT"); //$NON-NLS-1$
//        
//        // Encryption
//        supportedFunctions.add("AES_ENCRYPT"); //$NON-NLS-1$
//        supportedFunctions.add("AES_DECRYPT"); //$NON-NLS-1$
//        supportedFunctions.add("DECODE"); //$NON-NLS-1$
//        supportedFunctions.add("ENCODE"); //$NON-NLS-1$
//        supportedFunctions.add("DES_ENCRYPT"); //$NON-NLS-1$
//        supportedFunctions.add("DES_DECRYPT"); //$NON-NLS-1$
//        supportedFunctions.add("MD5"); //$NON-NLS-1$
//        supportedFunctions.add("OLD_PASSWORD"); //$NON-NLS-1$
//        supportedFunctions.add("PASSWORD"); //$NON-NLS-1$
//        supportedFunctions.add("SHA"); //$NON-NLS-1$
//        supportedFunctions.add("SHA1"); //$NON-NLS-1$
//        
//        // Information
//        supportedFunctions.add("BENCHMARK"); //$NON-NLS-1$
//        supportedFunctions.add("CHARSET"); //$NON-NLS-1$
//        supportedFunctions.add("COERCIBILITY"); //$NON-NLS-1$
//        supportedFunctions.add("COLLATION"); //$NON-NLS-1$
//        supportedFunctions.add("CONNECTION_ID"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_USER"); //$NON-NLS-1$
//        supportedFunctions.add("DATABASE"); //$NON-NLS-1$
//        supportedFunctions.add("FOUND_ROWS"); //$NON-NLS-1$
//        supportedFunctions.add("LAST_INSERT_ID"); //$NON-NLS-1$
//        supportedFunctions.add("ROW_COUNT"); //$NON-NLS-1$
//        supportedFunctions.add("SCHEMA"); //$NON-NLS-1$
//        supportedFunctions.add("SESSION_USER"); //$NON-NLS-1$
//        supportedFunctions.add("SYSTEM_USER"); //$NON-NLS-1$
//        supportedFunctions.add("USER"); //$NON-NLS-1$
//        supportedFunctions.add("VERSION"); //$NON-NLS-1$
//        
//        // Misc.
//        supportedFunctions.add("DEFAULT"); //$NON-NLS-1$
//        supportedFunctions.add("FORMAT"); //$NON-NLS-1$
////        supportedFunctions.add("GET_LOCK"); //$NON-NLS-1$
//        supportedFunctions.add("INET_ATON"); //$NON-NLS-1$
//        supportedFunctions.add("INET_NTOA"); //$NON-NLS-1$
////        supportedFunctions.add("IS_FREE_LOCK"); //$NON-NLS-1$
////        supportedFunctions.add("IS_USED_LOCK"); //$NON-NLS-1$
////        supportedFunctions.add("MASTER_POS_WAIT"); //$NON-NLS-1$
////        supportedFunctions.add("NAME_CONST"); //$NON-NLS-1$
////        supportedFunctions.add("RELEASE_LOCK"); //$NON-NLS-1$
////        supportedFunctions.add("SLEEP"); //$NON-NLS-1$
//        supportedFunctions.add("UUID"); //$NON-NLS-1$
//        supportedFunctions.add("VALUES"); //$NON-NLS-1$
        return supportedFunctions;
    }
    
    @Override
    public boolean supportsAggregatesDistinct() {
        return false;
    }
        
    @Override
    public boolean supportsRowLimit() {
        return true;
    }
    
    @Override
    public boolean supportsRowOffset() {
        return true;
    }  
    
    @Override
    public boolean supportsSelectWithoutFrom() {
    	return true;
    }
    
    @Override
    public String getHibernateDialectClassName() {
    	return "org.hibernate.dialect.MySQLDialect"; //$NON-NLS-1$
    }
    
    @Override
    public MetadataProcessor<Connection> getMetadataProcessor() {
        return new JDBCMetdataProcessor() {
            @Override
            protected String getRuntimeType(int type, String typeName, int precision) {
                //mysql will otherwise report a 0/null type for geometry
            	if ("geometry".equalsIgnoreCase(typeName)) { //$NON-NLS-1$
                    return TypeFacility.RUNTIME_NAMES.GEOMETRY;
                }                
                return super.getRuntimeType(type, typeName, precision);                    
            }
                
    		@Override
    		protected void getTableStatistics(Connection conn, String catalog, String schema, String name, Table table) throws SQLException {
    	        PreparedStatement stmt = null;
    	        ResultSet rs = null;
		        try {
		            stmt = conn.prepareStatement("SELECT cardinality FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema = ? AND table_name = ?");  //$NON-NLS-1$
		            if (catalog != null && schema == null) {
		            	//mysql jdbc reports the schema as the catalog
		            	stmt.setString(1, catalog);
		            } else {
		            	stmt.setString(1, schema);
		            }
		            stmt.setString(2, name);
		            rs = stmt.executeQuery();
		            if(rs.next()) {
		            	int cardinality = rs.getInt(1);
		            	if (!rs.wasNull()) {
		            		table.setCardinality(cardinality);
		            	}
		            }
		        } finally { 
		            if(rs != null) {
		                rs.close();
		            }
		            if(stmt != null) {
		                stmt.close();
		            }
		        }
    		}
    	};
    }
    
    @Override
    @Deprecated
    protected JDBCMetdataProcessor createMetadataProcessor() {
        return (JDBCMetdataProcessor)getMetadataProcessor();
    }    
    
    @Override
    public Expression translateGeometrySelect(Expression expr) {
    	return expr;
    }
    
    @Override
    public GeometryType retrieveGeometryValue(ResultSet results, int paramIndex) throws SQLException {
        Blob val = results.getBlob(paramIndex);
        
        return toGeometryType(val);
    }
    
    @Override
    public Object retrieveValue(CallableStatement results, int parameterIndex,
    		Class<?> expectedType) throws SQLException {
        Blob val = results.getBlob(parameterIndex);
        
        return toGeometryType(val);
    }

    /**
     * It appears that mysql will actually return a byte array or a blob backed by a byte array
     * but just to be safe we'll assume that there may be true blob and that we should back the
     * geometry value with that blob.
     * @param val
     * @return
     * @throws SQLException
     */
	GeometryType toGeometryType(final Blob val) throws SQLException {
        if (val == null) {
        	return null;
        }
    	//create a wrapper for that will handle the srid
    	long length = val.length() - 4;
    	InputStreamFactory streamFactory = new InputStreamFactory() {
			
			@Override
			public InputStream getInputStream() throws IOException {
				InputStream is;
				try {
					is = val.getBinaryStream();
				} catch (SQLException e) {
					throw new IOException(e);
				}
				for (int i = 0; i < 4; i++) {
					is.read();
				}
				return is;
			}
			
		};
		
		//read the little endian srid
		InputStream is = val.getBinaryStream();
		int srid = 0;
		try {
			for (int i = 0; i < 4; i++) {
				try {
					int b = is.read();
					srid += (b << i*8);
				} catch (IOException e) {
					srid = GeometryType.UNKNOWN_SRID; //could not determine srid
				}
			}
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				//i
			}
		}
		
		streamFactory.setLength(length);
		Blob b = new BlobImpl(streamFactory);
    	
		GeometryType geom = new GeometryType(b);
        geom.setSrid(srid);
        return geom;
	}
	
	@Override
	public List<?> translateCommand(Command command, ExecutionContext context) {
		if (command instanceof SetQuery) {
			//mysql may not be able to find a common collation if a cast is used in a union
			//TODO: it's a little sloppy to do this here as there can be nested set queries
			SetQuery sq = (SetQuery)command;
			if (!sq.isAll()) {
				List<Select> allQueries = new ArrayList<Select>();
				gatherSelects(sq, allQueries);
				int size = allQueries.get(0).getDerivedColumns().size();
				outer: for (int i = 0; i < size; i++) {
					boolean casted = false;
					boolean notCasted = false;
					for (Select select : allQueries) {
						Expression ex = select.getDerivedColumns().get(i).getExpression();
						if (ex.getType() != TypeFacility.RUNTIME_TYPES.STRING) {
							continue outer;
						}
						if (ex instanceof Function) {
							Function f = (Function)ex;
							if (f.getName().equalsIgnoreCase(SourceSystemFunctions.CONVERT)) {
								casted = true;
								continue;
							}
						}
						notCasted = true;
					}
					if (casted && notCasted) {
						//allow mysql to implicitly convert
						for (Select select : allQueries) {
							DerivedColumn dc = select.getDerivedColumns().get(i);
							if ((dc.getExpression() instanceof Function) &&
									(((Function)dc.getExpression()).getName().equalsIgnoreCase(SQLConstants.Reserved.CONVERT))) {
								dc.setExpression(((Function)dc.getExpression()).getParameters().get(0));
							}
						}	
					}
				}
			}
		}
		return super.translateCommand(command, context);
	}

	private void gatherSelects(QueryExpression qe, List<Select> allQueries) {
		if (qe instanceof Select) {
			allQueries.add((Select)qe);
			return;
		}
		SetQuery sq = (SetQuery)qe;
		gatherSelects(sq.getLeftQuery(), allQueries);
		gatherSelects(sq.getRightQuery(), allQueries);
	}
}
