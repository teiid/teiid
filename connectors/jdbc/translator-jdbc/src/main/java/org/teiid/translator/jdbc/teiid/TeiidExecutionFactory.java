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

/*
 */
package org.teiid.translator.jdbc.teiid;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.boot.TempTableDdlTransactionHandling;
import org.hibernate.hql.spi.id.AbstractMultiTableBulkIdStrategyImpl;
import org.hibernate.hql.spi.id.IdTableSupportStandardImpl;
import org.hibernate.hql.spi.id.local.AfterUseAction;
import org.hibernate.hql.spi.id.local.LocalTemporaryTableBulkIdStrategy;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.language.SQLConstants;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.JDBCMetdataProcessor;
import org.teiid.translator.jdbc.SQLConversionVisitor;
import org.teiid.translator.jdbc.SQLDialect;
import org.teiid.util.Version;

/** 
 * @since 4.3
 */
@Translator(name="teiid", description="A translator for Teiid 7.0 or later")
public class TeiidExecutionFactory extends JDBCExecutionFactory {
	
	public static final Version SEVEN_0 = Version.getVersion("7.0"); //$NON-NLS-1$
	public static final Version SEVEN_1 = Version.getVersion("7.1"); //$NON-NLS-1$
	public static final Version SEVEN_2 = Version.getVersion("7.2"); //$NON-NLS-1$
	public static final Version SEVEN_3 = Version.getVersion("7.3"); //$NON-NLS-1$
	public static final Version SEVEN_4 = Version.getVersion("7.4"); //$NON-NLS-1$
	public static final Version SEVEN_5 = Version.getVersion("7.5"); //$NON-NLS-1$
	public static final Version SEVEN_6 = Version.getVersion("7.6"); //$NON-NLS-1$
	public static final Version EIGHT_1 = Version.getVersion("8.1"); //$NON-NLS-1$
	public static final Version EIGHT_3 = Version.getVersion("8.3"); //$NON-NLS-1$
	public static final Version EIGHT_4 = Version.getVersion("8.4"); //$NON-NLS-1$
	public static final Version EIGHT_5 = Version.getVersion("8.5"); //$NON-NLS-1$
	public static final Version EIGHT_10 = Version.getVersion("8.10"); //$NON-NLS-1$
	public static final Version NINE_0 = Version.getVersion("9.0"); //$NON-NLS-1$
	public static final Version NINE_1 = Version.getVersion("9.1"); //$NON-NLS-1$
	public static final Version NINE_2 = Version.getVersion("9.2"); //$NON-NLS-1$
	
	public TeiidExecutionFactory() {
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
        supportedFunctions.add("FORMATBIGDECIMAL"); //$NON-NLS-1$
        supportedFunctions.add("FORMATBIGINTEGER"); //$NON-NLS-1$
        supportedFunctions.add("FORMATDOUBLE"); //$NON-NLS-1$
        supportedFunctions.add("FORMATFLOAT"); //$NON-NLS-1$
        supportedFunctions.add("FORMATINTEGER"); //$NON-NLS-1$
        supportedFunctions.add("FORMATLONG"); //$NON-NLS-1$
        supportedFunctions.add("LOG"); //$NON-NLS-1$
        supportedFunctions.add("LOG10"); //$NON-NLS-1$
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        supportedFunctions.add("PARSEBIGDECIMAL"); //$NON-NLS-1$
        supportedFunctions.add("PARSEBIGINTEGER"); //$NON-NLS-1$
        supportedFunctions.add("PARSEDOUBLE"); //$NON-NLS-1$
        supportedFunctions.add("PARSEFLOAT"); //$NON-NLS-1$
        supportedFunctions.add("PARSEINTEGER"); //$NON-NLS-1$
        supportedFunctions.add("PARSELONG"); //$NON-NLS-1$
        supportedFunctions.add("PI"); //$NON-NLS-1$
        supportedFunctions.add("POWER"); //$NON-NLS-1$
        supportedFunctions.add("RADIANS"); //$NON-NLS-1$
        supportedFunctions.add("RAND"); //$NON-NLS-1$
        supportedFunctions.add("ROUND"); //$NON-NLS-1$
        supportedFunctions.add("SIGN"); //$NON-NLS-1$
        supportedFunctions.add("SIN"); //$NON-NLS-1$
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        supportedFunctions.add("TAN"); //$NON-NLS-1$
        supportedFunctions.add("ASCII"); //$NON-NLS-1$
        supportedFunctions.add("CHAR"); //$NON-NLS-1$
        supportedFunctions.add("CHR"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT2"); //$NON-NLS-1$
        supportedFunctions.add("||"); //$NON-NLS-1$
        supportedFunctions.add("INITCAP"); //$NON-NLS-1$
        supportedFunctions.add("INSERT"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LOCATE"); //$NON-NLS-1$
        supportedFunctions.add("LPAD"); //$NON-NLS-1$
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        supportedFunctions.add("REPEAT"); //$NON-NLS-1$
        supportedFunctions.add("REPLACE"); //$NON-NLS-1$
        supportedFunctions.add("RPAD"); //$NON-NLS-1$
        supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
        supportedFunctions.add("TRANSLATE"); //$NON-NLS-1$
        supportedFunctions.add("UCASE"); //$NON-NLS-1$
        supportedFunctions.add("CURDATE"); //$NON-NLS-1$
        supportedFunctions.add("CURTIME"); //$NON-NLS-1$
        supportedFunctions.add("NOW"); //$NON-NLS-1$
        supportedFunctions.add("DAYNAME"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFMONTH"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFWEEK"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFYEAR"); //$NON-NLS-1$
        supportedFunctions.add("FORMATDATE"); //$NON-NLS-1$
        supportedFunctions.add("FORMATTIME"); //$NON-NLS-1$
        supportedFunctions.add("FORMATTIMESTAMP"); //$NON-NLS-1$
        supportedFunctions.add("HOUR"); //$NON-NLS-1$
        supportedFunctions.add("MINUTE"); //$NON-NLS-1$
        supportedFunctions.add("MONTH"); //$NON-NLS-1$
        supportedFunctions.add("MONTHNAME"); //$NON-NLS-1$
        supportedFunctions.add("PARSEDATE"); //$NON-NLS-1$
        supportedFunctions.add("PARSETIME"); //$NON-NLS-1$
        supportedFunctions.add("PARSETIMESTAMP"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPADD"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPDIFF"); //$NON-NLS-1$
        supportedFunctions.add("WEEK"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$
        supportedFunctions.add("MODIFYTIMEZONE"); //$NON-NLS-1$
        supportedFunctions.add("DECODESTRING"); //$NON-NLS-1$
        supportedFunctions.add("DECODEINTEGER"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("NVL");      //$NON-NLS-1$ 
        supportedFunctions.add("CAST"); //$NON-NLS-1$
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("USER"); //$NON-NLS-1$
        supportedFunctions.add("FROM_UNIXTIME"); //$NON-NLS-1$
        supportedFunctions.add("NULLIF"); //$NON-NLS-1$
        supportedFunctions.add("COALESCE"); //$NON-NLS-1$
        
        if (getVersion().compareTo(SEVEN_3) >= 0) {
        	supportedFunctions.add(SourceSystemFunctions.UNESCAPE);
        }
        
        if (getVersion().compareTo(SEVEN_4) >= 0) {
        	supportedFunctions.add(SourceSystemFunctions.UUID);
        	supportedFunctions.add(SourceSystemFunctions.ARRAY_GET);
        	supportedFunctions.add(SourceSystemFunctions.ARRAY_LENGTH);
        }
        
        if (getVersion().compareTo(SEVEN_5) >= 0) {
        	supportedFunctions.add(SourceSystemFunctions.TRIM);
        }
        
        if (getVersion().compareTo(EIGHT_3) >= 0) {
        	supportedFunctions.add(SourceSystemFunctions.ENDSWITH);
        }
        
        if (getVersion().compareTo(EIGHT_10) >= 0) {
        	supportedFunctions.add(SourceSystemFunctions.ST_ASBINARY);
        	supportedFunctions.add(SourceSystemFunctions.ST_ASGEOJSON);
        	supportedFunctions.add(SourceSystemFunctions.ST_ASTEXT);
        	supportedFunctions.add(SourceSystemFunctions.ST_CONTAINS);
        	supportedFunctions.add(SourceSystemFunctions.ST_CROSSES);
        	supportedFunctions.add(SourceSystemFunctions.ST_DISJOINT);
        	supportedFunctions.add(SourceSystemFunctions.ST_DISTANCE);
        	supportedFunctions.add(SourceSystemFunctions.ST_EQUALS);
        	supportedFunctions.add(SourceSystemFunctions.ST_GEOMFROMWKB);
        	supportedFunctions.add(SourceSystemFunctions.ST_GEOMFROMTEXT);
        	supportedFunctions.add(SourceSystemFunctions.ST_INTERSECTS);
        	supportedFunctions.add(SourceSystemFunctions.ST_OVERLAPS);
        	supportedFunctions.add(SourceSystemFunctions.ST_TOUCHES);
        	supportedFunctions.add(SourceSystemFunctions.ST_SRID);
        	supportedFunctions.add(SourceSystemFunctions.ST_SETSRID);
        }
        
        if (getVersion().compareTo(NINE_0) >= 0) {
        	supportedFunctions.add(SourceSystemFunctions.ST_TOUCHES);
        	supportedFunctions.add(SourceSystemFunctions.ST_HASARC);
            supportedFunctions.add(SourceSystemFunctions.ST_SIMPLIFY);
            supportedFunctions.add(SourceSystemFunctions.ST_FORCE_2D);
            supportedFunctions.add(SourceSystemFunctions.ST_ENVELOPE);
            supportedFunctions.add(SourceSystemFunctions.ST_WITHIN);
            supportedFunctions.add(SourceSystemFunctions.ST_DWITHIN);
            supportedFunctions.add(SourceSystemFunctions.ST_EXTENT);
            supportedFunctions.add(SourceSystemFunctions.DOUBLE_AMP_OP);
            supportedFunctions.add(SourceSystemFunctions.ST_GEOMFROMEWKT);
            supportedFunctions.add(SourceSystemFunctions.ST_GEOMFROMEWKB);
            supportedFunctions.add(SourceSystemFunctions.ST_ASEWKB);
            supportedFunctions.add(SourceSystemFunctions.ST_ASEWKT);
        }
        
        if (getVersion().compareTo(NINE_1) >= 0) {
            supportedFunctions.add(SourceSystemFunctions.ST_AREA);
            supportedFunctions.add(SourceSystemFunctions.ST_BOUNDARY);
            supportedFunctions.add(SourceSystemFunctions.ST_BUFFER);
            supportedFunctions.add(SourceSystemFunctions.ST_CENTROID);
            supportedFunctions.add(SourceSystemFunctions.ST_CONVEXHULL);
            supportedFunctions.add(SourceSystemFunctions.ST_COORDDIM);
            supportedFunctions.add(SourceSystemFunctions.ST_CURVETOLINE);
            supportedFunctions.add(SourceSystemFunctions.ST_DIFFERENCE);
            supportedFunctions.add(SourceSystemFunctions.ST_DIMENSION);
            supportedFunctions.add(SourceSystemFunctions.ST_ENDPOINT);
            supportedFunctions.add(SourceSystemFunctions.ST_EXTERIORRING);
            supportedFunctions.add(SourceSystemFunctions.ST_GEOMETRYN);
            supportedFunctions.add(SourceSystemFunctions.ST_GEOMETRYTYPE);
            supportedFunctions.add(SourceSystemFunctions.ST_INTERIORRINGN);
            supportedFunctions.add(SourceSystemFunctions.ST_INTERSECTION);
            supportedFunctions.add(SourceSystemFunctions.ST_ISCLOSED);
            supportedFunctions.add(SourceSystemFunctions.ST_ISEMPTY);
            supportedFunctions.add(SourceSystemFunctions.ST_ISRING);
            supportedFunctions.add(SourceSystemFunctions.ST_ISSIMPLE);
            supportedFunctions.add(SourceSystemFunctions.ST_ISVALID);
            supportedFunctions.add(SourceSystemFunctions.ST_LENGTH);
            supportedFunctions.add(SourceSystemFunctions.ST_NUMGEOMETRIES);
            supportedFunctions.add(SourceSystemFunctions.ST_NUMINTERIORRINGS);
            supportedFunctions.add(SourceSystemFunctions.ST_NUMPOINTS);
            supportedFunctions.add(SourceSystemFunctions.ST_ORDERINGEQUALS);
            supportedFunctions.add(SourceSystemFunctions.ST_PERIMETER);
            supportedFunctions.add(SourceSystemFunctions.ST_POINT);
            supportedFunctions.add(SourceSystemFunctions.ST_POINTN);
            supportedFunctions.add(SourceSystemFunctions.ST_POINTONSURFACE);
            supportedFunctions.add(SourceSystemFunctions.ST_POLYGON);
            supportedFunctions.add(SourceSystemFunctions.ST_RELATE);
            supportedFunctions.add(SourceSystemFunctions.ST_STARTPOINT);
            supportedFunctions.add(SourceSystemFunctions.ST_SYMDIFFERENCE);
            supportedFunctions.add(SourceSystemFunctions.ST_UNION);
            supportedFunctions.add(SourceSystemFunctions.ST_X);
            supportedFunctions.add(SourceSystemFunctions.ST_Y);
            supportedFunctions.add(SourceSystemFunctions.ST_Z);
        }
        
        if (getVersion().compareTo(NINE_2) >= 0) {
            supportedFunctions.add(SourceSystemFunctions.ST_MAKEENVELOPE);
            supportedFunctions.add(SourceSystemFunctions.MD5);
            supportedFunctions.add(SourceSystemFunctions.SHA1);
            supportedFunctions.add(SourceSystemFunctions.SHA2_256);
            supportedFunctions.add(SourceSystemFunctions.SHA2_512);
        }
        
        return supportedFunctions;
    }
    
    public boolean supportsInlineViews() {
        return true;
    }

    @Override
    public boolean supportsFunctionsInGroupBy() {
        return true;
    }    
    
    public boolean supportsRowLimit() {
        return true;
    }
    
    public boolean supportsRowOffset() {
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
    	return getVersion().compareTo(SEVEN_1) >= 0;
    }
    
    @Override
    public NullOrder getDefaultNullOrder() {
    	return NullOrder.UNKNOWN;
    }
    
    @Override
    public boolean supportsBulkUpdate() {
    	return true;
    }
    
    @Override
    public boolean supportsCommonTableExpressions() {
    	return getVersion().compareTo(SEVEN_2) >= 0;
    }
    
    @Override
    public boolean supportsRecursiveCommonTableExpressions() {
    	return getVersion().compareTo(EIGHT_10) >= 0;
    }
    
    @Override
    public boolean supportsAdvancedOlapOperations() {
    	return getVersion().compareTo(SEVEN_5) >= 0;
    }
    
    @Override
    public boolean supportsElementaryOlapOperations() {
    	return getVersion().compareTo(SEVEN_5) >= 0;
    }
    
    @Override
    public boolean supportsArrayAgg() {
    	return getVersion().compareTo(SEVEN_5) >= 0;
    }
    
    @Override
    public boolean supportsLikeRegex() {
    	return getVersion().compareTo(SEVEN_5) >= 0;
    }
    
    @Override
    public boolean supportsSimilarTo() {
    	return getVersion().compareTo(SEVEN_5) >= 0;
    }
    
    @Override
    public boolean supportsWindowDistinctAggregates() {
    	return getVersion().compareTo(SEVEN_6) >= 0;
    }
    
    @Override
    public boolean supportsWindowOrderByWithAggregates() {
    	return getVersion().compareTo(SEVEN_5) >= 0;
    }
    
    @Override
    public boolean supportsFormatLiteral(String literal,
    		org.teiid.translator.ExecutionFactory.Format format) {
    	return true;
    }
    
    @Override
    public boolean supportsGeneratedKeys() {
    	return getVersion().compareTo(EIGHT_3) >= 0;
    }
    
    @Override
    public boolean supportsInsertWithQueryExpression() {
    	return true;
    }
    
    @Override
    public boolean supportsOrderByNullOrdering() {
    	return true;
    }
    
	@Override
	protected boolean usesDatabaseVersion() {
		return true;
	}
	
    @Override
    public boolean supportsSelectWithoutFrom() {
    	return true;
    }
    
    @Override
    public boolean supportsStringAgg() {
    	return getVersion().compareTo(EIGHT_4) >= 0;
    }
    
    @Override
    public SQLDialect getDialect() {
    	if (dialect == null) {
    		//TODO: should pull in our own dialect
    		this.dialect = new SQLDialect() {
				
				@Override
				public String getTypeName(int code, long length, int precision, int scale) {
					return JDBCSQLTypeInfo.getJavaClassName(code);
				}
				
                public AbstractMultiTableBulkIdStrategyImpl getDefaultMultiTableBulkIdStrategy() {
                    return new LocalTemporaryTableBulkIdStrategy(
                            new IdTableSupportStandardImpl() {
                                @Override
                                public String getCreateIdTableCommand() {
                                    return "create local temporary table";
                                }
                                @Override
                                public String getDropIdTableCommand() {
                                    return "drop table";
                                }
                                @Override
                                public String getCreateIdTableStatementOptions() {
                                    return "";
                                }
                            }, AfterUseAction.DROP,
                            TempTableDdlTransactionHandling.NONE);
                }
			};
    	}
    	return super.getDialect();
    }
    
    @Override
    public boolean supportsGroupByRollup() {
    	return getVersion().compareTo(EIGHT_5) >= 0;
    }
    
    @Override
    public boolean useScientificNotation() {
    	return true;
    }
    
    @Override
    public boolean supportsOrderByUnrelated() {
    	//prior to 8.10 we did not support unrelated from the grouping columns
    	return getVersion().compareTo(EIGHT_10) >= 0;
    }
    
    @Override
    public MetadataProcessor<Connection> getMetadataProcessor() {
    	return new JDBCMetdataProcessor() {
    		@Override
            protected String getRuntimeType(int type, String typeName, int precision) {
            	if ("geometry".equalsIgnoreCase(typeName)) { //$NON-NLS-1$
                    return TypeFacility.RUNTIME_NAMES.GEOMETRY;
                }                
                return super.getRuntimeType(type, typeName, precision);                    
            }
            
            @Override
            protected void getGeometryMetadata(Column c, Connection conn,
            		String tableCatalog, String tableSchema, String tableName,
            		String columnName) {
            	PreparedStatement ps = null;
            	ResultSet rs = null;
            	try {
            		if (tableCatalog == null) {
            			tableCatalog = conn.getCatalog();
            		}
	            	ps = conn.prepareStatement("select coord_dimension, srid, type from sys.geometry_columns where f_table_catalog=? and f_table_schema=? and f_table_name=? and f_geometry_column=?"); //$NON-NLS-1$
	            	ps.setString(1, tableCatalog);
	            	ps.setString(2, tableSchema);
	            	ps.setString(3, tableName);
	            	ps.setString(4, columnName);
	            	rs = ps.executeQuery();
	            	if (rs.next()) {
	            		c.setProperty(MetadataFactory.SPATIAL_URI + "coord_dimension", rs.getString(1)); //$NON-NLS-1$
	            		c.setProperty(MetadataFactory.SPATIAL_URI + "srid", rs.getString(2)); //$NON-NLS-1$
	            		c.setProperty(MetadataFactory.SPATIAL_URI + "type", rs.getString(3)); //$NON-NLS-1$
	            	}
            	} catch (SQLException e) {
            		LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Could not get geometry metadata for column", tableSchema, tableName, columnName); //$NON-NLS-1$
            	} finally {
            		if (rs != null) {
            			try {
							rs.close();
						} catch (SQLException e) {
						}
            		}
            		if (ps != null) {
            			try {
							ps.close();
						} catch (SQLException e) {
						}
            		}
            	}
            }
    	};
    }
    
    @Override
    public boolean supportsLateralJoin() {
    	return true;
    }
    
    @Override
    public String getLateralKeyword() {
        if (getVersion().compareTo(EIGHT_1) < 0) {
        	return SQLConstants.Reserved.TABLE;
        }
    	return super.getLateralKeyword();
    }
    
    @Override
    public boolean supportsProcedureTable() {
    	return true;
    }
    
    @Override
    public boolean supportsArrayType() {
        return getVersion().compareTo(EIGHT_1) > 0;
    }
    
    @Override
    public boolean supportsUpsert() {
        return getVersion().compareTo(EIGHT_3) > 0;
    }
    
    @Override
    public SQLConversionVisitor getSQLConversionVisitor() {
        return new SQLConversionVisitor(this) {
            @Override
            protected String getUpsertKeyword() {
                if (getVersion().compareTo(NINE_1) >= 0) {
                    return SQLConstants.NonReserved.UPSERT;
                }
                return SQLConstants.Reserved.MERGE;
            }  
        };
    }
    
}
