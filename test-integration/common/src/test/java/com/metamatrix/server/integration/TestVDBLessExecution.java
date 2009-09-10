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

package com.metamatrix.server.integration;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.junit.Test;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.jdbc.api.AbstractMMQueryTestCase;

/**
 * Test the DatabaseMetadata results using the Parts VDB.
 */
public class TestVDBLessExecution extends AbstractMMQueryTestCase {

    private static final String DQP_PROP_FILE = UnitTestUtil.getTestDataPath() + "/vdbless/dqp.properties;user=test"; //$NON-NLS-1$
    private static final String VDB = "VDBLess"; //$NON-NLS-1$

    @Test public void testExecution() {
    	getConnection(VDB, DQP_PROP_FILE);
    	executeAndAssertResults("select * from Example", new String[] { //$NON-NLS-1$
    			"TRADEID[string]    NOTIONAL[integer]", //$NON-NLS-1$
    			                            "x    1",  //$NON-NLS-1$
    			                            "y    2"   //$NON-NLS-1$

    	});
    	closeConnection();
    }    
    
    @Test public void testIntegrationExecution() {
    	getConnection(VDB, DQP_PROP_FILE);
    	executeAndAssertResults("select * from Example, Smalla where notional = intkey", new String[] { //$NON-NLS-1$
    			"TRADEID[string]    NOTIONAL[integer]    INTKEY[integer]    STRINGKEY[string]    INTNUM[integer]    STRINGNUM[string]    FLOATNUM[float]    LONGNUM[long]    DOUBLENUM[double]    BYTENUM[short]    DATEVALUE[date]    TIMEVALUE[time]    TIMESTAMPVALUE[timestamp]    BOOLEANVALUE[short]    CHARVALUE[string]    SHORTVALUE[short]    BIGINTEGERVALUE[long]    BIGDECIMALVALUE[bigdecimal]    OBJECTVALUE[string]", //$NON-NLS-1$
                "x    1    1    1    -23    null    -23.0    -23    -23.0    -127    2000-01-02    01:00:00    2000-01-01 00:00:01.0    1    0    -32767    -23    -23    -23", //$NON-NLS-1$
                "y    2    2    2    -22    -22    null    -22    -22.0    -126    2000-01-03    02:00:00    2000-01-01 00:00:02.0    0    1    -32766    -22    -22    -22", //$NON-NLS-1$
    	});
    	closeConnection();
    }
    
    /**
     * We have no results to assert here since derby does not provide procedure resultset columns in their metadata.
     */
    @Test public void testProcedureExecution() {
    	getConnection(VDB, DQP_PROP_FILE);
    	execute("exec Derby.SQLUDTS(null, null, null, null, null)"); //$NON-NLS-1$
    	closeConnection();
    }
    
    @Test public void testDatabaseMetaDataTables() throws Exception {
    	Connection conn = getConnection(VDB, DQP_PROP_FILE);
    	DatabaseMetaData metadata = conn.getMetaData();
    	this.internalResultSet = metadata.getTables(null, null, "SummitData%", new String[] {"TABLE"}); //$NON-NLS-1$ //$NON-NLS-2$
    	assertResults(new String[] {
    			"TABLE_CAT[string]    TABLE_SCHEM[string]    TABLE_NAME[string]    TABLE_TYPE[string]    REMARKS[string]    TYPE_CAT[string]    TYPE_SCHEM[string]    TYPE_NAME[string]    SELF_REFERENCING_COL_NAME[string]    REF_GENERATION[string]    ISPHYSICAL[boolean]", //$NON-NLS-1$
    			"null    VDBLess    SummitData.EXAMPLE    TABLE    null    null    null    null    null    null    true" //$NON-NLS-1$
    	});
    	closeConnection();
    }
    
    /**
     * Ensures that system tables are still visible
     */
    @Test public void testDatabaseMetaDataTables1() throws Exception {
    	Connection conn = getConnection(VDB, DQP_PROP_FILE);
    	DatabaseMetaData metadata = conn.getMetaData();
    	this.internalResultSet = metadata.getTables(null, null, "%ElementProperties", null); //$NON-NLS-1$
    	assertResults(new String[] {
    			"TABLE_CAT[string]    TABLE_SCHEM[string]    TABLE_NAME[string]    TABLE_TYPE[string]    REMARKS[string]    TYPE_CAT[string]    TYPE_SCHEM[string]    TYPE_NAME[string]    SELF_REFERENCING_COL_NAME[string]    REF_GENERATION[string]    ISPHYSICAL[boolean]", //$NON-NLS-1$
    			"null    VDBLess    System.DataTypeElementProperties    SYSTEM TABLE    null    null    null    null    null    null    false", //$NON-NLS-1$
    			"null    VDBLess    System.ElementProperties    SYSTEM TABLE    null    null    null    null    null    null    false" //$NON-NLS-1$

    	});
    	closeConnection();
    }
    
    @Test public void testDatabaseMetaDataColumns() throws Exception {
    	Connection conn = getConnection(VDB, DQP_PROP_FILE);
    	DatabaseMetaData metadata = conn.getMetaData();
    	this.internalResultSet = metadata.getColumns(null, null, "SummitData%", "%"); //$NON-NLS-1$ //$NON-NLS-2$
    	assertResults(new String[] {
    			"TABLE_CAT[string]    TABLE_SCHEM[string]    TABLE_NAME[string]    COLUMN_NAME[string]    DATA_TYPE[short]    TYPE_NAME[string]    COLUMN_SIZE[integer]    BUFFER_LENGTH[string]    DECIMAL_DIGITS[integer]    NUM_PREC_RADIX[integer]    NULLABLE[integer]    REMARKS[string]    COLUMN_DEF[string]    SQL_DATA_TYPE[string]    SQL_DATETIME_SUB[string]    CHAR_OCTET_LENGTH[integer]    ORDINAL_POSITION[integer]    IS_NULLABLE[string]    SCOPE_CATALOG[string]    SCOPE_SCHEMA[string]    SCOPE_TABLE[string]    SOURCE_DATA_TYPE[string]    IS_AUTOINCREMENT[string]", //$NON-NLS-1$
    			"null    VDBLess    SummitData.EXAMPLE    TRADEID    12    string    4000    null    0    0    0    null    null    null    null    0    1    YES    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    SummitData.EXAMPLE    NOTIONAL    4    integer    10    null    0    0    0    null    null    null    null    0    2    YES    null    null    null    null    NO", //$NON-NLS-1$
    	});
    	closeConnection();
    }
    
    @Test public void testDatabaseMetaDataColumns1() throws Exception {
    	Connection conn = getConnection(VDB, DQP_PROP_FILE);
    	DatabaseMetaData metadata = conn.getMetaData();
    	this.internalResultSet = metadata.getColumns(null, null, "%smalla", "%"); //$NON-NLS-1$ //$NON-NLS-2$
    	assertResults(new String[] {
    			"TABLE_CAT[string]    TABLE_SCHEM[string]    TABLE_NAME[string]    COLUMN_NAME[string]    DATA_TYPE[short]    TYPE_NAME[string]    COLUMN_SIZE[integer]    BUFFER_LENGTH[string]    DECIMAL_DIGITS[integer]    NUM_PREC_RADIX[integer]    NULLABLE[integer]    REMARKS[string]    COLUMN_DEF[string]    SQL_DATA_TYPE[string]    SQL_DATETIME_SUB[string]    CHAR_OCTET_LENGTH[integer]    ORDINAL_POSITION[integer]    IS_NULLABLE[string]    SCOPE_CATALOG[string]    SCOPE_SCHEMA[string]    SCOPE_TABLE[string]    SOURCE_DATA_TYPE[string]    IS_AUTOINCREMENT[string]", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    INTKEY    4    integer    10    null    0    10    0    null    null    null    null    0    1    YES    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    STRINGKEY    12    string    4000    null    0    0    0    null    null    null    null    20    2    YES    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    INTNUM    4    integer    10    null    0    10    1    null    null    null    null    0    3    NO    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    STRINGNUM    12    string    4000    null    0    0    1    null    null    null    null    20    4    NO    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    FLOATNUM    7    float    20    null    0    2    1    null    null    null    null    0    5    NO    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    LONGNUM    -5    long    19    null    0    10    1    null    null    null    null    0    6    NO    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    DOUBLENUM    8    double    20    null    0    2    1    null    null    null    null    0    7    NO    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    BYTENUM    5    short    5    null    0    10    1    null    null    null    null    0    8    NO    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    DATEVALUE    91    date    10    null    0    10    1    null    null    null    null    0    9    NO    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    TIMEVALUE    92    time    8    null    0    10    1    null    null    null    null    0    10    NO    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    TIMESTAMPVALUE    93    timestamp    29    null    0    10    1    null    null    null    null    0    11    NO    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    BOOLEANVALUE    5    short    5    null    0    10    1    null    null    null    null    0    12    NO    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    CHARVALUE    12    string    4000    null    0    0    1    null    null    null    null    2    13    NO    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    SHORTVALUE    5    short    5    null    0    10    1    null    null    null    null    0    14    NO    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    BIGINTEGERVALUE    -5    long    19    null    0    10    1    null    null    null    null    0    15    NO    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    BIGDECIMALVALUE    2    bigdecimal    20    null    0    10    1    null    null    null    null    0    16    NO    null    null    null    null    NO", //$NON-NLS-1$
    			"null    VDBLess    Derby.SMALLA    OBJECTVALUE    12    string    4000    null    0    0    1    null    null    null    null    4096    17    NO    null    null    null    null    NO", //$NON-NLS-1$
    	});
    	closeConnection();
    }
    
    @Test public void testDatabaseMetaDataPrimaryKeys() throws Exception {
    	Connection conn = getConnection(VDB, DQP_PROP_FILE);
    	DatabaseMetaData metadata = conn.getMetaData();
    	//note - the use of null for the table name is a little against spec
    	this.internalResultSet = metadata.getPrimaryKeys(null, null, null);
    	assertResults(new String[] {
    	       "TABLE_CAT[string]    TABLE_SCHEM[string]    TABLE_NAME[string]    COLUMN_NAME[string]    KEY_SEQ[short]    PK_NAME[string]", //$NON-NLS-1$
    	       "null    VDBLess    Derby.FLIGHTS    FLIGHT_ID    1    SQL090709161814150", //$NON-NLS-1$
    	       "null    VDBLess    Derby.FLTAVAIL    FLIGHT_ID    1    FLTAVAIL_PK", //$NON-NLS-1$
    		   "null    VDBLess    Derby.SMALLA    INTKEY    1    SQL060110103634070", //$NON-NLS-1$
			   "null    VDBLess    Derby.SMALLB    INTKEY    1    SQL060110103635170", //$NON-NLS-1$
			   "null    VDBLess    Derby.FLIGHTS    SEGMENT_NUMBER    2    SQL090709161814150", //$NON-NLS-1$
			   "null    VDBLess    Derby.FLTAVAIL    SEGMENT_NUMBER    2    FLTAVAIL_PK", //$NON-NLS-1$
    	});
    	closeConnection();
    }
    
    @Test public void testDatabaseMetaDataExportedKeys() throws Exception {
    	Connection conn = getConnection(VDB, DQP_PROP_FILE);
    	DatabaseMetaData metadata = conn.getMetaData();
    	this.internalResultSet = metadata.getExportedKeys(null, "VDBLess", "Derby.FLIGHTS"); //$NON-NLS-1$ //$NON-NLS-2$
    	assertResults(new String[] {
    			"PKTABLE_CAT[string]    PKTABLE_SCHEM[string]    PKTABLE_NAME[string]    PKCOLUMN_NAME[string]    FKTABLE_CAT[string]    FKTABLE_SCHEM[string]    FKTABLE_NAME[string]    FKCOLUMN_NAME[string]    KEY_SEQ[short]    UPDATE_RULE[integer]    DELETE_RULE[integer]    FK_NAME[string]    PK_NAME[string]    DEFERRABILITY[integer]", //$NON-NLS-1$
                "null    VDBLess    Derby.FLIGHTS    FLIGHT_ID    null    VDBLess    Derby.FLTAVAIL    FLIGHT_ID    1    3    3    FLTS_FK    SQL090709161814150    5", //$NON-NLS-1$
    			"null    VDBLess    Derby.FLIGHTS    SEGMENT_NUMBER    null    VDBLess    Derby.FLTAVAIL    SEGMENT_NUMBER    2    3    3    FLTS_FK    SQL090709161814150    5" //$NON-NLS-1$
    	});
    	closeConnection();
    }
    
    @Test public void testDatabaseMetaDataImportedKeys() throws Exception {
    	Connection conn = getConnection(VDB, DQP_PROP_FILE);
    	DatabaseMetaData metadata = conn.getMetaData();
    	this.internalResultSet = metadata.getImportedKeys(null, "VDBLess", "Derby.FLTAVAIL"); //$NON-NLS-1$ //$NON-NLS-2$
    	assertResults(new String[] {
    			"PKTABLE_CAT[string]    PKTABLE_SCHEM[string]    PKTABLE_NAME[string]    PKCOLUMN_NAME[string]    FKTABLE_CAT[string]    FKTABLE_SCHEM[string]    FKTABLE_NAME[string]    FKCOLUMN_NAME[string]    KEY_SEQ[short]    UPDATE_RULE[integer]    DELETE_RULE[integer]    FK_NAME[string]    PK_NAME[string]    DEFERRABILITY[integer]", //$NON-NLS-1$
    			"null    VDBLess    Derby.FLIGHTS    FLIGHT_ID    null    VDBLess    Derby.FLTAVAIL    FLIGHT_ID    1    3    3    FLTS_FK    SQL090709161814150    5", //$NON-NLS-1$
    			"null    VDBLess    Derby.FLIGHTS    SEGMENT_NUMBER    null    VDBLess    Derby.FLTAVAIL    SEGMENT_NUMBER    2    3    3    FLTS_FK    SQL090709161814150    5" //$NON-NLS-1$
    	});
    	this.internalResultSet = metadata.getImportedKeys(null, null, "Derby.SMALLBRIDGE"); //$NON-NLS-1$
    	assertResults(new String[] {
    			"PKTABLE_CAT[string]    PKTABLE_SCHEM[string]    PKTABLE_NAME[string]    PKCOLUMN_NAME[string]    FKTABLE_CAT[string]    FKTABLE_SCHEM[string]    FKTABLE_NAME[string]    FKCOLUMN_NAME[string]    KEY_SEQ[short]    UPDATE_RULE[integer]    DELETE_RULE[integer]    FK_NAME[string]    PK_NAME[string]    DEFERRABILITY[integer]", //$NON-NLS-1$
                "null    VDBLess    Derby.SMALLA    INTKEY    null    VDBLess    Derby.SMALLBRIDGE    AKEY    1    3    3    SMLA_FK    SQL060110103634070    5", //$NON-NLS-1$
                "null    VDBLess    Derby.SMALLB    INTKEY    null    VDBLess    Derby.SMALLBRIDGE    BKEY    1    3    3    SMLB_FK    SQL060110103635170    5", //$NON-NLS-1$
    	});
    	closeConnection();
    }
    
    @Test public void testDatabaseMetaDataIndexInfo() throws Exception {
    	Connection conn = getConnection(VDB, DQP_PROP_FILE);
    	DatabaseMetaData metadata = conn.getMetaData();
    	//note - the use of null for the table name is a little against spec
    	this.internalResultSet = metadata.getIndexInfo(null, null, null, false, true);
    	assertResults(new String[] {
    			"TABLE_CAT[string]    TABLE_SCHEM[string]    TABLE_NAME[string]    NON_UNIQUE[boolean]    INDEX_QUALIFIER[string]    INDEX_NAME[string]    TYPE[integer]    ORDINAL_POSITION[short]    COLUMN_NAME[string]    ASC_OR_DESC[string]    CARDINALITY[integer]    PAGES[integer]    FILTER_CONDITION[string]", //$NON-NLS-1$
				"null    VDBLess    Derby.FLIGHTS    false    null    ORIGINDEX    0    1    ORIG_AIRPORT    null    0    1    null", //$NON-NLS-1$
				"null    VDBLess    Derby.FLTAVAIL    false    null    SQL090709161840271    0    1    FLIGHT_ID    null    0    1    null", //$NON-NLS-1$
				"null    VDBLess    Derby.FLTAVAIL    false    null    SQL090709161840271    0    2    SEGMENT_NUMBER    null    0    1    null", //$NON-NLS-1$
				"null    VDBLess    Derby.SMALLBRIDGE    false    null    SQL090710102514590    0    1    AKEY    null    0    1    null", //$NON-NLS-1$
				"null    VDBLess    Derby.SMALLBRIDGE    false    null    SQL090710102514591    0    1    BKEY    null    0    1    null", //$NON-NLS-1$
				"null    VDBLess    Derby.SYSCOLUMNS    false    null    SYSCOLUMNS_INDEX2    0    1    COLUMNDEFAULTID    null    0    1    null", //$NON-NLS-1$
				"null    VDBLess    Derby.SYSCONGLOMERATES    false    null    SYSCONGLOMERATES_INDEX1    0    1    CONGLOMERATEID    null    0    1    null", //$NON-NLS-1$
				"null    VDBLess    Derby.SYSCONGLOMERATES    false    null    SYSCONGLOMERATES_INDEX3    0    1    TABLEID    null    0    1    null", //$NON-NLS-1$
				"null    VDBLess    Derby.SYSCONSTRAINTS    false    null    SYSCONSTRAINTS_INDEX3    0    1    TABLEID    null    0    1    null", //$NON-NLS-1$
				"null    VDBLess    Derby.SYSDEPENDS    false    null    SYSDEPENDS_INDEX1    0    1    DEPENDENTID    null    0    1    null", //$NON-NLS-1$
				"null    VDBLess    Derby.SYSDEPENDS    false    null    SYSDEPENDS_INDEX2    0    1    PROVIDERID    null    0    1    null", //$NON-NLS-1$
				"null    VDBLess    Derby.SYSFOREIGNKEYS    false    null    SYSFOREIGNKEYS_INDEX2    0    1    KEYCONSTRAINTID    null    0    1    null", //$NON-NLS-1$
				"null    VDBLess    Derby.SYSSTATISTICS    false    null    SYSSTATISTICS_INDEX1    0    1    TABLEID    null    0    1    null", //$NON-NLS-1$
				"null    VDBLess    Derby.SYSSTATISTICS    false    null    SYSSTATISTICS_INDEX1    0    2    REFERENCEID    null    0    1    null", //$NON-NLS-1$
				"null    VDBLess    Derby.SYSTRIGGERS    false    null    SYSTRIGGERS_INDEX3    0    1    TABLEID    null    0    1    null", //$NON-NLS-1$
				"null    VDBLess    Derby.SYSTRIGGERS    false    null    SYSTRIGGERS_INDEX3    0    2    CREATIONTIMESTAMP    null    0    1    null", //$NON-NLS-1$
    	});
    	closeConnection();
    }
    
    @Test public void testDatabaseMetaDataProcedures() throws Exception {
    	Connection conn = getConnection(VDB, DQP_PROP_FILE);
    	DatabaseMetaData metadata = conn.getMetaData();
    	this.internalResultSet = metadata.getProcedures(null, null, "Derby.%JAR"); //$NON-NLS-1$
    	assertResults(new String[] {
    			"PROCEDURE_CAT[string]    PROCEDURE_SCHEM[string]    PROCEDURE_NAME[string]    RESERVED_1[string]    RESERVED_2[string]    RESERVED_3[string]    REMARKS[string]    PROCEDURE_TYPE[short]    SPECIFIC_NAME[string]", //$NON-NLS-1$
    			"null    VDBLess    Derby.INSTALL_JAR    null    null    null    null    1    Derby.INSTALL_JAR", //$NON-NLS-1$
    			"null    VDBLess    Derby.REMOVE_JAR    null    null    null    null    1    Derby.REMOVE_JAR", //$NON-NLS-1$
    			"null    VDBLess    Derby.REPLACE_JAR    null    null    null    null    1    Derby.REPLACE_JAR", //$NON-NLS-1$
    	});
    	closeConnection();
    }
    
    @Test public void testDatabaseMetaDataProcedureColumns() throws Exception {
    	Connection conn = getConnection(VDB, DQP_PROP_FILE);
    	DatabaseMetaData metadata = conn.getMetaData();
    	this.internalResultSet = metadata.getProcedureColumns(null, null, "Derby.SQLUDTS", null); //$NON-NLS-1$
    	assertResults(new String[] {
    			"PROCEDURE_CAT[string]    PROCEDURE_SCHEM[string]    PROCEDURE_NAME[string]    COLUMN_NAME[string]    COLUMN_TYPE[short]    DATA_TYPE[integer]    TYPE_NAME[string]    PRECISION[integer]    LENGTH[integer]    SCALE[short]    RADIX[integer]    NULLABLE[integer]    REMARKS[string]    COLUMN_DEF[string]    SQL_DATA_TYPE[string]    SQL_DATETIME_SUB[string]    CHAR_OCTET_LENGTH[string]    ORDINAL_POSITION[integer]    IS_NULLABLE[string]    SPECIFIC_NAME[string]", //$NON-NLS-1$
    			"null    VDBLess    Derby.SQLUDTS    CATALOGNAME    1    12    string    128    256    0    0    1    null    null    null    null    null    1    YES    Derby.SQLUDTS.CATALOGNAME", //$NON-NLS-1$
    			"null    VDBLess    Derby.SQLUDTS    SCHEMAPATTERN    1    12    string    128    256    0    0    1    null    null    null    null    null    2    YES    Derby.SQLUDTS.SCHEMAPATTERN", //$NON-NLS-1$
    			"null    VDBLess    Derby.SQLUDTS    TYPENAMEPATTERN    1    12    string    128    256    0    0    1    null    null    null    null    null    3    YES    Derby.SQLUDTS.TYPENAMEPATTERN", //$NON-NLS-1$
    			"null    VDBLess    Derby.SQLUDTS    UDTTYPES    1    12    string    128    256    0    0    1    null    null    null    null    null    4    YES    Derby.SQLUDTS.UDTTYPES", //$NON-NLS-1$
    			"null    VDBLess    Derby.SQLUDTS    OPTIONS    1    12    string    4000    8000    0    0    1    null    null    null    null    null    5    YES    Derby.SQLUDTS.OPTIONS", //$NON-NLS-1$
    	});
    	closeConnection();
    }
        
}
