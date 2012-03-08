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

package org.teiid.translator.jdbc.oracle;

import static org.junit.Assert.*;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.CommandBuilder;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.datamgr.ExecutionContextImpl;
import org.teiid.dqp.internal.datamgr.FakeExecutionContextImpl;
import org.teiid.language.Command;
import org.teiid.language.Comparison;
import org.teiid.language.Literal;
import org.teiid.language.Select;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.JDBCProcedureExecution;
import org.teiid.translator.jdbc.JDBCQueryExecution;
import org.teiid.translator.jdbc.SQLConversionVisitor;
import org.teiid.translator.jdbc.TranslatedCommand;
import org.teiid.translator.jdbc.TranslationHelper;

@SuppressWarnings("nls")
public class TestOracleTranslator {
	
    private OracleExecutionFactory TRANSLATOR;
    private String UDF = "/OracleSpatialFunctions.xmi"; //$NON-NLS-1$;
    private static ExecutionContext EMPTY_CONTEXT = new FakeExecutionContextImpl();

    @Before 
    public void setup() throws Exception {
        TRANSLATOR = new OracleExecutionFactory();  
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.start();
    }

	private void helpTestVisitor(String input, String expectedOutput) throws TranslatorException {
		helpTestVisitor(getOracleSpecificMetadata(), input, EMPTY_CONTEXT, null, expectedOutput);
    }
	
	@Test public void testSourceHint() throws Exception {
		ExecutionContextImpl impl = new FakeExecutionContextImpl();
		impl.setHint("hello world");
		helpTestVisitor(getTestVDB(), "select part_name from parts", impl, null, "SELECT /*+ hello world */ g_0.PART_NAME FROM PARTS g_0", true);
	}
	
	@Test public void testSourceHint1() throws Exception {
		ExecutionContextImpl impl = new FakeExecutionContextImpl();
		impl.setHint("hello world");
		helpTestVisitor(getTestVDB(), "select part_name from parts union select part_id from parts", impl, null, "SELECT /*+ hello world */ g_1.PART_NAME AS c_0 FROM PARTS g_1 UNION SELECT g_0.PART_ID AS c_0 FROM PARTS g_0", true);
	}
	
	@Test public void testInsertWithSequnce() throws Exception {
		helpTestVisitor("insert into smalla (doublenum) values (1)", "INSERT INTO SmallishA (DoubleNum, ID) VALUES (1.0, MYSEQUENCE.nextVal)"); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	@Test public void testInsertWithSequnce1() throws Exception {
		helpTestVisitor("insert into smalla (doublenum, id) values (1, 1)", "INSERT INTO SmallishA (DoubleNum, ID) VALUES (1.0, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	@Test public void testJoins() throws Exception {
        String input = "select smalla.intkey from bqt1.smalla inner join bqt1.smallb on smalla.stringkey=smallb.stringkey cross join bqt1.mediuma"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA INNER JOIN SmallB ON SmallA.StringKey = SmallB.StringKey CROSS JOIN MediumA"; //$NON-NLS-1$
          
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
            input, 
            output, TRANSLATOR);        
    }
    
	@Test public void testJoins2() throws Exception {
        String input = "select smalla.intkey from bqt1.smalla cross join (bqt1.smallb cross join bqt1.mediuma)"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA CROSS JOIN (SmallB CROSS JOIN MediumA)"; //$NON-NLS-1$
      
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
            input, 
            output, TRANSLATOR);        
    }
    
	@Test public void testConversion1() throws Exception {
        String input = "SELECT char(convert(STRINGNUM, integer) + 100) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT chr((trunc(to_number(SmallA.StringNum)) + 100)) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
            input, output, 
            TRANSLATOR);
    }
          
    @Test public void testConversion2() throws Exception {
        String input = "SELECT convert(STRINGNUM, long) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT trunc(to_number(SmallA.StringNum)) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
          
    @Test public void testConversion3() throws Exception {
        String input = "SELECT convert(convert(STRINGNUM, long), string) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT to_char(trunc(to_number(SmallA.StringNum))) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
          
    @Test public void testConversion4() throws Exception {
        String input = "SELECT convert(convert(TIMESTAMPVALUE, date), string) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT to_char(trunc(cast(SmallA.TimestampValue AS date)), 'YYYY-MM-DD') FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testConversion6() throws Exception {
        String input = "SELECT convert(convert(TIMEVALUE, timestamp), string) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT to_char(cast(SmallA.TimeValue AS timestamp), 'YYYY-MM-DD HH24:MI:SS.FF') FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    
    /**
     * here we use the date form of the conversion
     */
    @Test public void testConversion6a() throws Exception {
        String input = "SELECT convert(timestampvalue, string) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT to_char(SmallishA.timestampvalue, 'YYYY-MM-DD HH24:MI:SS') FROM SmallishA";  //$NON-NLS-1$

        helpTestVisitor(getOracleSpecificMetadata(), input, EMPTY_CONTEXT, null, output);
    }
    
    @Test public void testConversion8() throws Exception {
        String input = "SELECT nvl(INTNUM, 'otherString') FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT nvl(to_char(SmallA.IntNum), 'otherString') FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testConversion7() throws Exception {
        String input = "SELECT convert(convert(STRINGNUM, integer), string) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT to_char(trunc(to_number(SmallA.StringNum))) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(INTNUM, 'chimp', 1) FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate() throws Exception {
        String input = "SELECT locate(INTNUM, 'chimp', 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT INSTR('chimp', to_char(SmallA.IntNum), 1) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(STRINGNUM, 'chimp') FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate2() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp') FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT INSTR('chimp', SmallA.StringNum) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(INTNUM, '234567890', 1) FROM BQT1.SMALLA WHERE INTKEY = 26</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate3() throws Exception {
        String input = "SELECT locate(INTNUM, '234567890', 1) FROM BQT1.SMALLA WHERE INTKEY = 26"; //$NON-NLS-1$
        String output = "SELECT INSTR('234567890', to_char(SmallA.IntNum), 1) FROM SmallA WHERE SmallA.IntKey = 26";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate('c', 'chimp', 1) FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate4() throws Exception {
        String input = "SELECT locate('c', 'chimp', 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT 1 FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(STRINGNUM, 'chimp', -5) FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate5() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp', -5) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT INSTR('chimp', SmallA.StringNum, 1) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(STRINGNUM, 'chimp', INTNUM) FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate6() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp', INTNUM) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT INSTR('chimp', SmallA.StringNum, CASE WHEN SmallA.IntNum < 1 THEN 1 ELSE SmallA.IntNum END) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(STRINGNUM, 'chimp', LOCATE(STRINGNUM, 'chimp') + 1) FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate7() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp', LOCATE(STRINGNUM, 'chimp') + 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT INSTR('chimp', SmallA.StringNum, CASE WHEN (INSTR('chimp', SmallA.StringNum) + 1) < 1 THEN 1 ELSE (INSTR('chimp', SmallA.StringNum) + 1) END) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }

    @Test public void testSubstring1() throws Exception {
        String input = "SELECT substring(StringNum, 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT substr(SmallA.StringNum, 1) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testSubstring2() throws Exception {
        String input = "SELECT substring(StringNum, 1, 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT substr(SmallA.StringNum, 1, 1) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testUnionWithOrderBy() throws Exception {
        String input = "SELECT IntKey FROM BQT1.SMALLA UNION SELECT IntKey FROM BQT1.SMALLB ORDER BY IntKey"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA UNION SELECT SmallB.IntKey FROM SmallB ORDER BY IntKey";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRowLimit1() throws Exception {
        String input = "select intkey from bqt1.smalla limit 10, 0"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT VIEW_FOR_LIMIT.*, ROWNUM ROWNUM_ FROM (SELECT SmallA.IntKey FROM SmallA) VIEW_FOR_LIMIT WHERE ROWNUM <= 10) WHERE ROWNUM_ > 10"; //$NON-NLS-1$
               
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRowLimit2() throws Exception {
        String input = "select intkey from bqt1.smalla limit 0, 10"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT SmallA.IntKey FROM SmallA) WHERE ROWNUM <= 10"; //$NON-NLS-1$
               
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRowLimit3() throws Exception {
        String input = "select intkey from bqt1.smalla limit 1, 10"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT VIEW_FOR_LIMIT.*, ROWNUM ROWNUM_ FROM (SELECT SmallA.IntKey FROM SmallA) VIEW_FOR_LIMIT WHERE ROWNUM <= 11) WHERE ROWNUM_ > 1"; //$NON-NLS-1$
               
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRowLimit4() throws Exception {
        String input = "select intkey from bqt1.mediuma limit 100"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT MediumA.IntKey FROM MediumA) WHERE ROWNUM <= 100"; //$NON-NLS-1$
               
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRowLimit5() throws Exception {
        String input = "select intkey from bqt1.mediuma limit 50, 100"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT VIEW_FOR_LIMIT.*, ROWNUM ROWNUM_ FROM (SELECT MediumA.IntKey FROM MediumA) VIEW_FOR_LIMIT WHERE ROWNUM <= 150) WHERE ROWNUM_ > 50"; //$NON-NLS-1$
               
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testConcat2_useLiteral() throws Exception {        
        String input = "select concat2(stringnum,'_xx') from bqt1.Smalla"; //$NON-NLS-1$
        String output = "SELECT concat(nvl(SmallA.StringNum, ''), '_xx') FROM SmallA"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }

    @Test public void testConcat2() throws Exception {        
        String input = "select concat2(stringnum, stringkey) from bqt1.Smalla"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN SmallA.StringNum IS NULL AND SmallA.StringKey IS NULL THEN NULL ELSE concat(nvl(SmallA.StringNum, ''), nvl(SmallA.StringKey, '')) END FROM SmallA"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    
    /**
     * Test a query which uses 
     * <code>sdo_relate(Object element, Object element, String literal) in its 
     * criteria into a source specific command.
     * 
     * @throws Exception
     */
    @Test public void test_sdo_relate() throws Exception {
        String input = "SELECT a.INTKEY FROM BQT1.SMALLA A, BQT1.SMALLB B WHERE sdo_relate(A.OBJECTVALUE, b.OBJECTVALUE, 'mask=ANYINTERACT') = true"; //$NON-NLS-1$
        String output = "SELECT /*+ ORDERED */ A.IntKey FROM SmallA A, SmallB B WHERE sdo_relate(A.ObjectValue, B.ObjectValue, 'mask=ANYINTERACT') = 'true'";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test a query which uses 
     * <code>sdo_within_distance(Object element, String literal, String literal) 
     * in its criteria into a source specific command.
     * 
     * @throws Exception
     */
    @Test public void test_sdo_within_distance() throws Exception {
        String input = "SELECT INTKEY FROM BQT1.SMALLA WHERE sdo_within_distance(OBJECTVALUE, 'SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL)', 'DISTANCE=25.0 UNIT=NAUT_MILE') = true"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE sdo_within_distance(SmallA.ObjectValue, SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL), 'DISTANCE=25.0 UNIT=NAUT_MILE') = 'true'";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void test_sdo_within_distance_pushdownfunction() throws Exception {
        String input = "SELECT INTKEY FROM BQT1.SMALLA WHERE sdo_within_distance(OBJECTVALUE, 'SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL)', 'DISTANCE=25.0 UNIT=NAUT_MILE') = true"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE sdo_within_distance(SmallA.ObjectValue, SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL), 'DISTANCE=25.0 UNIT=NAUT_MILE') = 'true'";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, 
                TRANSLATOR);
    }

    /**
     * Test a query which uses 
     * <code>sdo_within_distance(String literal, Object element, String literal) 
     * in its criteria into a source specific command.
     * 
     * @throws Exception
     */
    @Test public void test_sdo_within_distance2() throws Exception {
        String input = "SELECT INTKEY FROM BQT1.SMALLA WHERE sdo_within_distance('SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL)', OBJECTVALUE, 'DISTANCE=25.0 UNIT=NAUT_MILE') = true"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE sdo_within_distance(SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL), SmallA.ObjectValue, 'DISTANCE=25.0 UNIT=NAUT_MILE') = 'true'";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test a query which uses 
     * <code>sdo_within_distance(String element, String literal, String literal) 
     * in its criteria into a source specific command.
     * 
     * @throws Exception
     */
    @Test public void test_sdo_within_distance3() throws Exception {
        String input = "SELECT INTKEY FROM BQT1.SMALLA WHERE sdo_within_distance(STRINGKEY, 'SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL)', 'DISTANCE=25.0 UNIT=NAUT_MILE') = true"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE sdo_within_distance(SmallA.StringKey, SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL), 'DISTANCE=25.0 UNIT=NAUT_MILE') = 'true'";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test a query which uses 
     * <code>sdo_within_distance(String literal, String literal, String literal) 
     * in its criteria into a source specific command.
     * 
     * @throws Exception
     */
    @Test public void test_sdo_within_distance4() throws Exception {
        String input = "SELECT INTKEY FROM BQT1.SMALLA WHERE sdo_within_distance('SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL)', 'SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL)', 'DISTANCE=25.0 UNIT=NAUT_MILE') = true"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE sdo_within_distance(SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL), SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL), 'DISTANCE=25.0 UNIT=NAUT_MILE') = 'true'";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test a query which uses 
     * <code>sdo_within_distance(Object element, Object element, String literal) 
     * in its criteria into a source specific command.
     * 
     * @throws Exception
     */
    @Test public void test_sdo_within_distance5() throws Exception {
        String input = "SELECT a.INTKEY FROM BQT1.SMALLA A, BQT1.SMALLB B WHERE sdo_within_distance(a.OBJECTVALUE, b.OBJECTVALUE, 'DISTANCE=25.0 UNIT=NAUT_MILE') = true"; //$NON-NLS-1$
        String output = "SELECT A.IntKey FROM SmallA A, SmallB B WHERE sdo_within_distance(A.ObjectValue, B.ObjectValue, 'DISTANCE=25.0 UNIT=NAUT_MILE') = 'true'";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testLogFunction() throws Exception {
        String input = "SELECT log(CONVERT(stringkey, INTEGER)) FROM bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT ln(trunc(to_number(SmallA.StringKey))) FROM SmallA"; //$NON-NLS-1$
    
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testLog10Function() throws Exception {
        String input = "SELECT log10(CONVERT(stringkey, INTEGER)) FROM bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT log(10, trunc(to_number(SmallA.StringKey))) FROM SmallA"; //$NON-NLS-1$
    
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testAliasedFunctions() throws Exception {
        String input = "SELECT char(CONVERT(stringkey, INTEGER)), lcase(stringkey), ucase(stringkey), ifnull(stringkey, 'x') FROM bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT chr(trunc(to_number(SmallA.StringKey))), lower(SmallA.StringKey), upper(SmallA.StringKey), nvl(SmallA.StringKey, 'x') FROM SmallA"; //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, UDF,
                input, output, 
                TRANSLATOR);
    }    
    
    private String getTestVDB() {
        return UnitTestUtil.getTestDataPath() + "/PartsSupplierOracle.vdb"; //$NON-NLS-1$
    }
    
    private void helpTestVisitor(String vdb, String input, String dbmsTimeZone, String expectedOutput) throws TranslatorException {
        helpTestVisitor(vdb, input, EMPTY_CONTEXT, dbmsTimeZone, expectedOutput, false);
    }

    private void helpTestVisitor(String vdb, String input, String dbmsTimeZone, String expectedOutput, boolean correctNaming) throws TranslatorException {
        helpTestVisitor(vdb, input, EMPTY_CONTEXT, dbmsTimeZone, expectedOutput, correctNaming);
    }

    private void helpTestVisitor(String vdb, String input, ExecutionContext context, String dbmsTimeZone, String expectedOutput, boolean correctNaming) throws TranslatorException {
        // Convert from sql to objects
        TranslationUtility util = new TranslationUtility(vdb);
        Command obj =  util.parseCommand(input, correctNaming, true);        
		this.helpTestVisitor(obj, context, dbmsTimeZone, expectedOutput);
    }

    /** Helper method takes a QueryMetadataInterface impl instead of a VDB filename 
     * @throws TranslatorException 
     */
    private void helpTestVisitor(QueryMetadataInterface metadata, String input, ExecutionContext context, String dbmsTimeZone, String expectedOutput) throws TranslatorException {
        // Convert from sql to objects
        CommandBuilder commandBuilder = new CommandBuilder(metadata);
        Command obj = commandBuilder.getCommand(input);
		this.helpTestVisitor(obj, context, dbmsTimeZone, expectedOutput);
    }
    
    private void helpTestVisitor(Command obj, ExecutionContext context, String dbmsTimeZone, String expectedOutput) throws TranslatorException {
        OracleExecutionFactory translator = new OracleExecutionFactory();
        if (dbmsTimeZone != null) {
        	translator.setDatabaseTimeZone(dbmsTimeZone);
        }
        translator.setUseBindVariables(false);
        translator.start();
        // Convert back to SQL
        TranslatedCommand tc = new TranslatedCommand(context, translator);
        tc.translateCommand(obj);
        
        // Check stuff
        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
    }
    
    /** defect 21775 */
    @Test public void testDateStuff() throws Exception {
        String input = "SELECT ((CASE WHEN month(datevalue) < 10 THEN ('0' || convert(month(datevalue), string)) ELSE convert(month(datevalue), string) END || CASE WHEN dayofmonth(datevalue) < 10 THEN ('0' || convert(dayofmonth(datevalue), string)) ELSE convert(dayofmonth(datevalue), string) END) || convert(year(datevalue), string)), SUM(intkey) FROM bqt1.SMALLA GROUP BY datevalue"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN CASE WHEN CASE WHEN EXTRACT(MONTH FROM SmallA.DateValue) < 10 THEN CASE WHEN to_char(EXTRACT(MONTH FROM SmallA.DateValue)) IS NULL THEN NULL ELSE concat('0', to_char(EXTRACT(MONTH FROM SmallA.DateValue))) END ELSE to_char(EXTRACT(MONTH FROM SmallA.DateValue)) END IS NULL OR CASE WHEN EXTRACT(DAY FROM SmallA.DateValue) < 10 THEN CASE WHEN to_char(EXTRACT(DAY FROM SmallA.DateValue)) IS NULL THEN NULL ELSE concat('0', to_char(EXTRACT(DAY FROM SmallA.DateValue))) END ELSE to_char(EXTRACT(DAY FROM SmallA.DateValue)) END IS NULL THEN NULL ELSE concat(CASE WHEN EXTRACT(MONTH FROM SmallA.DateValue) < 10 THEN CASE WHEN to_char(EXTRACT(MONTH FROM SmallA.DateValue)) IS NULL THEN NULL ELSE concat('0', to_char(EXTRACT(MONTH FROM SmallA.DateValue))) END ELSE to_char(EXTRACT(MONTH FROM SmallA.DateValue)) END, CASE WHEN EXTRACT(DAY FROM SmallA.DateValue) < 10 THEN CASE WHEN to_char(EXTRACT(DAY FROM SmallA.DateValue)) IS NULL THEN NULL ELSE concat('0', to_char(EXTRACT(DAY FROM SmallA.DateValue))) END ELSE to_char(EXTRACT(DAY FROM SmallA.DateValue)) END) END IS NULL OR to_char(EXTRACT(YEAR FROM SmallA.DateValue)) IS NULL THEN NULL ELSE concat(CASE WHEN CASE WHEN EXTRACT(MONTH FROM SmallA.DateValue) < 10 THEN CASE WHEN to_char(EXTRACT(MONTH FROM SmallA.DateValue)) IS NULL THEN NULL ELSE concat('0', to_char(EXTRACT(MONTH FROM SmallA.DateValue))) END ELSE to_char(EXTRACT(MONTH FROM SmallA.DateValue)) END IS NULL OR CASE WHEN EXTRACT(DAY FROM SmallA.DateValue) < 10 THEN CASE WHEN to_char(EXTRACT(DAY FROM SmallA.DateValue)) IS NULL THEN NULL ELSE concat('0', to_char(EXTRACT(DAY FROM SmallA.DateValue))) END ELSE to_char(EXTRACT(DAY FROM SmallA.DateValue)) END IS NULL THEN NULL ELSE concat(CASE WHEN EXTRACT(MONTH FROM SmallA.DateValue) < 10 THEN CASE WHEN to_char(EXTRACT(MONTH FROM SmallA.DateValue)) IS NULL THEN NULL ELSE concat('0', to_char(EXTRACT(MONTH FROM SmallA.DateValue))) END ELSE to_char(EXTRACT(MONTH FROM SmallA.DateValue)) END, CASE WHEN EXTRACT(DAY FROM SmallA.DateValue) < 10 THEN CASE WHEN to_char(EXTRACT(DAY FROM SmallA.DateValue)) IS NULL THEN NULL ELSE concat('0', to_char(EXTRACT(DAY FROM SmallA.DateValue))) END ELSE to_char(EXTRACT(DAY FROM SmallA.DateValue)) END) END, to_char(EXTRACT(YEAR FROM SmallA.DateValue))) END, SUM(SmallA.IntKey) FROM SmallA GROUP BY SmallA.DateValue"; //$NON-NLS-1$
        
        helpTestVisitor(RealMetadataFactory.exampleBQTCached(),
                        input, 
                        EMPTY_CONTEXT, null, output);
    }
    
    @Test public void testAliasedGroup() throws Exception {
        helpTestVisitor(getTestVDB(),
            "select y.part_name from parts as y", //$NON-NLS-1$
            null,
            "SELECT y.PART_NAME FROM PARTS y"); //$NON-NLS-1$
    }
    
    @Test public void testDateLiteral() throws Exception {
        helpTestVisitor(getTestVDB(),
            "select {d '2002-12-31'} FROM parts", //$NON-NLS-1$
            null,
            "SELECT {d '2002-12-31'} FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testTimeLiteral() throws Exception {
        helpTestVisitor(getTestVDB(),
            "select {t '13:59:59'} FROM parts", //$NON-NLS-1$
            null,
            "SELECT to_date('1970-01-01 13:59:59', 'YYYY-MM-DD HH24:MI:SS') FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testTimestampLiteral() throws Exception {
        helpTestVisitor(getTestVDB(),
            "select {ts '2002-12-31 13:59:59.1'} FROM parts", //$NON-NLS-1$
            null,
            "SELECT {ts '2002-12-31 13:59:59.1'} FROM PARTS"); //$NON-NLS-1$
    }
    
    @Test public void testTimestampLiteral1() throws Exception {
        helpTestVisitor(getTestVDB(),
            "select {ts '2002-12-31 13:59:59'} FROM parts", //$NON-NLS-1$
            null,
            "SELECT to_date('2002-12-31 13:59:59', 'YYYY-MM-DD HH24:MI:SS') FROM PARTS"); //$NON-NLS-1$
    }

    @Test public void testUnionOrderByWithThreeBranches() throws Exception {
        helpTestVisitor(getTestVDB(),
                        "select part_id id FROM parts UNION ALL select part_name FROM parts UNION ALL select part_id FROM parts ORDER BY id", //$NON-NLS-1$
                        null,
                        "SELECT g_2.PART_ID AS c_0 FROM PARTS g_2 UNION ALL SELECT g_1.PART_NAME AS c_0 FROM PARTS g_1 UNION ALL SELECT g_0.PART_ID AS c_0 FROM PARTS g_0 ORDER BY c_0", //$NON-NLS-1$
                        true); 
    }
    
    @Test public void testUnionOrderBy() throws Exception {
        helpTestVisitor(getTestVDB(),
                        "select part_id FROM parts UNION ALL select part_name FROM parts ORDER BY part_id", //$NON-NLS-1$
                        null,
                        "SELECT g_1.PART_ID AS c_0 FROM PARTS g_1 UNION ALL SELECT g_0.PART_NAME AS c_0 FROM PARTS g_0 ORDER BY c_0", //$NON-NLS-1$
                        true); 
    }

    @Test public void testUnionOrderBy2() throws Exception {
        helpTestVisitor(getTestVDB(),
                        "select part_id as p FROM parts UNION ALL select part_name FROM parts ORDER BY p", //$NON-NLS-1$
                        null,
                        "SELECT PARTS.PART_ID AS p FROM PARTS UNION ALL SELECT PARTS.PART_NAME FROM PARTS ORDER BY p"); //$NON-NLS-1$
    }

    @Test public void testUpdateWithFunction() throws Exception {
        String input = "UPDATE bqt1.smalla SET intkey = intkey + 1"; //$NON-NLS-1$
        String output = "UPDATE SmallA SET IntKey = (SmallA.IntKey + 1)"; //$NON-NLS-1$
        
        helpTestVisitor(RealMetadataFactory.exampleBQTCached(),
                input, 
                EMPTY_CONTEXT, null, output);
    }
    

    /**
     * Oracle's DUAL table is a pseudo-table; element names cannot be 
     * fully qualified since the table doesn't really exist nor contain
     * any columns.  But this requires modeling the DUAL table in 
     * MM as if it were a real physical table, and also modeling any
     * columns in the table.  Case 3742
     * 
     * @since 4.3
     */
    @Test public void testDUAL() throws Exception {
        String input = "SELECT something FROM DUAL"; //$NON-NLS-1$
        String output = "SELECT something FROM DUAL"; //$NON-NLS-1$
               
        helpTestVisitor(getOracleSpecificMetadata(),
            input, 
            EMPTY_CONTEXT,
            null,
            output);
    }

    /**
     * Test Oracle's rownum pseudo-column.  Not a real column, so it can't
     * be fully-qualified with a table name.  MM requires this column to be
     * modeled in any table which the user wants to use rownum with.
     * Case 3739
     * 
     * @since 4.3
     */
    @Test public void testROWNUM() throws Exception {
        String input = "SELECT part_name, rownum FROM parts"; //$NON-NLS-1$
        String output = "SELECT PARTS.PART_NAME, ROWNUM FROM PARTS"; //$NON-NLS-1$
               
        helpTestVisitor(getTestVDB(),
            input, 
            null,
            output);        
    }
    
    /**
     * Test Oracle's rownum pseudo-column.  Not a real column, so it can't
     * be fully-qualified with a table name.  MM requires this column to be
     * modeled in any table which the user wants to use rownum with.  Case 3739
     * 
     * @since 4.3
     */
    @Test public void testROWNUM2() throws Exception {
        String input = "SELECT part_name FROM parts where rownum < 100"; //$NON-NLS-1$
        String output = "SELECT PARTS.PART_NAME FROM PARTS WHERE ROWNUM < 100"; //$NON-NLS-1$
               
        helpTestVisitor(getTestVDB(),
            input, 
            null,
            output);            }    
    
    /**
     * Case 3744.  Test that an Oracle-specific db hint, delivered as a String via command
     * payload, is added to the translated SQL.
     * 
     * @since 4.3
     */
    @Test public void testOracleCommentPayload() throws Exception {
        String input = "SELECT part_name, rownum FROM parts"; //$NON-NLS-1$
        String output = "SELECT /*+ ALL_ROWS */ PARTS.PART_NAME, ROWNUM FROM PARTS"; //$NON-NLS-1$
               
        String hint = "/*+ ALL_ROWS */"; //$NON-NLS-1$
        ExecutionContext context = new ExecutionContextImpl(null, 1, hint, null, "", null, null, null); //$NON-NLS-1$
        
        helpTestVisitor(getTestVDB(),
            input, 
            context,
            null,
            output,
            false);        
    }
    
    @Test public void testOracleCommentPayload1() throws Exception {
        String input = "SELECT part_name, rownum FROM parts"; //$NON-NLS-1$
        String output = "SELECT PARTS.PART_NAME, ROWNUM FROM PARTS"; //$NON-NLS-1$
               
        String hint = "/*+ ALL_ROWS */ something else"; //$NON-NLS-1$
        ExecutionContext context = new ExecutionContextImpl(null, 1, hint, null, "", null, null, null); //$NON-NLS-1$
        
        helpTestVisitor(getTestVDB(),
            input, 
            context,
            null,
            output,
            false);        
    }
    
    /**
     * reproducing this case relies on the name in source for the table being different from
     * the name
     */
    @Test public void testCase3845() throws Exception {
        
        String input = "SELECT (DoubleNum * 1.0) FROM BQT1.Smalla"; //$NON-NLS-1$
        String output = "SELECT (SmallishA.DoubleNum * 1.0) FROM SmallishA"; //$NON-NLS-1$

        QueryMetadataInterface metadata = getOracleSpecificMetadata();

        helpTestVisitor(metadata, input, EMPTY_CONTEXT, null, output);
    }
    
    /** create fake BQT metadata to test this case, name in source is important */
    private QueryMetadataInterface getOracleSpecificMetadata() { 
    	MetadataStore metadataStore = new MetadataStore();
    	Schema foo = RealMetadataFactory.createPhysicalModel("BQT1", metadataStore); //$NON-NLS-1$
        Table table = RealMetadataFactory.createPhysicalGroup("SmallA", foo); //$NON-NLS-1$
        Table x = RealMetadataFactory.createPhysicalGroup("x", foo); //$NON-NLS-1$
        x.setProperty(SQLConversionVisitor.TEIID_NATIVE_QUERY, "select c from d");
        Table dual = RealMetadataFactory.createPhysicalGroup("DUAL", foo); //$NON-NLS-1$
        table.setNameInSource("SmallishA");//$NON-NLS-1$
        String[] elemNames = new String[] {
            "DoubleNum",  //$NON-NLS-1$ 
            "ID", //$NON-NLS-1$
            "timestampvalue", //$NON-NLS-1$
            "description"
        };
        String[] elemTypes = new String[] {  
            DataTypeManager.DefaultDataTypes.DOUBLE,
            DataTypeManager.DefaultDataTypes.INTEGER,
            DataTypeManager.DefaultDataTypes.TIMESTAMP,
            DataTypeManager.DefaultDataTypes.STRING,
        };
        RealMetadataFactory.createElements(x, elemNames, elemTypes);
        List<Column> cols = RealMetadataFactory.createElements(table, elemNames, elemTypes);
        cols.get(1).setAutoIncremented(true);
        cols.get(1).setNameInSource("ID:SEQUENCE=MYSEQUENCE.nextVal"); //$NON-NLS-1$
        cols.get(2).setNativeType("date"); //$NON-NLS-1$
        cols.get(3).setNativeType("CHAR");
        RealMetadataFactory.createElements(dual, new String[] {"something"}, new String[] {DataTypeManager.DefaultDataTypes.STRING}); //$NON-NLS-1$
        
        ProcedureParameter in1 = RealMetadataFactory.createParameter("in1", SPParameter.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
		ColumnSet<Procedure> rs3 = RealMetadataFactory.createResultSet("proc.rs1", new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ 
        Procedure p = RealMetadataFactory.createStoredProcedure("proc", foo, Arrays.asList(in1));
        p.setResultSet(rs3);
        p.setProperty(SQLConversionVisitor.TEIID_NATIVE_QUERY, "select x from y where z = $1");
        
        CompositeMetadataStore store = new CompositeMetadataStore(metadataStore);
        return new TransformationMetadata(null, store, null, RealMetadataFactory.SFM.getSystemFunctions(), null);
    }

	public void helpTestVisitor(String vdb, String input, String expectedOutput) throws TranslatorException {
		helpTestVisitor(vdb, input, null, expectedOutput);
	}

    @Test public void testLimitWithNestedInlineView() throws Exception {
        String input = "select max(intkey), stringkey from (select intkey, stringkey from bqt1.smalla order by intkey limit 100) x group by stringkey"; //$NON-NLS-1$
        String output = "SELECT MAX(x.intkey), x.stringkey FROM (SELECT * FROM (SELECT SmallA.IntKey, SmallA.StringKey FROM SmallA ORDER BY SmallA.IntKey) WHERE ROWNUM <= 100) x GROUP BY x.stringkey"; //$NON-NLS-1$
               
        helpTestVisitor(RealMetadataFactory.exampleBQTCached(),
                input, 
                EMPTY_CONTEXT, null, output);        
    }
    
    @Test public void testExceptAsMinus() throws Exception {
        String input = "select intkey, intnum from bqt1.smalla except select intnum, intkey from bqt1.smallb"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey, SmallA.IntNum FROM SmallA MINUS SELECT SmallB.IntNum, SmallB.IntKey FROM SmallB"; //$NON-NLS-1$
               
        helpTestVisitor(RealMetadataFactory.exampleBQTCached(),
                input, 
                EMPTY_CONTEXT, null, output);        
    }
    
    @Test public void testConcat() throws Exception {
        String sql = "select concat(stringnum, stringkey) from BQT1.Smalla"; //$NON-NLS-1$       
        String expected = "SELECT CASE WHEN SmallA.StringNum IS NULL OR SmallA.StringKey IS NULL THEN NULL ELSE concat(SmallA.StringNum, SmallA.StringKey) END FROM SmallA"; //$NON-NLS-1$
        helpTestVisitor(RealMetadataFactory.exampleBQTCached(), sql, EMPTY_CONTEXT, null, expected);
    }
    
    @Test public void testConcat_withLiteral() throws Exception {
        String sql = "select stringnum || '1' from BQT1.Smalla"; //$NON-NLS-1$       
        String expected = "SELECT CASE WHEN SmallA.StringNum IS NULL THEN NULL ELSE concat(SmallA.StringNum, '1') END FROM SmallA"; //$NON-NLS-1$
        helpTestVisitor(RealMetadataFactory.exampleBQTCached(), sql, EMPTY_CONTEXT, null, expected);
    }
    
    @Test public void testRowLimitWithUnionOrderBy() throws Exception {
        String input = "(select intkey from bqt1.smalla limit 50, 100) union select intnum from bqt1.smalla order by intkey"; //$NON-NLS-1$
        String output = "(SELECT c_0 FROM (SELECT VIEW_FOR_LIMIT.*, ROWNUM ROWNUM_ FROM (SELECT g_1.IntKey AS c_0 FROM SmallA g_1) VIEW_FOR_LIMIT WHERE ROWNUM <= 150) WHERE ROWNUM_ > 50) UNION SELECT g_0.IntNum AS c_0 FROM SmallA g_0 ORDER BY c_0"; //$NON-NLS-1$
               
		CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Command obj = commandBuilder.getCommand(input, true, true);
		this.helpTestVisitor(obj, EMPTY_CONTEXT, null, output);
    }
    
    @Test public void testCot() throws Exception {
    	String sql = "select cot(doublenum) from BQT1.Smalla"; //$NON-NLS-1$       
        String expected = "SELECT (1 / tan(SmallA.DoubleNum)) FROM SmallA"; //$NON-NLS-1$
        helpTestVisitor(RealMetadataFactory.exampleBQTCached(), sql, EMPTY_CONTEXT, null, expected);
    }
    
    @Test public void testLikeRegex() throws Exception {
        String input = "SELECT intkey FROM BQT1.SMALLA where stringkey like_regex 'ab.*c+' and stringkey not like_regex 'ab{3,5}c'"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE SmallA.StringKey REGEXP_LIKE 'ab.*c+' AND SmallA.StringKey NOT REGEXP_LIKE 'ab{3,5}c'";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testCallWithResultSet() throws Exception {
        String input = "call spTest5(1)"; //$NON-NLS-1$
        String output = "{ ?= call spTest5(?)}";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    
	@Test public void testProcedureExecution() throws Exception {
		Command command = TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "call spTest8(1)"); //$NON-NLS-1$
		Connection connection = Mockito.mock(Connection.class);
		CallableStatement cs = Mockito.mock(CallableStatement.class);
		Mockito.stub(cs.getUpdateCount()).toReturn(-1);
		ResultSet rs = Mockito.mock(ResultSet.class);
		Mockito.stub(cs.getObject(1)).toReturn(rs);
		Mockito.stub(cs.getInt(3)).toReturn(4);
		Mockito.stub(connection.prepareCall("{ ?= call spTest8(?,?)}")).toReturn(cs); //$NON-NLS-1$
		OracleExecutionFactory ef = new OracleExecutionFactory();
		
		JDBCProcedureExecution procedureExecution = new JDBCProcedureExecution(command, connection, Mockito.mock(ExecutionContext.class),  ef);
		procedureExecution.execute();
		assertEquals(Arrays.asList(4), procedureExecution.getOutputParameterValues());
		Mockito.verify(cs, Mockito.times(1)).registerOutParameter(1, OracleExecutionFactory.CURSOR_TYPE);
		Mockito.verify(cs, Mockito.times(1)).getObject(1);
	}
	
	@Test public void testNativeQuery() throws Exception {
		String input = "SELECT (DoubleNum * 1.0) FROM x"; //$NON-NLS-1$
        String output = "SELECT (x.DoubleNum * 1.0) FROM (select c from d) x"; //$NON-NLS-1$

        QueryMetadataInterface metadata = getOracleSpecificMetadata();

        helpTestVisitor(metadata, input, EMPTY_CONTEXT, null, output);
	}
	
	@Test public void testNativeQueryProc() throws Exception {
		String input = "call proc(2)"; //$NON-NLS-1$
        String output = "select x from y where z = ?"; //$NON-NLS-1$

        QueryMetadataInterface metadata = getOracleSpecificMetadata();

        helpTestVisitor(metadata, input, EMPTY_CONTEXT, null, output);
	}
	
	@Test public void testNativeQueryProcPreparedExecution() throws Exception {
		CommandBuilder commandBuilder = new CommandBuilder(getOracleSpecificMetadata());
        Command command = commandBuilder.getCommand("call proc(2)");
		Connection connection = Mockito.mock(Connection.class);
		CallableStatement cs = Mockito.mock(CallableStatement.class);
		Mockito.stub(cs.getUpdateCount()).toReturn(-1);
		ResultSet rs = Mockito.mock(ResultSet.class);
		Mockito.stub(cs.getObject(1)).toReturn(rs);
		Mockito.stub(cs.getInt(3)).toReturn(4);
		Mockito.stub(connection.prepareCall("select x from y where z = ?")).toReturn(cs); //$NON-NLS-1$
		OracleExecutionFactory ef = new OracleExecutionFactory();
		
		JDBCProcedureExecution procedureExecution = new JDBCProcedureExecution(command, connection, Mockito.mock(ExecutionContext.class),  ef);
		procedureExecution.execute();
		Mockito.verify(cs, Mockito.never()).registerOutParameter(1, OracleExecutionFactory.CURSOR_TYPE);
		Mockito.verify(cs, Mockito.never()).getObject(1);
		Mockito.verify(cs, Mockito.times(1)).setObject(1, 2, Types.INTEGER);
	}
	
	@Test public void testCharType() throws Exception {
		CommandBuilder commandBuilder = new CommandBuilder(getOracleSpecificMetadata());
        Command command = commandBuilder.getCommand("select id from smalla where description = 'a'");
        ((Literal)((Comparison)((Select)command).getWhere()).getRightExpression()).setBindEligible(true);
		Connection connection = Mockito.mock(Connection.class);
		PreparedStatement ps = Mockito.mock(PreparedStatement.class);
		Mockito.stub(connection.prepareStatement("SELECT SmallishA.ID FROM SmallishA WHERE SmallishA.description = ?")).toReturn(ps); //$NON-NLS-1$
		OracleExecutionFactory ef = new OracleExecutionFactory();
		
		JDBCQueryExecution e = new JDBCQueryExecution(command, connection, Mockito.mock(ExecutionContext.class),  ef);
		e.execute();
		Mockito.verify(ps, Mockito.times(1)).setObject(1, "a", 999);
	}
	
    @Test public void testParseFormat() throws Exception {
        String input = "select parsetimestamp(smalla.timestampvalue, 'yyyy.MM.dd'), formattimestamp(smalla.timestampvalue, 'yy.MM.dd') from bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT TO_TIMESTAMP(to_char(cast(g_0.TimestampValue AS timestamp), 'YYYY-MM-DD HH24:MI:SS.FF'), YYYY.MM.DD), TO_CHAR(g_0.TimestampValue, YY.MM.DD) FROM SmallA g_0"; //$NON-NLS-1$
               
		CommandBuilder commandBuilder = new CommandBuilder(RealMetadataFactory.exampleBQTCached());
        Command obj = commandBuilder.getCommand(input, true, true);
        TranslationHelper.helpTestVisitor(output, TRANSLATOR, obj);
    }

}
