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

package org.teiid.query.unittest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.SPParameter;

@SuppressWarnings("nls")
public class RealMetadataFactory {

    private static TransformationMetadata CACHED_BQT = exampleBQT();
        
	private RealMetadataFactory() { }
	
    public static TransformationMetadata exampleBQTCached() {
        return CACHED_BQT;
    }
    public static MetadataStore exampleBQTStore() {
    	MetadataStore metadataStore = new MetadataStore();
    	
        Schema bqt1 = createPhysicalModel("BQT1", metadataStore); //$NON-NLS-1$
        Schema bqt2 = createPhysicalModel("BQT2", metadataStore); //$NON-NLS-1$
        Schema bqt3 = createPhysicalModel("BQT3", metadataStore); //$NON-NLS-1$
        Schema lob = createPhysicalModel("LOB", metadataStore); //$NON-NLS-1$
        Schema vqt = createVirtualModel("VQT", metadataStore); //$NON-NLS-1$
        Schema bvqt = createVirtualModel("BQT_V", metadataStore); //$NON-NLS-1$
        Schema bvqt2 = createVirtualModel("BQT2_V", metadataStore); //$NON-NLS-1$
        
        // Create physical groups
        Table bqt1SmallA = createPhysicalGroup("SmallA", bqt1); //$NON-NLS-1$
        Table bqt1SmallB = createPhysicalGroup("SmallB", bqt1); //$NON-NLS-1$
        Table bqt1MediumA = createPhysicalGroup("MediumA", bqt1); //$NON-NLS-1$
        Table bqt1MediumB = createPhysicalGroup("MediumB", bqt1); //$NON-NLS-1$
        Table bqt2SmallA = createPhysicalGroup("SmallA", bqt2); //$NON-NLS-1$
        Table bqt2SmallB = createPhysicalGroup("SmallB", bqt2); //$NON-NLS-1$
        Table bqt2MediumA = createPhysicalGroup("MediumA", bqt2); //$NON-NLS-1$
        Table bqt2MediumB = createPhysicalGroup("MediumB", bqt2); //$NON-NLS-1$
        Table bqt3SmallA = createPhysicalGroup("SmallA", bqt3); //$NON-NLS-1$
        Table bqt3SmallB = createPhysicalGroup("SmallB", bqt3); //$NON-NLS-1$
        Table bqt3MediumA = createPhysicalGroup("MediumA", bqt3); //$NON-NLS-1$
        Table bqt3MediumB = createPhysicalGroup("MediumB", bqt3); //$NON-NLS-1$
        Table lobTable = createPhysicalGroup("LobTbl", lob); //$NON-NLS-1$
        Table library = createPhysicalGroup("LOB_TESTING_ONE", lob); //$NON-NLS-1$
        
        createElements( library, new String[] { "CLOB_COLUMN", "BLOB_COLUMN", "KEY_EMULATOR" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        		new String[] { DataTypeManager.DefaultDataTypes.CLOB, DataTypeManager.DefaultDataTypes.BLOB, DataTypeManager.DefaultDataTypes.INTEGER }); 

        // Create virtual groups
        QueryNode vqtn1 = new QueryNode("SELECT * FROM BQT1.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vqtg1 = createUpdatableVirtualGroup("SmallA", vqt, vqtn1); //$NON-NLS-1$
        
        QueryNode vqtn2 = new QueryNode("SELECT Concat(stringKey, stringNum) as a12345 FROM BQT1.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vqtg2 = createUpdatableVirtualGroup("SmallB", vqt, vqtn2); //$NON-NLS-1$        

        // Case 2589
        QueryNode vqtn2589 = new QueryNode("SELECT * FROM BQT1.SmallA WHERE StringNum = '10'"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vqtg2589 = createVirtualGroup("SmallA_2589", vqt, vqtn2589); //$NON-NLS-1$

        QueryNode vqtn2589a = new QueryNode("SELECT BQT1.SmallA.* FROM BQT1.SmallA INNER JOIN BQT1.SmallB ON SmallA.IntKey = SmallB.IntKey WHERE SmallA.StringNum = '10'"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vqtg2589a = createVirtualGroup("SmallA_2589a", vqt, vqtn2589a); //$NON-NLS-1$

        QueryNode vqtn2589b = new QueryNode("SELECT BQT1.SmallA.* FROM BQT1.SmallA INNER JOIN BQT1.SmallB ON SmallA.StringKey = SmallB.StringKey WHERE SmallA.StringNum = '10'"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vqtg2589b = createVirtualGroup("SmallA_2589b", vqt, vqtn2589b); //$NON-NLS-1$

        QueryNode vqtn2589c = new QueryNode("SELECT BQT1.SmallA.* FROM BQT1.SmallA INNER JOIN BQT1.SmallB ON SmallA.StringKey = SmallB.StringKey WHERE concat(SmallA.StringNum, SmallB.StringNum) = '1010'"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vqtg2589c = createVirtualGroup("SmallA_2589c", vqt, vqtn2589c); //$NON-NLS-1$
        
        QueryNode vqtn2589d = new QueryNode("SELECT BQT1.SmallA.* FROM BQT1.SmallA INNER JOIN BQT1.SmallB ON SmallA.StringKey = SmallB.StringKey WHERE SmallA.StringNum = '10' AND SmallA.IntNum = 10"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vqtg2589d = createVirtualGroup("SmallA_2589d", vqt, vqtn2589d); //$NON-NLS-1$

        QueryNode vqtn2589f = new QueryNode("SELECT * FROM VQT.SmallA_2589"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vqtg2589f = createVirtualGroup("SmallA_2589f", vqt, vqtn2589f); //$NON-NLS-1$

        QueryNode vqtn2589g = new QueryNode("SELECT * FROM SmallA_2589b"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vqtg2589g = createVirtualGroup("SmallA_2589g", vqt, vqtn2589g); //$NON-NLS-1$

        QueryNode vqtn2589h = new QueryNode("SELECT VQT.SmallA_2589.* FROM VQT.SmallA_2589 INNER JOIN BQT1.SmallB ON VQT.SmallA_2589.StringKey = SmallB.StringKey"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vqtg2589h = createVirtualGroup("SmallA_2589h", vqt, vqtn2589h); //$NON-NLS-1$
        
        QueryNode vqtn2589i = new QueryNode("SELECT BQT1.SmallA.* FROM BQT1.SmallA INNER JOIN BQT1.SmallB ON SmallA.StringKey = SmallB.StringKey WHERE SmallA.StringNum = '10' AND SmallB.StringNum = '10'"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vqtg2589i = createVirtualGroup("SmallA_2589i", vqt, vqtn2589i); //$NON-NLS-1$

        // defect 15355
        QueryNode vqtn15355  = new QueryNode("SELECT convert(IntKey, string) as StringKey, BigIntegerValue FROM BQT1.SmallA UNION SELECT StringKey, (SELECT BigIntegerValue FROM BQT3.SmallA WHERE BQT3.SmallA.BigIntegerValue = BQT2.SmallA.StringNum) FROM BQT2.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vqtg15355  = createVirtualGroup("Defect15355", vqt, vqtn15355); //$NON-NLS-1$
        QueryNode vqtn15355a  = new QueryNode("SELECT StringKey, StringNum, BigIntegerValue FROM BQT1.SmallA UNION SELECT StringKey, StringNum, (SELECT BigIntegerValue FROM BQT3.SmallA WHERE BQT3.SmallA.BigIntegerValue = BQT2.SmallA.StringNum) FROM BQT2.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vqtg15355a  = createVirtualGroup("Defect15355a", vqt, vqtn15355a); //$NON-NLS-1$
        QueryNode vqtn15355b  = new QueryNode("SELECT convert(IntKey, string) as IntKey, BigIntegerValue FROM BQT1.SmallA UNION SELECT StringKey, (SELECT BigIntegerValue FROM BQT3.SmallA WHERE BQT3.SmallA.BigIntegerValue = BQT2.SmallA.StringNum) FROM BQT2.SmallA"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vqtg15355b  = createVirtualGroup("Defect15355b", vqt, vqtn15355b); //$NON-NLS-1$
        
        QueryNode bvqtn1 = new QueryNode("SELECT a.* FROM BQT1.SMALLA AS a WHERE a.INTNUM = (SELECT MIN(b.INTNUM) FROM BQT1.SMALLA AS b WHERE b.INTKEY = a.IntKey ) OPTION MAKEDEP a"); //$NON-NLS-1$ //$NON-NLS-2$
        Table bvqtg1 = createUpdatableVirtualGroup("BQT_V", bvqt, bvqtn1); //$NON-NLS-1$
        QueryNode bvqt2n1 = new QueryNode("SELECT BQT2.SmallA.* FROM BQT2.SmallA, BQT_V.BQT_V WHERE BQT2.SmallA.IntKey = BQT_V.BQT_V.IntKey"); //$NON-NLS-1$ //$NON-NLS-2$
        Table bvqt2g1 = createUpdatableVirtualGroup("BQT2_V", bvqt2, bvqt2n1); //$NON-NLS-1$

     // Create physical elements
        String[] elemNames = new String[] { 
             "IntKey", "StringKey",  //$NON-NLS-1$ //$NON-NLS-2$
             "IntNum", "StringNum",  //$NON-NLS-1$ //$NON-NLS-2$
             "FloatNum", "LongNum",  //$NON-NLS-1$ //$NON-NLS-2$
             "DoubleNum", "ByteNum",  //$NON-NLS-1$ //$NON-NLS-2$
             "DateValue", "TimeValue",  //$NON-NLS-1$ //$NON-NLS-2$
             "TimestampValue", "BooleanValue",  //$NON-NLS-1$ //$NON-NLS-2$
             "CharValue", "ShortValue",  //$NON-NLS-1$ //$NON-NLS-2$
             "BigIntegerValue", "BigDecimalValue",  //$NON-NLS-1$ //$NON-NLS-2$
             "ObjectValue" }; //$NON-NLS-1$
         String[] elemTypes = new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, 
                             DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, 
                             DataTypeManager.DefaultDataTypes.FLOAT, DataTypeManager.DefaultDataTypes.LONG, 
                             DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.BYTE, 
                             DataTypeManager.DefaultDataTypes.DATE, DataTypeManager.DefaultDataTypes.TIME, 
                             DataTypeManager.DefaultDataTypes.TIMESTAMP, DataTypeManager.DefaultDataTypes.BOOLEAN, 
                             DataTypeManager.DefaultDataTypes.CHAR, DataTypeManager.DefaultDataTypes.SHORT, 
                             DataTypeManager.DefaultDataTypes.BIG_INTEGER, DataTypeManager.DefaultDataTypes.BIG_DECIMAL, 
                             DataTypeManager.DefaultDataTypes.OBJECT };
        
        List<Column> bqt1SmallAe = createElements(bqt1SmallA, elemNames, elemTypes);
        bqt1SmallAe.get(1).setNativeType("char"); //$NON-NLS-1$
        createElements(bqt1SmallB, elemNames, elemTypes);
        createElements(bqt1MediumA, elemNames, elemTypes);
        createElements(bqt1MediumB, elemNames, elemTypes);
        createElements(bqt2SmallA, elemNames, elemTypes);
        createElements(bqt2SmallB, elemNames, elemTypes);
        createElements(bqt2MediumA, elemNames, elemTypes);
        createElements(bqt2MediumB, elemNames, elemTypes);                
        createElements(bqt3SmallA, elemNames, elemTypes);
        createElements(bqt3SmallB, elemNames, elemTypes);
        createElements(bqt3MediumA, elemNames, elemTypes);
        createElements(bqt3MediumB, elemNames, elemTypes);
        createElements(lobTable, new String[] {"ClobValue"}, new String[] {DataTypeManager.DefaultDataTypes.CLOB}); //$NON-NLS-1$
        
        // Create virtual elements
        createElements(vqtg1, elemNames, elemTypes);        
        createElements(vqtg2, new String[] {"a12345"}, new String[] {DataTypeManager.DefaultDataTypes.STRING});  //$NON-NLS-1$
        createElements(vqtg15355, new String[] {"StringKey", "BigIntegerValue"}, new String[] {DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.BIG_INTEGER});         //$NON-NLS-1$ //$NON-NLS-2$
        createElements(vqtg15355a, new String[] {"StringKey", "StringNum", "BigIntegerValue"}, new String[] {DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.BIG_INTEGER});         //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        createElements(vqtg15355b, new String[] {"IntKey", "BigIntegerValue"}, new String[] {DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.BIG_INTEGER});         //$NON-NLS-1$ //$NON-NLS-2$
        createElements(vqtg2589, elemNames, elemTypes);        
        createElements(vqtg2589a, elemNames, elemTypes);        
        createElements(vqtg2589b, elemNames, elemTypes);        
        createElements(vqtg2589c, elemNames, elemTypes);        
        createElements(vqtg2589d, elemNames, elemTypes);        
        createElements(vqtg2589f, elemNames, elemTypes);        
        createElements(vqtg2589g, elemNames, elemTypes);        
        createElements(vqtg2589h, elemNames, elemTypes);        
        createElements(vqtg2589i, elemNames, elemTypes);
        createElements(bvqtg1, elemNames, elemTypes);        
        createElements(bvqt2g1, elemNames, elemTypes);        

     // Add stored procedure
        Schema pm1 = createPhysicalModel("pm1", metadataStore); //$NON-NLS-1$
        ProcedureParameter rs1p1 = createParameter("intkey", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER);         //$NON-NLS-1$
        ColumnSet<Procedure> rs1 = createResultSet("rs1", new String[] { "IntKey", "StringKey" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Procedure spTest5 = createStoredProcedure("spTest5", pm1, Arrays.asList(rs1p1), "spTest5"); //$NON-NLS-1$ //$NON-NLS-2$
        spTest5.setResultSet(rs1);

        Schema pm2 = createPhysicalModel("pm2", metadataStore); //$NON-NLS-1$
        ProcedureParameter rs2p1 = createParameter("inkey", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        ProcedureParameter rs2p2 = createParameter("outkey", ParameterInfo.OUT, DataTypeManager.DefaultDataTypes.INTEGER);                 //$NON-NLS-1$
        ColumnSet<Procedure> rs2 = createResultSet("rs2", new String[] { "IntKey", "StringKey"}, new String[] { DataTypeManager.DefaultDataTypes.INTEGER , DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Procedure spTest8 = createStoredProcedure("spTest8", pm2, Arrays.asList(rs2p1, rs2p2), "spTest8"); //$NON-NLS-1$ //$NON-NLS-2$
        spTest8.setResultSet(rs2);
        
        ProcedureParameter rs2p2a = createParameter("outkey", ParameterInfo.OUT, DataTypeManager.DefaultDataTypes.INTEGER);                 //$NON-NLS-1$
        ColumnSet<Procedure> rs2a = createResultSet("rs2", new String[] { "IntKey", "StringKey"}, new String[] { DataTypeManager.DefaultDataTypes.INTEGER , DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Procedure spTest8a = createStoredProcedure("spTest8a", pm2, Arrays.asList(rs2p2a), "spTest8a"); //$NON-NLS-1$ //$NON-NLS-2$
        spTest8a.setResultSet(rs2a);
        
        Schema pm4 = createPhysicalModel("pm4", metadataStore); //$NON-NLS-1$
        ProcedureParameter rs4p1 = createParameter("ret", ParameterInfo.RETURN_VALUE, DataTypeManager.DefaultDataTypes.INTEGER);  //$NON-NLS-1$
        ProcedureParameter rs4p2 = createParameter("inkey", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        createStoredProcedure("spTest9", pm4, Arrays.asList(rs4p1, rs4p2), "spTest9"); //$NON-NLS-1$ //$NON-NLS-2$
        
        Schema pm3 = createPhysicalModel("pm3", metadataStore); //$NON-NLS-1$
        ProcedureParameter rs3p1 = createParameter("inkey", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        ProcedureParameter rs3p2 = createParameter("outkey", ParameterInfo.INOUT, DataTypeManager.DefaultDataTypes.INTEGER);                 //$NON-NLS-1$
        ColumnSet<Procedure> rs3 = createResultSet("rs3", new String[] { "IntKey", "StringKey"}, new String[] { DataTypeManager.DefaultDataTypes.INTEGER , DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Procedure spTest11 = createStoredProcedure("spTest11", pm3, Arrays.asList(rs3p1, rs3p2), "spTest11"); //$NON-NLS-1$ //$NON-NLS-2$
        spTest11.setResultSet(rs3);
        
        //add virtual stored procedures 
        Schema mmspTest1 = createVirtualModel("mmspTest1", metadataStore); //$NON-NLS-1$
        ColumnSet<Procedure> vsprs1 = createResultSet("mmspTest1.vsprs1", new String[] { "StringKey" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode vspqn1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT intkey FROM bqt1.smallA) AS intKeyCursor BEGIN x= intKeyCursor.intkey - 1; END SELECT stringkey FROM bqt1.smalla where intkey=x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp1 = createVirtualProcedure("MMSP1", mmspTest1, null, vspqn1); //$NON-NLS-1$
        vsp1.setResultSet(vsprs1);

        ColumnSet<Procedure> vsprs2 = createResultSet("mmspTest1.vsprs1", new String[] { "StringKey" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode vspqn2 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT intkey FROM bqt1.smallA) AS intKeyCursor1 BEGIN LOOP ON (SELECT intkey FROM bqt1.smallB) AS intKeyCursor2 BEGIN x= intKeyCursor1.intkey - intKeyCursor2.intkey; END END SELECT stringkey FROM bqt1.smalla where intkey=x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp2 = createVirtualProcedure("MMSP2", mmspTest1, null, vspqn2); //$NON-NLS-1$
        vsp2.setResultSet(vsprs2);

        ColumnSet<Procedure> vsprs3 = createResultSet("mmspTest1.vsprs1", new String[] { "StringKey" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode vspqn3 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT intkey FROM bqt1.smallA) AS intKeyCursor BEGIN x= intKeyCursor.intkey - 1; if(x = 25) BEGIN BREAK; END ELSE BEGIN CONTINUE; END END SELECT stringkey FROM bqt1.smalla where intkey=x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp3 = createVirtualProcedure("MMSP3", mmspTest1, null, vspqn3); //$NON-NLS-1$
        vsp3.setResultSet(vsprs3);

        ColumnSet<Procedure> vsprs4 = createResultSet("mmspTest1.vsprs1", new String[] { "StringKey" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode vspqn4 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; x=0; WHILE(x < 50) BEGIN x= x + 1; if(x = 25) BEGIN BREAK; END ELSE BEGIN CONTINUE; END END SELECT stringkey FROM bqt1.smalla where intkey=x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp4 = createVirtualProcedure("MMSP4", mmspTest1, null, vspqn4); //$NON-NLS-1$
        vsp4.setResultSet(vsprs4);
        
        ColumnSet<Procedure> vsprs5 = createResultSet("mmspTest1.vsprs1", new String[] { "StringKey" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter vsp5p1 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        QueryNode vspqn5 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT 0; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp5 = createVirtualProcedure("MMSP5", mmspTest1, Arrays.asList(vsp5p1), vspqn5); //$NON-NLS-1$
        vsp5.setResultSet(vsprs5);

        ColumnSet<Procedure> vsprs6 = createResultSet("mmspTest1.vsprs1", new String[] { "StringKey" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter vsp6p1 = createParameter("p1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        QueryNode vspqn6 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT p1 as StringKey; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp6 = createVirtualProcedure("MMSP6", mmspTest1, Arrays.asList(vsp6p1), vspqn6); //$NON-NLS-1$
        vsp6.setResultSet(vsprs6);
        
        createStoredProcedure("spRetOut", pm4, Arrays.asList(createParameter("ret", ParameterInfo.RETURN_VALUE, DataTypeManager.DefaultDataTypes.INTEGER),
        		createParameter("x", ParameterInfo.OUT, DataTypeManager.DefaultDataTypes.INTEGER)), "spRetOut"); //$NON-NLS-1$ //$NON-NLS-2$
        
        ColumnSet<Procedure> vsprs7 = createResultSet("TEIIDSP7.vsprs1", new String[] { "StringKey" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter vsp7p1 = createParameter("p1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn7 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN declare integer x; x = exec spTest9(p1); declare integer y; exec spTest11(inkey=>x, outkey=>y); select convert(x, string) || y; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp7 = createVirtualProcedure("TEIIDSP7", mmspTest1, Arrays.asList(vsp7p1), vspqn7); //$NON-NLS-1$
        vsp7.setResultSet(vsprs7);
        
        ProcedureParameter vsp8p1 = createParameter("r", ParameterInfo.RETURN_VALUE, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        ProcedureParameter vsp8p2 = createParameter("p1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn8 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN r = p1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        createVirtualProcedure("TEIIDSP8", mmspTest1, Arrays.asList(vsp8p1, vsp8p2), vspqn8); //$NON-NLS-1$

        ColumnSet<Procedure> vsprs9 = createResultSet("TEIIDSP9.vsprs1", new String[] { "StringKey" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter vsp9p1 = createParameter("r", ParameterInfo.RETURN_VALUE, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        vsp9p1.setNullType(NullType.No_Nulls);
        ProcedureParameter vsp9p2 = createParameter("p1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        ProcedureParameter vsp9p3 = createParameter("p2", ParameterInfo.OUT, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn9 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN if (p1 = 1) begin\n r = 1; end\n p2 = 10; select 'hello'; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp9 = createVirtualProcedure("TEIIDSP9", mmspTest1, Arrays.asList(vsp9p1, vsp9p2, vsp9p3), vspqn9); //$NON-NLS-1$
        vsp9.setResultSet(vsprs9);
        
        // this is for the source added function
        bqt1.addFunction(new FunctionMethod("reverse", "reverse", "misc", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
                new FunctionParameter[] {new FunctionParameter("columnName", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$    		
        
    	 return metadataStore;
    }
    
	public static TransformationMetadata exampleBQT() {
		return createTransformationMetadata(exampleBQTStore(), "bqt");	
	}
    

	public static TransformationMetadata createTransformationMetadata(MetadataStore metadataStore, String vdbName) {
		CompositeMetadataStore store = new CompositeMetadataStore(metadataStore);
    	VDBMetaData vdbMetaData = new VDBMetaData();
    	vdbMetaData.setName(vdbName); //$NON-NLS-1$
    	vdbMetaData.setVersion(1);
    	List<FunctionTree> udfs = new ArrayList<FunctionTree>();
    	for (Schema schema : metadataStore.getSchemas().values()) {
			vdbMetaData.addModel(FakeMetadataFactory.createModel(schema.getName(), schema.isPhysical()));
			if (!schema.getFunctions().isEmpty()) {
				udfs.add(new FunctionTree(schema.getName(), new UDFSource(schema.getFunctions().values()), true));
			}
		}
    	return new TransformationMetadata(vdbMetaData, store, null, FakeMetadataFactory.SFM.getSystemFunctions(), udfs);
	}
	
    /** 
     * Metadata for Materialized Views
     * @return
     * @since 4.2
     */
    public static QueryMetadataInterface exampleMaterializedView() {
    	MetadataStore metadataStore = new MetadataStore();
        Schema virtModel = createVirtualModel("MatView", metadataStore); //$NON-NLS-1$
        Schema physModel = createPhysicalModel("MatTable", metadataStore); //$NON-NLS-1$
        Schema physModel_virtSrc = createPhysicalModel("MatSrc", metadataStore); //$NON-NLS-1$
        
        Table physTable = createPhysicalGroup("info", physModel); //$NON-NLS-1$
        createElements(physTable,
                                      new String[] { "e1", "e2", "e3"}, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING});
        
        Table physGroup = createPhysicalGroup("MatTable", physModel); //$NON-NLS-1$
        createElements(physGroup,
                                      new String[] { "e1" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING});
        
        Table physGroupStage = createPhysicalGroup("MatStage", physModel); //$NON-NLS-1$
        createElements(physGroupStage,
                                      new String[] { "e1" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING});
        
        Table physGroup1 = createPhysicalGroup("MatTable1", physModel); //$NON-NLS-1$
        createElements(physGroup1,
                                      new String[] { "e1" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING});
        
        Table physGroupStage1 = createPhysicalGroup("MatStage1", physModel); //$NON-NLS-1$
        createElements(physGroupStage,
                                      new String[] { "e1" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING});
        
        Table physGroup_virtSrc = createPhysicalGroup("MatSrc", physModel_virtSrc); //$NON-NLS-1$
        createElements(physGroup_virtSrc,
                                      new String[] { "X" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING});
        
        QueryNode virtTrans = new QueryNode("SELECT x as e1 FROM MatSrc.MatSrc");         //$NON-NLS-1$ //$NON-NLS-2$
        Table virtGroup = createVirtualGroup("MatView", virtModel, virtTrans); //$NON-NLS-1$
        createElements(virtGroup,
                                      new String[] { "e1" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING});
       
        virtGroup.setMaterialized(true);
        virtGroup.setMaterializedTable(physGroup);
        virtGroup.setMaterializedStageTable(physGroupStage);
        
        //add one virtual group that uses the materialized group in transformation with NOCACHE option
        QueryNode vTrans = new QueryNode("SELECT e1 FROM MatView.MatView option NOCACHE");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vGroup = createVirtualGroup("VGroup", virtModel, vTrans); //$NON-NLS-1$
        createElements(vGroup,
                                      new String[] { "e1" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING});
        
        QueryNode virtTrans1 = new QueryNode("SELECT e1 FROM MatView.MatView where e1 = 1");         //$NON-NLS-1$ //$NON-NLS-2$
        Table virtGroup1 = createVirtualGroup("MatView1", virtModel, virtTrans1); //$NON-NLS-1$
        createElements(virtGroup1,
                                      new String[] { "e1" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING});
        
        virtGroup1.setMaterializedTable(physGroup1);
        virtGroup1.setMaterializedStageTable(physGroupStage1);

        QueryNode vTrans2 = new QueryNode("SELECT x FROM matsrc");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vGroup2 = createVirtualGroup("VGroup2", virtModel, vTrans2); //$NON-NLS-1$
        vGroup2.setMaterialized(true);
        createElements(vGroup2,
                                      new String[] { "x" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING});
        
        //covering index
        QueryNode vTrans3 = new QueryNode("SELECT x, 'z' || substring(x, 2) as y FROM matsrc");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vGroup3 = createVirtualGroup("VGroup3", virtModel, vTrans3); //$NON-NLS-1$
        vGroup3.setMaterialized(true);
        List<Column> vElements3 = createElements(vGroup3,
                                      new String[] { "x", "y" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});
        
        createKey(KeyRecord.Type.Primary, "pk", vGroup3, vElements3.subList(0, 1));
        createKey(KeyRecord.Type.Index, "idx", vGroup3, vElements3.subList(1, 2));

        QueryNode vTrans4 = new QueryNode("/*+ cache(ttl:100) */ SELECT x FROM matsrc");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vGroup4 = createVirtualGroup("VGroup4", virtModel, vTrans4); //$NON-NLS-1$
        vGroup4.setMaterialized(true);
        createElements(vGroup4,
                                      new String[] { "x" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING});
        
        //non-covering index
        QueryNode vTrans5 = new QueryNode("SELECT x, 'z' || substring(x, 2) as y, 1 as z FROM matsrc");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vGroup5 = createVirtualGroup("VGroup5", virtModel, vTrans5); //$NON-NLS-1$
        vGroup5.setMaterialized(true);
        List<Column> vElements5 = createElements(vGroup5,
                                      new String[] { "x", "y", "z" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER});
        
        createKey(KeyRecord.Type.Primary, "pk", vGroup5, vElements5.subList(0, 1));
        createKey(KeyRecord.Type.Index, "idx", vGroup5, vElements5.subList(1, 2));
        
        //no pk
        QueryNode vTrans6 = new QueryNode("SELECT x, 'z' || substring(x, 2) as y FROM matsrc");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vGroup6 = createVirtualGroup("VGroup6", virtModel, vTrans6); //$NON-NLS-1$
        vGroup6.setMaterialized(true);
        List<Column> vElements6 = createElements(vGroup6,
                                      new String[] { "x", "y" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});
        
        createKey(KeyRecord.Type.Index, "idx", vGroup6, vElements6.subList(1, 2));
        
        //non-covering index
        QueryNode vTrans7 = new QueryNode("SELECT '1', 'z' || substring(x, 2) as y, 1 as z FROM matsrc");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vGroup7 = createVirtualGroup("VGroup7", virtModel, vTrans7); //$NON-NLS-1$
        vGroup7.setMaterialized(true);
        List<Column> vElements7 = createElements(vGroup7,
                                      new String[] { "x", "y", "z" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER});
        
        createKey(KeyRecord.Type.Primary, "pk", vGroup7, vElements7.subList(1, 2));
        
        Schema sp = createVirtualModel("sp", metadataStore); //$NON-NLS-1$
        ColumnSet<Procedure> rs = createResultSet("sp1.vsprs1", new String[] { "StringKey" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter param = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);
        QueryNode sp1qn = new QueryNode("/*+ cache */ CREATE VIRTUAL PROCEDURE BEGIN SELECT x as StringKey from matsrc where x = param1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp5 = createVirtualProcedure("sp1", sp, Arrays.asList(param), sp1qn); //$NON-NLS-1$
        vsp5.setResultSet(rs);

        return createTransformationMetadata(metadataStore, "");
    }
    
	/**
	 * Create primary key.  The name will be used as the Object metadataID.
	 * @param name String name of key
	 * @param group the group for the key
	 * @param elements the elements of the key (will be used as if they were
	 * metadata IDs)
	 * @return key metadata object
	 */
	public static KeyRecord createKey(KeyRecord.Type type, String name, Table group, List<Column> elements) {
		KeyRecord key = new KeyRecord(type);
		key.setName(name);
		for (Column column : elements) {
			key.addColumn(column);
		}
		switch (type) {
		case Primary:
			group.setPrimaryKey(key);
			break;
		case Index:
			group.getIndexes().add(key);
			break;
		case Unique:
			group.getUniqueKeys().add(key);
			break;
		default:
			throw new AssertionError("TODO");
		}
		return key;
	}
	
	public static ForeignKey createForeignKey(String name, Table group, List<Column> elements, KeyRecord primaryKey) {
		ForeignKey key = new ForeignKey();
		key.setName(name);
		for (Column column : elements) {
			key.addColumn(column);
		}
		key.setPrimaryKey(primaryKey);
		group.getForeignKeys().add(key);
		return key;
	}

    /**
     * Create a physical model with default settings.
     */
	public static Schema createPhysicalModel(String name, MetadataStore metadataStore) {
		Schema schema = new Schema();
		schema.setName(name);
		metadataStore.addSchema(schema);
		return schema;	
	}
	
    /**
     * Create a virtual model with default settings.
     */
	public static Schema createVirtualModel(String name, MetadataStore metadataStore) {
		Schema schema = new Schema();
		schema.setName(name);
		schema.setPhysical(false);
		metadataStore.addSchema(schema);
		return schema;	
	}
	
    /**
     * Create a physical group with default settings.
     * @param name Name of physical group, must match model name
     * @param model Associated model
     * @return FakeMetadataObject Metadata object for group
     */
	public static Table createPhysicalGroup(String name, Schema model, boolean fullyQualify) {
		Table table = new Table();
		table.setName(name);
		model.addTable(table);
		table.setSupportsUpdate(true);
		table.setNameInSource((fullyQualify || name.lastIndexOf(".") == -1)? name : name.substring(name.lastIndexOf(".") + 1));  //$NON-NLS-1$ //$NON-NLS-2$
		return table;
	}

    public static Table createPhysicalGroup(String name, Schema model) {
    	return createPhysicalGroup(name, model, false);
    }
    	
    /**
     * Create a virtual group with default settings.
     */
	public static Table createVirtualGroup(String name, Schema model, QueryNode plan) {
        Table table = new Table();
        table.setName(name);
        model.addTable(table);
        table.setVirtual(true);
        table.setSelectTransformation(plan.getQuery());
		return table;
	}

    /**
     * Create a virtual group that allows updates with default settings.
     */
	public static Table createUpdatableVirtualGroup(String name, Schema model, QueryNode plan) {
		return createUpdatableVirtualGroup(name, model, plan, null);
	}
    
    public static Table createUpdatableVirtualGroup(String name, Schema model, QueryNode plan, String updatePlan) {
        Table table = createVirtualGroup(name, model, plan);
        table.setUpdatePlan(updatePlan);
        table.setSupportsUpdate(true);
        return table;
    }
		
    public static Column createElement(String name, ColumnSet<?> group, String type) { 
        Column column = new Column();
        column.setName(name);
        group.addColumn(column);
        column.setRuntimeType(type);
        if(type.equals(DataTypeManager.DefaultDataTypes.STRING)) {  
            column.setSearchType(SearchType.Searchable);        
        } else if (DataTypeManager.isNonComparable(type)){
        	column.setSearchType(SearchType.Unsearchable);
        } else {        	
        	column.setSearchType(SearchType.All_Except_Like);
        }   
        column.setNullType(NullType.Nullable);
        column.setPosition(group.getColumns().size()); //1 based indexing
        column.setUpdatable(true);
        column.setLength(100);
        column.setNameInSource(name);
        return column; 
    }
    
    /**
     * Create a set of elements in batch 
     */
	public static List<Column> createElements(ColumnSet<?> group, String[] names, String[] types) { 
		return createElementsWithDefaults(group, names, types, new String[names.length]);
	}

    /**
     * Create a set of elements in batch 
     */
	public static List<Column> createElementsWithDefaults(ColumnSet<?> group, String[] names, String[] types, String[] defaults) {
		List<Column> elements = new ArrayList<Column>();
		
		for(int i=0; i<names.length; i++) { 
            Column element = createElement(names[i], group, types[i]);
            element.setDefaultValue(defaults[i]);
			elements.add(element);		
		}
		
		return elements;
	}	

    /**
     * Create stored procedure parameter.
     */
    public static ProcedureParameter createParameter(String name, int direction, String type) {
        ProcedureParameter param = new ProcedureParameter();
        param.setName(name);
        switch (direction) {
        case SPParameter.IN:
        	param.setType(Type.In);
        	break;
        case SPParameter.INOUT:
        	param.setType(Type.InOut);
        	break;
        case SPParameter.OUT:
        	param.setType(Type.Out);
        	break;
        case SPParameter.RESULT_SET:
        	throw new AssertionError("should not directly create a resultset param"); //$NON-NLS-1$
        case SPParameter.RETURN_VALUE:
        	param.setType(Type.ReturnValue);
        	break;
        }
        param.setRuntimeType(type);
        return param;
    }

    /**
     * Create stored procedure.
     * @param name Name of procedure, must match model name
     * @param model Metadata object for the model
     * @param params List of FakeMetadataObject that are the parameters for the procedure
     * @param callableName Callable name of procedure, usually same as procedure name
     * @return Metadata object for stored procedure
     */
    public static Procedure createStoredProcedure(String name, Schema model, List<ProcedureParameter> params, String callableName) {
    	Procedure proc = new Procedure();
    	proc.setName(name);
    	if (params != null) {
    		int index = 1;
	    	for (ProcedureParameter procedureParameter : params) {
				procedureParameter.setProcedure(proc);
				procedureParameter.setPosition(index++);
			}
	    	proc.setParameters(params);
    	}
    	model.addProcedure(proc);
        return proc;    
    }
    
    /**
     * Create virtual sotred procedure.
     * @param name Name of stored query, must match model name
     * @param model Metadata object for the model
     * @param params List of FakeMetadataObject that are the parameters for the procedure
     * @param queryPlan Object representing query plan
     * @return Metadata object for stored procedure
     */
    public static Procedure createVirtualProcedure(String name, Schema model, List<ProcedureParameter> params, QueryNode queryPlan) {
    	Procedure proc = createStoredProcedure(name, model, params, null);
    	proc.setVirtual(true);
    	proc.setQueryPlan(queryPlan.getQuery());
        return proc;
    }
    
    /**
     * Create a result set.
     */
    public static ColumnSet<Procedure> createResultSet(String name, String[] colNames, String[] colTypes) {
    	ColumnSet<Procedure> rs = new ColumnSet<Procedure>();
    	rs.setName(name);
        for(Column column : createElements(rs, colNames, colTypes)) {
        	column.setParent(rs);
        }
        return rs;
    }

}
