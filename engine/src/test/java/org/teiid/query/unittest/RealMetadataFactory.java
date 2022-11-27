/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.unittest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.multisource.MultiSourceMetadataWrapper;
import org.teiid.metadata.*;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.Table.TriggerEvent;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.function.UDFSource;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.MaterializationMetadataRepository;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.FakeFunctionMetadataSource;
import org.teiid.query.parser.TestDDLParser;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.validator.ValidatorReport;

@SuppressWarnings("nls")
public class RealMetadataFactory {

    public static final SystemFunctionManager SFM = SystemMetadata.getInstance().getSystemFunctionManager();

    private static TransformationMetadata CACHED_EXAMPLE1 = example1();
    private static TransformationMetadata CACHED_BQT = exampleBQT();
    static TransformationMetadata CACHED_AGGREGATES = exampleAggregates();

    private RealMetadataFactory() { }

    public static TransformationMetadata exampleBQTCached() {
        return CACHED_BQT;
    }

    public static TransformationMetadata example1Cached() {
        return CACHED_EXAMPLE1;
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

        Schema gis = createPhysicalModel("GIS", metadataStore);
        Table colaMarkets = createPhysicalGroup("COLA_MARKETS", gis);
        createElement("MKT_ID", colaMarkets, "integer");
        createElement("NAME", colaMarkets, "string");
        createElement("SHAPE", colaMarkets, "geometry");
        createElement("GEOG_SHAPE", colaMarkets, "geography");

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

        Table bin = createPhysicalGroup("binary_test", lob); //$NON-NLS-1$

        // add direct query procedure
        ColumnSet<Procedure> nativeProcResults = createResultSet("bqt1.nativers", new String[] {"tuple"}, new String[] { DataTypeManager.DefaultDataTypes.OBJECT}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter nativeparam = createParameter("param", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        ProcedureParameter vardic = createParameter("varag", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.OBJECT); //$NON-NLS-1$
        vardic.setVarArg(true);
        Procedure nativeProc = createStoredProcedure("native", bqt1, Arrays.asList(nativeparam,vardic));  //$NON-NLS-1$ //$NON-NLS-2$
        nativeProc.setResultSet(nativeProcResults);

        createElements( library, new String[] { "CLOB_COLUMN", "BLOB_COLUMN", "KEY_EMULATOR" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                new String[] { DataTypeManager.DefaultDataTypes.CLOB, DataTypeManager.DefaultDataTypes.BLOB, DataTypeManager.DefaultDataTypes.INTEGER });

        createElements( bin, new String[] { "BIN_COL" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                new String[] { DataTypeManager.DefaultDataTypes.VARBINARY });

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

        ProcedureParameter rsp1 = createParameter("ret", ParameterInfo.RETURN_VALUE, DataTypeManager.DefaultDataTypes.INTEGER);  //$NON-NLS-1$
        ProcedureParameter rsp2 = createParameter("inkey", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        createVirtualProcedure("v_spTest9", bvqt, Arrays.asList(rsp1, rsp2), new QueryNode("ret = call pm4.spTest9(inkey);")); //$NON-NLS-1$ //$NON-NLS-2$

     // Add stored procedure
        Schema pm1 = createPhysicalModel("pm1", metadataStore); //$NON-NLS-1$
        ProcedureParameter rs1p1 = createParameter("intkey", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER);         //$NON-NLS-1$
        ColumnSet<Procedure> rs1 = createResultSet("rs1", new String[] { "IntKey", "StringKey" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Procedure spTest5 = createStoredProcedure("spTest5", pm1, Arrays.asList(rs1p1)); //$NON-NLS-1$ //$NON-NLS-2$
        spTest5.setResultSet(rs1);

        Schema pm2 = createPhysicalModel("pm2", metadataStore); //$NON-NLS-1$
        ProcedureParameter rs2p1 = createParameter("inkey", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        ProcedureParameter rs2p2 = createParameter("outkey", ParameterInfo.OUT, DataTypeManager.DefaultDataTypes.INTEGER);                 //$NON-NLS-1$
        ColumnSet<Procedure> rs2 = createResultSet("rs2", new String[] { "IntKey", "StringKey"}, new String[] { DataTypeManager.DefaultDataTypes.INTEGER , DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Procedure spTest8 = createStoredProcedure("spTest8", pm2, Arrays.asList(rs2p1, rs2p2)); //$NON-NLS-1$ //$NON-NLS-2$
        spTest8.setResultSet(rs2);

        ProcedureParameter rs2p2a = createParameter("outkey", ParameterInfo.OUT, DataTypeManager.DefaultDataTypes.INTEGER);                 //$NON-NLS-1$
        ColumnSet<Procedure> rs2a = createResultSet("rs2", new String[] { "IntKey", "StringKey"}, new String[] { DataTypeManager.DefaultDataTypes.INTEGER , DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Procedure spTest8a = createStoredProcedure("spTest8a", pm2, Arrays.asList(rs2p2a)); //$NON-NLS-1$ //$NON-NLS-2$
        spTest8a.setResultSet(rs2a);

        Schema pm4 = createPhysicalModel("pm4", metadataStore); //$NON-NLS-1$
        ProcedureParameter rs4p1 = createParameter("ret", ParameterInfo.RETURN_VALUE, DataTypeManager.DefaultDataTypes.INTEGER);  //$NON-NLS-1$
        ProcedureParameter rs4p2 = createParameter("inkey", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        createStoredProcedure("spTest9", pm4, Arrays.asList(rs4p1, rs4p2)); //$NON-NLS-1$ //$NON-NLS-2$

        Schema pm3 = createPhysicalModel("pm3", metadataStore); //$NON-NLS-1$
        ProcedureParameter rs3p1 = createParameter("inkey", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        ProcedureParameter rs3p2 = createParameter("outkey", ParameterInfo.INOUT, DataTypeManager.DefaultDataTypes.INTEGER);                 //$NON-NLS-1$
        ColumnSet<Procedure> rs3 = createResultSet("rs3", new String[] { "IntKey", "StringKey"}, new String[] { DataTypeManager.DefaultDataTypes.INTEGER , DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Procedure spTest11 = createStoredProcedure("spTest11", pm3, Arrays.asList(rs3p1, rs3p2)); //$NON-NLS-1$ //$NON-NLS-2$
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
                createParameter("x", ParameterInfo.OUT, DataTypeManager.DefaultDataTypes.INTEGER))); //$NON-NLS-1$ //$NON-NLS-2$

        ColumnSet<Procedure> vsprs7 = createResultSet("TEIIDSP7.vsprs1", new String[] { "StringKey" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter vsp7p1 = createParameter("p1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn7 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN declare integer x; x = exec spTest9(p1); declare integer y; exec spTest11(inkey=>x, outkey=>y) without return; select convert(x, string) || y; END"); //$NON-NLS-1$ //$NON-NLS-2$
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

        createStoredProcedure("sp_noreturn", pm4, Collections.EMPTY_LIST); //$NON-NLS-1$ //$NON-NLS-2$


        // this is for the source added function
        bqt1.addFunction(new FunctionMethod("reverse", "reverse", "misc", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                new FunctionParameter[] {new FunctionParameter("columnName", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$

         return metadataStore;
    }

    public static TransformationMetadata exampleBQT() {
        return createTransformationMetadata(exampleBQTStore(), "bqt");
    }

    public static TransformationMetadata createTransformationMetadata(MetadataStore metadataStore, String vdbName, FunctionTree... functionModels) {
        CompositeMetadataStore cms = null;
        if (metadataStore instanceof CompositeMetadataStore) {
            cms = (CompositeMetadataStore)metadataStore;
        } else {
            cms = new CompositeMetadataStore(metadataStore);
        }
        return createTransformationMetadata(cms, vdbName, null, functionModels);
    }

    public static TransformationMetadata createTransformationMetadata(CompositeMetadataStore store, String vdbName, Properties vdbProperties, FunctionTree... functionModels) {
        VDBMetaData vdbMetaData = new VDBMetaData();
        vdbMetaData.setName(vdbName); //$NON-NLS-1$
        vdbMetaData.setVersion(1);
        if (vdbProperties != null) {
            vdbMetaData.setProperties(vdbProperties);
        }
        List<FunctionTree> udfs = new ArrayList<FunctionTree>();
        udfs.addAll(Arrays.asList(functionModels));
        for (Schema schema : store.getSchemas().values()) {
            vdbMetaData.addModel(RealMetadataFactory.createModel(schema.getName(), schema.isPhysical()));
            if (!schema.getFunctions().isEmpty()) {
                udfs.add(new FunctionTree(schema.getName(), new UDFSource(schema.getFunctions().values()), true));
            }
            if (!schema.getProcedures().isEmpty()) {
                FunctionTree ft = FunctionTree.getFunctionProcedures(schema);
                if (ft != null) {
                    udfs.add(ft);
                }
            }
        }
        TransformationMetadata metadata = new TransformationMetadata(vdbMetaData, store, null, SFM.getSystemFunctions(), udfs);
        vdbMetaData.addAttachment(TransformationMetadata.class, metadata);
        vdbMetaData.addAttachment(QueryMetadataInterface.class, metadata);
        return metadata;
    }

    /**
     * Metadata for Materialized Views
     * @return
     * @since 4.2
     */
    public static TransformationMetadata exampleMaterializedView() {
        MetadataStore metadataStore = new MetadataStore();
        Schema virtModel = createVirtualModel("MatView", metadataStore); //$NON-NLS-1$
        Schema physModel = createPhysicalModel("MatTable", metadataStore); //$NON-NLS-1$
        Schema physModel_virtSrc = createPhysicalModel("MatSrc", metadataStore); //$NON-NLS-1$

        Table physTable = createPhysicalGroup("info", physModel); //$NON-NLS-1$
        createElements(physTable,
                                      new String[] { "e1", "e2", "e3", "value"}, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});

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
        createElements(physGroupStage1,
                                      new String[] { "e1" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING});

        Table physGroup_virtSrc = createPhysicalGroup("MatSrc", physModel_virtSrc); //$NON-NLS-1$
        createElements(physGroup_virtSrc,
                                      new String[] { "x" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING});

        Table status = createPhysicalGroup("Status", physModel_virtSrc); //$NON-NLS-1$
        createElements(status,
                                      new String[] { "VDBName", "VDBVersion", "SchemaName", "Name", "TargetSchemaName", "TargetName", "Valid", "LoadState", "Cardinality", "OnErrorAction", "Updated" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.TIMESTAMP});

        QueryNode virtTrans = new QueryNode("SELECT x as e1 FROM MatSrc.MatSrc");         //$NON-NLS-1$ //$NON-NLS-2$
        Table virtGroup = createVirtualGroup("MatView", virtModel, virtTrans); //$NON-NLS-1$
        createElements(virtGroup,
                                      new String[] { "e1" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING});

        virtGroup.setMaterialized(true);
        virtGroup.setMaterializedTable(physGroup);
        virtGroup.setMaterializedStageTable(physGroupStage);

        QueryNode virtTransManaged = new QueryNode("SELECT x as e1 FROM MatSrc.MatSrc");         //$NON-NLS-1$ //$NON-NLS-2$
        Table virtGroupManaged = createVirtualGroup("ManagedMatView", virtModel, virtTransManaged); //$NON-NLS-1$
        createElements(virtGroupManaged,
                                      new String[] { "e1" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING});

        virtGroupManaged.setMaterialized(true);
        virtGroupManaged.setMaterializedTable(physGroup);
        virtGroupManaged.setMaterializedStageTable(physGroupStage);
        virtGroupManaged.setProperty(MaterializationMetadataRepository.ALLOW_MATVIEW_MANAGEMENT, "true");
        virtGroupManaged.setProperty(MaterializationMetadataRepository.MATVIEW_STATUS_TABLE, "MatSrc.Status");
        virtGroupManaged.setProperty(MaterializationMetadataRepository.MATVIEW_SHARE_SCOPE, "FULL");
        virtGroupManaged.setProperty(MaterializationMetadataRepository.MATVIEW_OWNER_VDB_NAME, "X");
        virtGroupManaged.setProperty(MaterializationMetadataRepository.MATVIEW_OWNER_VDB_VERSION, "1");


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

        QueryNode vTrans2a = new QueryNode("SELECT x FROM matsrc");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vGroup2a = createVirtualGroup("VGroup2a", virtModel, vTrans2a); //$NON-NLS-1$
        KeyRecord fbi = new KeyRecord(KeyRecord.Type.Index);
        Column c = new Column();
        c.setParent(fbi);
        c.setName("upper(x)");
        c.setNameInSource("upper(x)");
        fbi.addColumn(c);
        vGroup2a.getFunctionBasedIndexes().add(fbi);
        vGroup2.setMaterialized(true);
        createElements(vGroup2a,
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

        QueryNode vTrans4 = new QueryNode("/*+ cache(ttl:10000) */ SELECT x FROM matsrc");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vGroup4 = createVirtualGroup("VGroup4", virtModel, vTrans4); //$NON-NLS-1$
        vGroup4.setProperty(MaterializationMetadataRepository.MATVIEW_TTL, "100"); //$NON-NLS-1$
        vGroup4.setMaterialized(true);
        createElements(vGroup4,
                                      new String[] { "x" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING});

        //non-covering index
        QueryNode vTrans5 = new QueryNode("SELECT x, 'z' || substring(x, 2) as y, 1 as z FROM matsrc "
                + "union all SELECT ifnull(x, ' ') || 'b', 'x' || substring(x, 2) as y, 1 as z FROM matsrc "
                + "union all SELECT ifnull(x, ' ') || 'c', 'y' || substring(x, 2) as y, 1 as z FROM matsrc "
                + "union all SELECT ifnull(x, ' ') || 'd', 'w' || substring(x, 2) as y, 1 as z FROM matsrc");
        Table vGroup5 = createVirtualGroup("VGroup5", virtModel, vTrans5); //$NON-NLS-1$
        vGroup5.setMaterialized(true);
        List<Column> vElements5 = createElements(vGroup5,
                                      new String[] { "x", "y", "z" }, //$NON-NLS-1$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER});

        KeyRecord pk = createKey(KeyRecord.Type.Primary, "pk", vGroup5, vElements5.subList(0, 1));
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
        createForeignKey("fk", vGroup7, vElements7.subList(0, 1), pk);

        Schema sp = createVirtualModel("sp", metadataStore); //$NON-NLS-1$
        ColumnSet<Procedure> rs = createResultSet("sp1.vsprs1", new String[] { "StringKey" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter param = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);
        QueryNode sp1qn = new QueryNode("/*+ cache */ CREATE VIRTUAL PROCEDURE BEGIN SELECT x as StringKey from matsrc where x = param1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp5 = createVirtualProcedure("sp1", sp, Arrays.asList(param), sp1qn); //$NON-NLS-1$
        vsp5.setResultSet(rs);

        return createTransformationMetadata(metadataStore, "");
    }

    public static MetadataStore example1Store() {
        MetadataStore metadataStore = new MetadataStore();
        // Create models
        Schema pm1 = createPhysicalModel("pm1", metadataStore); //$NON-NLS-1$

        pm1.addFunction(new FakeFunctionMetadataSource().getFunctionMethods().iterator().next());

        Schema pm2 = createPhysicalModel("pm2", metadataStore); //$NON-NLS-1$
        Schema pm3 = createPhysicalModel("pm3", metadataStore); //allows push of SELECT DISTINCT //$NON-NLS-1$
        Schema pm4 = createPhysicalModel("pm4", metadataStore); //all groups w/ access pattern(s) //$NON-NLS-1$
        Schema pm5 = createPhysicalModel("pm5", metadataStore); //all groups w/ access pattern(s); model supports join //$NON-NLS-1$
        Schema pm6 = createPhysicalModel("pm6", metadataStore); //model does not support where all //$NON-NLS-1$
        Schema vm1 = createVirtualModel("vm1", metadataStore);     //$NON-NLS-1$
        Schema vm2 = createVirtualModel("vm2", metadataStore);     //$NON-NLS-1$
        Schema xmltest = createVirtualModel("xmltest", metadataStore); //$NON-NLS-1$

        // Create physical groups
        Table pm1g1 = createPhysicalGroup("g1", pm1); //$NON-NLS-1$
        Table pm1g2 = createPhysicalGroup("g2", pm1); //$NON-NLS-1$
        Table pm1g3 = createPhysicalGroup("g3", pm1); //$NON-NLS-1$
        Table pm1g4 = createPhysicalGroup("g4", pm1); //$NON-NLS-1$
        Table pm1g5 = createPhysicalGroup("g5", pm1); //$NON-NLS-1$
        Table pm1g6 = createPhysicalGroup("g6", pm1); //$NON-NLS-1$
        Table pm1table = createPhysicalGroup("table1", pm1); //$NON-NLS-1$
        Table pm2g1 = createPhysicalGroup("g1", pm2); //$NON-NLS-1$
        Table pm2g2 = createPhysicalGroup("g2", pm2); //$NON-NLS-1$
        Table pm2g3 = createPhysicalGroup("g3", pm2); //$NON-NLS-1$
        Table pm3g1 = createPhysicalGroup("g1", pm3); //$NON-NLS-1$
        Table pm3g2 = createPhysicalGroup("g2", pm3); //$NON-NLS-1$
        Table pm4g1 = createPhysicalGroup("g1", pm4); //$NON-NLS-1$
        Table pm4g2 = createPhysicalGroup("g2", pm4); //$NON-NLS-1$
        Table pm5g1 = createPhysicalGroup("g1", pm5); //$NON-NLS-1$
        Table pm5g2 = createPhysicalGroup("g2", pm5); //$NON-NLS-1$
        Table pm5g3 = createPhysicalGroup("g3", pm5); //$NON-NLS-1$
        Table pm6g1 = createPhysicalGroup("g1", pm6); //$NON-NLS-1$

        // Create physical elements
        createElements(pm1g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm1g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm1g3,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List<Column> pm1g4e = createElements(pm1g4,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        pm1g4e.get(1).setSelectable(false);
        pm1g4e.get(3).setSelectable(false);
        List<Column> pm1g5e = createElements(pm1g5,
            new String[] { "e1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });
        pm1g5e.get(0).setSelectable(false);
        createElements(pm1g6,
            new String[] { "in", "in3" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        createElements(pm1table,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm2g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm2g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm2g3,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm3g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DATE, DataTypeManager.DefaultDataTypes.TIME, DataTypeManager.DefaultDataTypes.TIMESTAMP });
        createElements(pm3g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DATE, DataTypeManager.DefaultDataTypes.TIME, DataTypeManager.DefaultDataTypes.TIMESTAMP });
        List<Column> pm4g1e = createElements(pm4g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List<Column> pm4g2e = createElements(pm4g2,
            new String[] { "e1", "e2", "e3", "e4", "e5", "e6" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        List<Column> pm5g1e = createElements(pm5g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List<Column> pm5g2e = createElements(pm5g2,
            new String[] { "e1", "e2", "e3", "e4", "e5", "e6" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        createElements(pm5g3,
            new String[] { "e1", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.SHORT });
        createElements(pm6g1,
            new String[] { "e1", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });


        // Create access patterns - pm4
        List<Column> elements = new ArrayList<Column>(1);
        elements.add(pm4g1e.iterator().next());
        createAccessPattern("pm4.g1.ap1", pm4g1, elements); //e1 //$NON-NLS-1$
        elements = new ArrayList<Column>(2);
        Iterator<Column> iter = pm4g2e.iterator();
        elements.add(iter.next());
        elements.add(iter.next());
        createAccessPattern("pm4.g2.ap1", pm4g2, elements); //e1,e2 //$NON-NLS-1$
        elements = new ArrayList<Column>(1);
        elements.add(pm4g2e.get(4)); //"e5"
        createAccessPattern("pm4.g2.ap2", pm4g2, elements); //e5 //$NON-NLS-1$
        // Create access patterns - pm5
        elements = new ArrayList<Column>(1);
        elements.add(pm5g1e.iterator().next());
        createAccessPattern("pm5.g1.ap1", pm5g1, elements); //e1 //$NON-NLS-1$
        elements = new ArrayList<Column>(2);
        iter = pm5g2e.iterator();
        elements.add(iter.next());
        elements.add(iter.next());
        createAccessPattern("pm5.g2.ap1", pm5g2, elements); //e1,e2 //$NON-NLS-1$
        elements = new ArrayList<Column>(1);
        elements.add(pm5g2e.get(4)); //"e5"
        createAccessPattern("pm5.g2.ap2", pm5g2, elements); //e5 //$NON-NLS-1$

        // Create temp groups
        Table tm1g1 = createXmlStagingTable("doc4.tm1.g1", xmltest, new QueryNode("select null, null, null, null, null")); //$NON-NLS-1$

        // Create temp elements - the element "node1" is purposely named to be ambiguous with a document node named "node1"
        createElements(tm1g1,
            new String[] { "e1", "e2", "e3", "e4", "node1"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.STRING });

        // Create virtual groups
        QueryNode vm1g1n1 = new QueryNode("SELECT * FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g1 = createUpdatableVirtualGroup("g1", vm1, vm1g1n1); //$NON-NLS-1$

        QueryNode vm2g1n1 = new QueryNode("SELECT pm1.g1.* FROM pm1.g1, pm1.g2 where pm1.g1.e2 = pm1.g2.e2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm2g1 = createUpdatableVirtualGroup("g1", vm2, vm2g1n1); //$NON-NLS-1$

        QueryNode vm1g1n1_defect10711 = new QueryNode("SELECT * FROM vm1.g1 as X"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g1_defect10711 = createVirtualGroup("g1a", vm1, vm1g1n1_defect10711); //$NON-NLS-1$

        QueryNode vm1g1n1_defect12081 = new QueryNode("SELECT e1, upper(e1) as e1Upper FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g1_defect12081 = createVirtualGroup("g1b", vm1, vm1g1n1_defect12081); //$NON-NLS-1$

        QueryNode vm1g1n1c = new QueryNode("SELECT PARSETIMESTAMP(pm1.g1.e1, 'MMM dd yyyy hh:mm:ss') as e5, e2, e3, e4 FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g1c = createVirtualGroup("g1c", vm1, vm1g1n1c); //$NON-NLS-1$

        QueryNode vm1g2an1 = new QueryNode("SELECT * FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g2a = createVirtualGroup("g2a", vm1, vm1g2an1); //$NON-NLS-1$

        QueryNode vm1g2n1 = new QueryNode("SELECT pm1.g1.e1, pm1.g1.e2, pm1.g2.e3, pm1.g2.e4 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e1=pm1.g2.e1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g2 = createVirtualGroup("g2", vm1, vm1g2n1); //$NON-NLS-1$

        QueryNode vm1g4n1 = new QueryNode("SELECT e1 FROM pm1.g1 UNION ALL SELECT convert(e2, string) as x FROM pm1.g2 ORDER BY e1");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g4 = createVirtualGroup("g4", vm1, vm1g4n1); //$NON-NLS-1$

        QueryNode vm1g5n1 = new QueryNode("SELECT concat(e1, 'val'), e2 FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g5 = createVirtualGroup("g5", vm1, vm1g5n1); //$NON-NLS-1$

        QueryNode vm1g6n1 = new QueryNode("SELECT concat(e1, 'val') AS e, e2 FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g6 = createVirtualGroup("g6", vm1, vm1g6n1); //$NON-NLS-1$

        QueryNode vm1g7n1 = new QueryNode("SELECT concat(e1, e2) AS e, e2 FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g7 = createVirtualGroup("g7", vm1, vm1g7n1); //$NON-NLS-1$

        QueryNode vm1g8n1 = new QueryNode("SELECT concat(e1, 'val') AS e, e2 FROM pm1.g1 ORDER BY e"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g8 = createVirtualGroup("g8", vm1, vm1g8n1); //$NON-NLS-1$

        QueryNode vm1g9n1 = new QueryNode("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1, pm4.g1 WHERE pm1.g1.e1 = pm4.g1.e1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g9 = createVirtualGroup("g9", vm1, vm1g9n1); //$NON-NLS-1$

        QueryNode vm1g10n1 = new QueryNode("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1, pm4.g2 WHERE pm1.g1.e1 = pm4.g2.e1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g10 = createVirtualGroup("g10", vm1, vm1g10n1); //$NON-NLS-1$

        QueryNode vm1g11n1 = new QueryNode("SELECT * FROM pm4.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g11 = createVirtualGroup("g11", vm1, vm1g11n1); //$NON-NLS-1$

        QueryNode vm1g12n1 = new QueryNode("SELECT DISTINCT * FROM pm3.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g12 = createVirtualGroup("g12", vm1, vm1g12n1); //$NON-NLS-1$

        QueryNode vm1g13n1 = new QueryNode("SELECT DISTINCT * FROM pm3.g1 ORDER BY e1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g13 = createVirtualGroup("g13", vm1, vm1g13n1); //$NON-NLS-1$

        QueryNode vm1g14n1 = new QueryNode("SELECT * FROM pm3.g1 ORDER BY e1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g14 = createVirtualGroup("g14", vm1, vm1g14n1); //$NON-NLS-1$

        QueryNode vm1g15n1 = new QueryNode("SELECT e1, concat(e1, convert(e2, string)) AS x FROM pm3.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g15 = createVirtualGroup("g15", vm1, vm1g15n1); //$NON-NLS-1$

        QueryNode vm1g16n1 = new QueryNode("SELECT concat(e1, 'val') AS e, e2 FROM pm3.g1 ORDER BY e"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g16 = createVirtualGroup("g16", vm1, vm1g16n1); //$NON-NLS-1$

        QueryNode vm1g17n1 = new QueryNode("SELECT pm3.g1.e1, pm3.g1.e2 FROM pm3.g1 UNION ALL SELECT pm3.g2.e1, pm3.g2.e2 FROM pm3.g2 ORDER BY e2");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g17 = createVirtualGroup("g17", vm1, vm1g17n1); //$NON-NLS-1$

        QueryNode vm1g18n1 = new QueryNode("SELECT (e4 * cast(100.0 as double)) as x FROM pm1.g1");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g18 = createVirtualGroup("g18", vm1, vm1g18n1); //$NON-NLS-1$

        // Transformations with subqueries and correlated subqueries
        QueryNode vm1g19n1 = new QueryNode("Select * from vm1.g4 where not (e1 in (select e1 FROM vm1.g1 WHERE vm1.g4.e1 = e1))");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g19 = createVirtualGroup("g19", vm1, vm1g19n1); //$NON-NLS-1$

        QueryNode vm1g20n1 = new QueryNode("Select * from vm1.g1 where exists (select e1 FROM vm1.g2 WHERE vm1.g1.e1 = e1)");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g20 = createVirtualGroup("g20", vm1, vm1g20n1); //$NON-NLS-1$

        QueryNode vm1g21n1 = new QueryNode("Select * from pm1.g1 where exists (select e1 FROM pm2.g1 WHERE pm1.g1.e1 = e1)");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g21 = createVirtualGroup("g21", vm1, vm1g21n1); //$NON-NLS-1$

        QueryNode vm1g22n1 = new QueryNode("Select e1, e2, e3, e4, (select e4 FROM vm1.g21 WHERE vm1.g20.e4 = e4 and e4 = 7.0) as E5 from vm1.g20");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g22 = createVirtualGroup("g22", vm1, vm1g22n1); //$NON-NLS-1$

        QueryNode vm1g23n1 = new QueryNode("Select e1, e2, e3, e4, (select e4 FROM vm1.g21 WHERE vm1.g20.e4 = 7.0 and e4 = 7.0) as E5 from vm1.g20");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g23 = createVirtualGroup("g23", vm1, vm1g23n1); //$NON-NLS-1$

        QueryNode vm1g24n1 = new QueryNode("Select * from vm1.g20 where exists (select * FROM vm1.g21 WHERE vm1.g20.e4 = E4)");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g24 = createVirtualGroup("g24", vm1, vm1g24n1); //$NON-NLS-1$

        QueryNode vm1g25n1 = new QueryNode("Select e1, e2, e3, e4, (select e4 FROM pm1.g2 WHERE e1 = 'b') as E5 from pm1.g1");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g25 = createVirtualGroup("g25", vm1, vm1g25n1); //$NON-NLS-1$

        QueryNode vm1g26n1 = new QueryNode("Select e1, e2, e3, e4, (select e4 FROM pm1.g2 WHERE e4 = pm1.g1.e4 and e1 = 'b') as E5 from pm1.g1");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g26 = createVirtualGroup("g26", vm1, vm1g26n1); //$NON-NLS-1$

        //defect 10976
//        QueryNode vm1g27n1 = new QueryNode("vm1.g27", "SELECT DISTINCT x as a, lower(e1) as x FROM vm1.g28");         //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode vm1g27n1 = new QueryNode("SELECT upper(e1) as x, e1 FROM pm1.g1");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g27 = createVirtualGroup("g27", vm1, vm1g27n1); //$NON-NLS-1$

        QueryNode vm1g28n1 = new QueryNode("SELECT DISTINCT x as a, lower(e1) as x FROM vm1.g27");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g28 = createVirtualGroup("g28", vm1, vm1g28n1); //$NON-NLS-1$

        QueryNode vm1g29n1 = new QueryNode("SELECT DISTINCT x, lower(e1) FROM vm1.g27");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g29 = createVirtualGroup("g29", vm1, vm1g29n1); //$NON-NLS-1$

        QueryNode vm1g30n1 = new QueryNode("SELECT DISTINCT e1 as x, e1 as y FROM pm1.g1");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g30 = createVirtualGroup("g30", vm1, vm1g30n1); //$NON-NLS-1$

        QueryNode vm1g31n1 = new QueryNode("SELECT e1 as x, e1 as y FROM pm1.g1 ORDER BY x");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g31 = createVirtualGroup("g31", vm1, vm1g31n1); //$NON-NLS-1$

        QueryNode vm1g32n1 = new QueryNode("SELECT DISTINCT e1 as x, e1 as y FROM pm1.g1 ORDER BY x");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g32 = createVirtualGroup("g32", vm1, vm1g32n1); //$NON-NLS-1$

        QueryNode vm1g33n1 = new QueryNode("SELECT e2 FROM pm1.g1 WHERE 2 = e2");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g33 = createVirtualGroup("g33", vm1, vm1g33n1); //$NON-NLS-1$

        QueryNode vm1g34n1 = new QueryNode("SELECT e1 as e1_, e2 as e2_ FROM pm1.g1 UNION ALL SELECT e1 as e1_, e2 as e2_ FROM pm2.g1");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g34 = createVirtualGroup("g34", vm1, vm1g34n1); //$NON-NLS-1$

        QueryNode vm1g36n1 = new QueryNode("SELECT pm1.g1.e1 as ve1, pm1.g2.e1 as ve2 FROM pm1.g1 LEFT OUTER JOIN /* optional */ pm1.g2 on pm1.g1.e1 = pm1.g2.e1");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g36 = createVirtualGroup("g36", vm1, vm1g36n1); //$NON-NLS-1$

        QueryNode vm1g37n1 = new QueryNode("SELECT * from pm4.g1");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g37 = createVirtualGroup("g37", vm1, vm1g37n1); //$NON-NLS-1$
        vm1g37.setSupportsUpdate(true);
        vm1g37.setDeletePlan("for each row begin atomic end");

        QueryNode vm1g38n1 = new QueryNode("SELECT a.e1, b.e2 from pm1.g1 as a, pm6.g1 as b where a.e1=b.e1");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g38 = createVirtualGroup("g38", vm1, vm1g38n1); //$NON-NLS-1$

        // Create virtual groups
        QueryNode vm1g39n1 = new QueryNode("SELECT * FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g39 = createUpdatableVirtualGroup("g39", vm1, vm1g39n1, null); //$NON-NLS-1$
        // Create virtual elements
        createElements(vm1g39,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm2g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g1_defect10711,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g1_defect12081,
            new String[] { "e1", "e1Upper" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        createElements(vm1g1c,
            new String[] { "e5", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.TIMESTAMP, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g2a,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g4,
            new String[] { "e1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });
        createElements(vm1g5,
            new String[] { "expr", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        createElements(vm1g6,
            new String[] { "e", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        createElements(vm1g7,
            new String[] { "e", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        createElements(vm1g8,
            new String[] { "e", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        createElements(vm1g9,
            new String[] { "e1", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        createElements(vm1g10,
            new String[] { "e1", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        createElements(vm1g11,
            new String[] { "e1", "e2", "e3", "e4", "e5", "e6"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        createElements(vm1g12,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DATE, DataTypeManager.DefaultDataTypes.TIME, DataTypeManager.DefaultDataTypes.TIMESTAMP });
        createElements(vm1g13,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DATE, DataTypeManager.DefaultDataTypes.TIME, DataTypeManager.DefaultDataTypes.TIMESTAMP });
        createElements(vm1g14,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DATE, DataTypeManager.DefaultDataTypes.TIME, DataTypeManager.DefaultDataTypes.TIMESTAMP });
        createElements(vm1g15,
            new String[] { "e1", "x" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        createElements(vm1g16,
            new String[] { "e", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DATE });
        createElements(vm1g17,
            new String[] { "e1", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DATE });
        createElements(vm1g18,
            new String[] { "x" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g19,
            new String[] { "e1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });
        createElements(vm1g20,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g21,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g22,
            new String[] { "e1", "e2", "e3", "e4", "e5" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g23,
            new String[] { "e1", "e2", "e3", "e4", "e5" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g24,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g25,
            new String[] { "e1", "e2", "e3", "e4", "e5" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g26,
            new String[] { "e1", "e2", "e3", "e4", "e5" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g27,
            new String[] { "x", "e1"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        createElements(vm1g28,
            new String[] { "a", "x"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        createElements(vm1g29,
            new String[] { "x", "expr"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        createElements(vm1g30,
            new String[] { "x", "y"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        createElements(vm1g31,
            new String[] { "x", "y"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        createElements(vm1g32,
            new String[] { "x", "y"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        createElements(vm1g33,
            new String[] { "e2"}, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER });
        createElements(vm1g34,
            new String[] { "e1_", "e2_"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        createElements(vm1g36,
             new String[] { "ve1", "ve2" }, //$NON-NLS-1$ //$NON-NLS-2$
             new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        List<Column> vm1g37e = createElements(vm1g37,
              new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
              new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g38,
              new String[] { "e1", "e2" }, //$NON-NLS-1$ //$NON-NLS-2$
              new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });

        //Create access patterns on vm1.g37
        elements = new ArrayList<Column>(1);
        elements.add(vm1g37e.iterator().next());
        createAccessPattern("vm1.g37.ap1", vm1g37, elements); //e1 //$NON-NLS-1$

        // Create mapping classes for xmltest.doc5
        QueryNode mc1n1 = new QueryNode("SELECT e1 FROM pm1.g1 UNION ALL SELECT e1 FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1mc1 = createVirtualGroup("mc1", xmltest, mc1n1); //$NON-NLS-1$
        createElements(vm1mc1,
            new String[] { "e1" }, //$NON-NLS-1$
            new String[] { DataTypeManager.DefaultDataTypes.STRING });

        //XML STUFF =============================================

        // Procedures and stored queries
        ColumnSet<Procedure> rs1 = createResultSet("pm1.rs1", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        QueryNode sq1n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq1 = createVirtualProcedure("sq1", pm1, null, sq1n1); //$NON-NLS-1$
        sq1.setResultSet(rs1);

        ColumnSet<Procedure> rs2 = createResultSet("ret", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter rs2p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sq2.in; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq2 = createVirtualProcedure("sq2", pm1, Arrays.asList(rs2p2), sq2n1);  //$NON-NLS-1$
        sq2.setResultSet(rs2);

        ColumnSet<Procedure> rs5 = createResultSet("pm1.r5", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter rs5p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        ProcedureParameter rs5p3 = createParameter("in2", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER);  //$NON-NLS-1$
        QueryNode sq3n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sq3.in UNION ALL SELECT e1, e2 FROM pm1.g1 WHERE e2=pm1.sq3.in2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq3 = createVirtualProcedure("sq3", pm1, Arrays.asList(rs5p2, rs5p3), sq3n1);  //$NON-NLS-1$
        sq3.setResultSet(rs5);

        //For defect 8211 - this stored query has two input params, no return param, and
        //the input params are PURPOSELY numbered with indices "1" and "3" - see defect 8211
        ColumnSet<Procedure> rs5a = createResultSet("pm1.r5a", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter rs5p1a = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        ProcedureParameter rs5p2a = createParameter("in2", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER);  //$NON-NLS-1$
        QueryNode sq3n1a = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sq3a.in UNION ALL SELECT e1, e2 FROM pm1.g1 WHERE e2=pm1.sq3a.in2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq3a = createVirtualProcedure("sq3a", pm1, Arrays.asList(rs5p1a, rs5p2a), sq3n1a);  //$NON-NLS-1$
        sq3a.setResultSet(rs5a);
        //Case 3281 - create procedures with optional parameter(s)

        //make "in2" parameter optional, make "in3" required but with a default value
        ColumnSet<Procedure> rs5b = createResultSet("pm1.r5b", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter rs5p2b = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        ProcedureParameter rs5p3b = createParameter("in2", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER);  //$NON-NLS-1$
        ProcedureParameter rs5p4b = createParameter("in3", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        rs5p3b.setNullType(NullType.Nullable);
        rs5p4b.setDefaultValue("YYZ"); //$NON-NLS-1$
        QueryNode sq3n1b = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sq3b.in UNION ALL SELECT e1, e2 FROM pm1.g1 WHERE e2=pm1.sq3b.in2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq3b = createVirtualProcedure("sq3b", pm1, Arrays.asList(rs5p2b, rs5p3b, rs5p4b), sq3n1b);  //$NON-NLS-1$
        sq3b.setResultSet(rs5b);

        //Make parameters of all different types, all with appropriate default values
        //Make some parameters required, some optional
        //Also, fully-qualify the param names
        ColumnSet<Procedure> rsDefaults = createResultSet("pm1.rDefaults", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter rsDefaultsParameterString = createParameter("inString", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        //rsDefaultsParameterString.setNullType(NullType.Nullable);
        rsDefaultsParameterString.setDefaultValue(new String("x")); //$NON-NLS-1$
        ProcedureParameter rsParameterBigDecimal = createParameter("inBigDecimal", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.BIG_DECIMAL);  //$NON-NLS-1$
        rsParameterBigDecimal.setNullType(NullType.Nullable);
        rsParameterBigDecimal.setDefaultValue(new String("13.0")); //$NON-NLS-1$
        ProcedureParameter rsParameterBigInteger = createParameter("inBigInteger", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.BIG_INTEGER);  //$NON-NLS-1$
        rsParameterBigInteger.setNullType(NullType.Nullable);
        rsParameterBigInteger.setDefaultValue(new String("13")); //$NON-NLS-1$
        ProcedureParameter rsParameterBoolean = createParameter("inBoolean", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.BOOLEAN);  //$NON-NLS-1$
        rsParameterBoolean.setNullType(NullType.Nullable);
        rsParameterBoolean.setDefaultValue(new String("True")); //$NON-NLS-1$
        ProcedureParameter rsParameterByte = createParameter("inByte", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.BYTE);  //$NON-NLS-1$
        rsParameterByte.setNullType(NullType.Nullable);
        rsParameterByte.setDefaultValue(new String("1")); //$NON-NLS-1$
        ProcedureParameter rsParameterChar = createParameter("inChar", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.CHAR);  //$NON-NLS-1$
        rsParameterChar.setNullType(NullType.Nullable);
        rsParameterChar.setDefaultValue(new String("q")); //$NON-NLS-1$
        ProcedureParameter rsParameterDate = createParameter("inDate", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.DATE);  //$NON-NLS-1$
        rsParameterDate.setNullType(NullType.Nullable);
        rsParameterDate.setDefaultValue(new String("2003-03-20")); //$NON-NLS-1$
        ProcedureParameter rsParameterDouble = createParameter("inDouble", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.DOUBLE);  //$NON-NLS-1$
        rsParameterDouble.setNullType(NullType.Nullable);
        rsParameterDouble.setDefaultValue(new String("13.0")); //$NON-NLS-1$
        ProcedureParameter rsParameterFloat = createParameter("inFloat", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.FLOAT);  //$NON-NLS-1$
        rsParameterFloat.setNullType(NullType.Nullable);
        rsParameterFloat.setDefaultValue(new String("13")); //$NON-NLS-1$
        ProcedureParameter rsParameterInteger = createParameter("inInteger", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER);  //$NON-NLS-1$
        rsParameterInteger.setNullType(NullType.Nullable);
        rsParameterInteger.setDefaultValue(new String("13")); //$NON-NLS-1$
        ProcedureParameter rsParameterLong = createParameter("inLong", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.LONG);  //$NON-NLS-1$
        rsParameterLong.setNullType(NullType.Nullable);
        rsParameterLong.setDefaultValue(new String("13")); //$NON-NLS-1$
        ProcedureParameter rsParameterShort = createParameter("inShort", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.SHORT);  //$NON-NLS-1$
        rsParameterShort.setNullType(NullType.Nullable);
        rsParameterShort.setDefaultValue(new String("13")); //$NON-NLS-1$
        ProcedureParameter rsParameterTimestamp = createParameter("inTimestamp", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.TIMESTAMP);  //$NON-NLS-1$
        rsParameterTimestamp.setNullType(NullType.Nullable);
        rsParameterTimestamp.setDefaultValue(new String("2003-03-20 21:26:00.000000")); //$NON-NLS-1$
        ProcedureParameter rsParameterTime = createParameter("inTime", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.TIME);  //$NON-NLS-1$
        rsParameterTime.setNullType(NullType.Nullable);
        rsParameterTime.setDefaultValue(new String("21:26:00")); //$NON-NLS-1$
        QueryNode sqDefaultsNode = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1 WHERE e1=pm1.sqDefaults.inString UNION ALL SELECT e1, e2 FROM pm1.g1 WHERE e2=pm1.sqDefaults.inInteger; END"); //$NON-NLS-1$ //$NON-NLS-2$

        Procedure sqDefaults = createVirtualProcedure("sqDefaults", pm1, //$NON-NLS-1$
                                                          Arrays.asList(
                                                              rsDefaultsParameterString,
                                                              rsParameterBigDecimal,
                                                              rsParameterBigInteger,
                                                              rsParameterBoolean,
                                                              rsParameterByte,
                                                              rsParameterChar,
                                                              rsParameterDate,
                                                              rsParameterDouble,
                                                              rsParameterFloat,
                                                              rsParameterInteger,
                                                              rsParameterLong,
                                                              rsParameterShort,
                                                              rsParameterTimestamp,
                                                              rsParameterTime
                                                          ), sqDefaultsNode);
        sqDefaults.setResultSet(rsDefaults);

        createResultSet("pm1.rBadDefault", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter paramBadDefaultIn = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER);  //$NON-NLS-1$
        paramBadDefaultIn.setNullType(NullType.Nullable);
        paramBadDefaultIn.setDefaultValue("Clearly Not An Integer"); //$NON-NLS-1$
        QueryNode sqnBadDefault = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1 WHERE e2=pm1.sqBadDefault.in; END"); //$NON-NLS-1$ //$NON-NLS-2$
        createVirtualProcedure("sqBadDefault", pm1, Arrays.asList(paramBadDefaultIn), sqnBadDefault);  //$NON-NLS-1$

        //end case 3281

        ColumnSet<Procedure> nativeProcResults = createResultSet("pm1.nativers", new String[] {"tuple"}, new String[] { DataTypeManager.DefaultDataTypes.OBJECT}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter nativeparam = createParameter("param", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        ProcedureParameter vardic = createParameter("varag", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.OBJECT); //$NON-NLS-1$
        vardic.setVarArg(true);
        Procedure nativeProc = createStoredProcedure("native", pm1, Arrays.asList(nativeparam,vardic));  //$NON-NLS-1$ //$NON-NLS-2$
        nativeProc.setResultSet(nativeProcResults);

        ColumnSet<Procedure> rs3 = createResultSet("pm1.rs3", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Procedure sp1 = createStoredProcedure("sp1", pm1, null);  //$NON-NLS-1$ //$NON-NLS-2$
        sp1.setResultSet(rs3);

        ColumnSet<Procedure> rs4 = createResultSet("pm1.rs4", new String[] { "e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode sqsp1n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM (EXEC pm1.sp1()) as x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sqsp1 = createVirtualProcedure("sqsp1", pm1, null, sqsp1n1);  //$NON-NLS-1$
        sqsp1.setResultSet(rs4);

        ColumnSet<Procedure> rs6 = createResultSet("pm1.rs6", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        QueryNode sq4n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN EXEC pm1.sq1(); END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq4 = createVirtualProcedure("sq4", pm1, null, sq4n1);  //$NON-NLS-1$
        sq4.setResultSet(rs6);

        ColumnSet<Procedure> rs7 = createResultSet("pm1.rs7", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter rs7p2 = createParameter("in1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        QueryNode sq5n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN EXEC pm1.sq2(pm1.sq5.in1); END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq5 = createVirtualProcedure("sq5", pm1, Arrays.asList( rs7p2 ), sq5n1);  //$NON-NLS-1$
        sq5.setResultSet(rs7);

        ColumnSet<Procedure> rs8 = createResultSet("pm1.rs8", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        QueryNode sq6n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN EXEC pm1.sq2(\'1\'); END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq6 = createVirtualProcedure("sq6", pm1, null, sq6n1);  //$NON-NLS-1$
        sq6.setResultSet(rs8);

        ColumnSet<Procedure> rs9 = createResultSet("pm1.rs9", new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode sq7n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM (EXEC pm1.sq1()) as x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq7 = createVirtualProcedure("sq7", pm1, null, sq7n1);  //$NON-NLS-1$
        sq7.setResultSet(rs9);

        ColumnSet<Procedure> rs10 = createResultSet("pm1.rs10", new String[] { "e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter rs10p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        QueryNode sq8n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM (EXEC pm1.sq1()) as x WHERE x.e1=pm1.sq8.in; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq8 = createVirtualProcedure("sq8", pm1, Arrays.asList( rs10p2 ), sq8n1);  //$NON-NLS-1$
        sq8.setResultSet(rs10);

        ColumnSet<Procedure> rs11 = createResultSet("pm1.rs11", new String[] { "e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter rs11p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        QueryNode sq9n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM (EXEC pm1.sq2(pm1.sq9.in)) as x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq9 = createVirtualProcedure("sq9", pm1, Arrays.asList( rs11p2 ), sq9n1);  //$NON-NLS-1$
        sq9.setResultSet(rs11);

        ColumnSet<Procedure> rs12 = createResultSet("pm1.rs12", new String[] { "e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING}); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter rs12p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        ProcedureParameter rs12p3 = createParameter("in2", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER);  //$NON-NLS-1$
        QueryNode sq10n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM (EXEC pm1.sq2(pm1.sq10.in)) as x where e2=pm1.sq10.in2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq10 = createVirtualProcedure("sq10", pm1, Arrays.asList( rs12p2,  rs12p3), sq10n1);  //$NON-NLS-1$
        sq10.setResultSet(rs12);

        ColumnSet<Procedure> rs13 = createResultSet("pm1.rs13", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter rs13p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER);  //$NON-NLS-1$
        Procedure sp2 = createStoredProcedure("sp2", pm1, Arrays.asList( rs13p2 ));  //$NON-NLS-1$ //$NON-NLS-2$
        sp2.setResultSet(rs13);

        ColumnSet<Procedure> rs14 = createResultSet("pm1.rs14", new String[] { "e1"}, new String[] { DataTypeManager.DefaultDataTypes.STRING}); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter rs14p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        ProcedureParameter rs14p3 = createParameter("in2", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER);  //$NON-NLS-1$
        QueryNode sq11n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM (EXEC pm1.sp2(?)) as x where e2=pm1.sq11.in; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq11 = createVirtualProcedure("sq11", pm1, Arrays.asList( rs14p2,  rs14p3), sq11n1);  //$NON-NLS-1$
        sq11.setResultSet(rs14);

        ColumnSet<Procedure> rs15 = createResultSet("pm1.rs15", new String[] { "count" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter rs15p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        ProcedureParameter rs15p3 = createParameter("in2", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER);  //$NON-NLS-1$
        QueryNode sq12n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN INSERT INTO pm1.g1 ( e1, e2 ) VALUES( pm1.sq12.in, pm1.sq12.in2 ); END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq12 = createVirtualProcedure("sq12", pm1, Arrays.asList( rs15p2, rs15p3 ), sq12n1);  //$NON-NLS-1$
        sq12.setResultSet(rs15);

        ColumnSet<Procedure> rs16 = createResultSet("pm1.rs16", new String[] { "count" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter rs16p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        QueryNode sq13n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN INSERT INTO pm1.g1 ( e1, e2 ) VALUES( pm1.sq13.in, 2 ); END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq13 = createVirtualProcedure("sq13", pm1, Arrays.asList( rs16p2 ), sq13n1);  //$NON-NLS-1$
        sq13.setResultSet(rs16);

        ColumnSet<Procedure> rs17 = createResultSet("pm1.rs17", new String[] { "count" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter rs17p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        ProcedureParameter rs17p3 = createParameter("in2", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER);  //$NON-NLS-1$
        QueryNode sq14n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN UPDATE pm1.g1 SET e1 = pm1.sq14.in WHERE e2 = pm1.sq14.in2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq14 = createVirtualProcedure("sq14", pm1, Arrays.asList( rs17p2, rs17p3 ), sq14n1);  //$NON-NLS-1$
        sq14.setResultSet(rs17);

        ColumnSet<Procedure> rs18 = createResultSet("pm1.rs17", new String[] { "count" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter rs18p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        ProcedureParameter rs18p3 = createParameter("in2", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER);  //$NON-NLS-1$
        QueryNode sq15n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DELETE FROM pm1.g1 WHERE e1 = pm1.sq15.in AND e2 = pm1.sq15.in2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq15 = createVirtualProcedure("sq15", pm1, Arrays.asList( rs18p2, rs18p3 ), sq15n1);  //$NON-NLS-1$
        sq15.setResultSet(rs18);

        QueryNode sq16n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN INSERT INTO pm1.g1 ( e1, e2 ) VALUES( 1, 2 ); END"); //$NON-NLS-1$ //$NON-NLS-2$
        createVirtualProcedure("sq16", pm1, null, sq16n1);  //$NON-NLS-1$

        ColumnSet<Procedure> rs19 = createResultSet("pm1.rs19", new String[] { "xml" }, new String[] { DataTypeManager.DefaultDataTypes.XML }); //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode sq17n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT * FROM xmltest.doc1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq17 = createVirtualProcedure("sq17", pm1, null, sq17n1);  //$NON-NLS-1$
        sq17.setResultSet(rs19);

        createStoredProcedure("sp3", pm1, null);  //$NON-NLS-1$ //$NON-NLS-2$

        ColumnSet<Procedure> rs20 = createResultSet("pm1.rs20", new String[] { "xml" }, new String[] { DataTypeManager.DefaultDataTypes.XML }); //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode sq18n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT * FROM xmltest.doc1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq18 = createVirtualProcedure("sq18", pm1, null, sq18n1); //$NON-NLS-1$
        sq18.setResultSet(rs20);

        ColumnSet<Procedure> rs21 = createResultSet("pm1.rs21", new String[] { "xml" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter sq19p2 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        QueryNode sq19n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT * FROM xmltest.doc4 WHERE root.node1 = param1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq19 = createVirtualProcedure("sq19", pm1, Arrays.asList( sq19p2 ), sq19n1); //$NON-NLS-1$
        sq19.setResultSet(rs21);

        ColumnSet<Procedure> rs22 = createResultSet("pm1.rs13", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter rs22p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.BIG_INTEGER);  //$NON-NLS-1$
        Procedure sp4 = createStoredProcedure("sp4", pm1, Arrays.asList( rs22p2 ));  //$NON-NLS-1$ //$NON-NLS-2$
        sp4.setResultSet(rs22);

        // no params or result set at all
        createStoredProcedure("sp5", pm1, new ArrayList<ProcedureParameter>());  //$NON-NLS-1$ //$NON-NLS-2$

        //virtual stored procedures
        ColumnSet<Procedure> vsprs1 = vsprs1(); //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode vspqn1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN x=mycursor.e2; IF(x = 15) BEGIN BREAK; END END SELECT e1 FROM pm1.g1 where pm1.g1.e2 = x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp1 = createVirtualProcedure("vsp1", pm1, null, vspqn1); //$NON-NLS-1$\
        vsp1.setResultSet(vsprs1);

        QueryNode vspqn2 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN x=mycursor.e2; END SELECT e1 FROM pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp2 = createVirtualProcedure("vsp2", pm1, null, vspqn2); //$NON-NLS-1$
        vsp2.setResultSet(vsprs1());

        QueryNode vspqn3 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN x=mycursor.e2; END SELECT e1 FROM pm1.g1 WHERE x=e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp3 = createVirtualProcedure("vsp3", pm1, null, vspqn3); //$NON-NLS-1$
        vsp3.setResultSet(vsprs1());

        QueryNode vspqn4 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN IF(mycursor.e2 > 10) BEGIN BREAK; END x=mycursor.e2; END SELECT e1 FROM pm1.g1 WHERE x=e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp4 = createVirtualProcedure("vsp4", pm1, null, vspqn4); //$NON-NLS-1$
        vsp4.setResultSet(vsprs1());

        QueryNode vspqn5 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN IF(mycursor.e2 > 10) BEGIN CONTINUE; END x=mycursor.e2; END SELECT e1 FROM pm1.g1 WHERE x=e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp5 = createVirtualProcedure("vsp5", pm1, null, vspqn5); //$NON-NLS-1$
        vsp5.setResultSet(vsprs1());

        QueryNode vspqn6 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; x=0; WHILE (x < 15) BEGIN x=x+1; END SELECT e1 FROM pm1.g1 WHERE x=e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp6 = createVirtualProcedure("vsp6", pm1, null, vspqn6); //$NON-NLS-1$
        vsp6.setResultSet(vsprs1());

        ProcedureParameter vspp2 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn7 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; x=0; WHILE (x < 12) BEGIN x=x+pm1.vsp7.param1; END SELECT e1 FROM pm1.g1 WHERE x=e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp7 = createVirtualProcedure("vsp7", pm1, Arrays.asList( vspp2 ), vspqn7); //$NON-NLS-1$
        vsp7.setResultSet(vsprs1());

        ProcedureParameter vspp8 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn8 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; x=0; WHILE (x < 12) BEGIN x=x+pm1.vsp8.param1; END SELECT e1 FROM pm1.g1 WHERE e2 >= param1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp8 = createVirtualProcedure("vsp8", pm1, Arrays.asList( vspp8 ), vspqn8); //$NON-NLS-1$
        vsp8.setResultSet(vsprs1());

        ProcedureParameter vspp9 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn9 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; x=0; WHILE (x < param1) BEGIN x=x+pm1.vsp9.param1; END SELECT e1 FROM pm1.g1 WHERE e2 >= param1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp9 = createVirtualProcedure("vsp9", pm1, Arrays.asList( vspp9 ), vspqn9); //$NON-NLS-1$
        vsp9.setResultSet(vsprs1());

        ProcedureParameter vspp3 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn10 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1 WHERE e2=param1) AS mycursor BEGIN x=mycursor.e2; END END"); //$NON-NLS-1$ //$NON-NLS-2$
        createVirtualProcedure("vsp10", pm1, Arrays.asList( vspp3 ), vspqn10); //$NON-NLS-1$

        //invalid
        QueryNode vspqn11 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN LOOP ON (SELECT e1 FROM pm1.g1) AS mycursor BEGIN END END SELECT e1 FROM pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp11 = createVirtualProcedure("vsp11", pm1, null, vspqn11); //$NON-NLS-1$
        vsp11.setResultSet(vsprs1());

        //invalid
        QueryNode vspqn12 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN END x=mycursor.e2; SELECT e1 FROM pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp12 = createVirtualProcedure("vsp12", pm1, null, vspqn12); //$NON-NLS-1$
        vsp12.setResultSet(vsprs1());

        ColumnSet<Procedure> vsprs2 = vspp4(); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        QueryNode vspqn13 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; LOOP ON (SELECT e1 FROM pm1.g1) AS mycursor BEGIN x=mycursor.e1; END SELECT x, 5; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp13 = createVirtualProcedure("vsp13", pm1, null, vspqn13); //$NON-NLS-1$
        vsp13.setResultSet(vsprs2);

        QueryNode vspqn14 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 INTO #temptable FROM pm1.g1; SELECT e1 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp14 = createVirtualProcedure("vsp14", pm1, null, vspqn14); //$NON-NLS-1$
        vsp14.setResultSet(vsprs1());

        QueryNode vspqn15 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 INTO #temptable FROM pm1.g1; SELECT #temptable.e1 FROM #temptable, pm1.g2 WHERE #temptable.e2 = pm1.g2.e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp15 = createVirtualProcedure("vsp15", pm1, null, vspqn15); //$NON-NLS-1$
        vsp15.setResultSet(vsprs1());

        QueryNode vspqn16 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 INTO #temptable FROM pm1.g1; SELECT a.e1 FROM (SELECT pm1.g2.e1 FROM #temptable, pm1.g2 WHERE #temptable.e2 = pm1.g2.e2) AS a; END"); //$NON-NLS-1$ //$NON-NLS-2$
        //QueryNode vspqn16 = new QueryNode("vsp16", "CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 INTO #temptable FROM pm1.g1; SELECT e1 FROM #temptable where e1 in (SELECT pm1.g2.e1 FROM  #temptable, pm1.g2 WHERE #temptable.e2 = pm1.g2.e2); END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp16 = createVirtualProcedure("vsp16", pm1, null, vspqn16); //$NON-NLS-1$
        vsp16.setResultSet(vsprs1());

        QueryNode vspqn17 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; SELECT e1, e2 INTO #temptable FROM pm1.g1; LOOP ON (SELECT e1, e2 FROM #temptable) AS mycursor BEGIN x=mycursor.e2; END SELECT e1 FROM pm1.g1 WHERE x=e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp17 = createVirtualProcedure("vsp17", pm1, null, vspqn17); //$NON-NLS-1$
        vsp17.setResultSet(vsprs1());

        //invalid
         QueryNode vspqn18 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 INTO temptable FROM pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
         Procedure vsp18 = createVirtualProcedure("vsp18", pm1, null, vspqn18); //$NON-NLS-1$
         vsp18.setResultSet(vsprs1());

        QueryNode vspqn19 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 INTO #temptable FROM pm1.g1; SELECT e1 INTO #temptable FROM pm1.g1; SELECT e1 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp19 = createVirtualProcedure("vsp19", pm1, null, vspqn19); //$NON-NLS-1$
        vsp19.setResultSet(vsprs1());

        QueryNode vspqn20 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 INTO #temptable FROM pm1.g1; INSERT INTO #temptable(e1) VALUES( 'Fourth'); SELECT e1 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp20 = createVirtualProcedure("vsp20", pm1, null, vspqn20); //$NON-NLS-1$
        vsp20.setResultSet(vsprs1());

        ProcedureParameter vspp21 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn21 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 INTO #temptable FROM pm1.g1; INSERT INTO #temptable(#temptable.e1, e2) VALUES( 'Fourth', param1); SELECT e1, e2 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp21 = createVirtualProcedure("vsp21", pm1, Arrays.asList( vspp21 ), vspqn21); //$NON-NLS-1$
        vsp21.setResultSet(vspp4());

        ProcedureParameter vspp22 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn22 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 INTO #temptable FROM pm1.g1 where e2 > param1; SELECT e1, e2 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp22 = createVirtualProcedure("vsp22", pm1, Arrays.asList( vspp22 ), vspqn22); //$NON-NLS-1$
        vsp22.setResultSet(vspp4());

        ProcedureParameter vspp23 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn23 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; SELECT e1, e2 INTO #temptable FROM pm1.g1 where e2 > param1; x = SELECT e1 FROM #temptable WHERE e2=15; SELECT x, 15; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp23 = createVirtualProcedure("vsp23", pm1, Arrays.asList( vspp23 ), vspqn23); //$NON-NLS-1$
        vsp23.setResultSet(vspp4());

        QueryNode vspqn24 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 INTO #temptable FROM pm1.g1; SELECT #temptable.e1 FROM #temptable WHERE #temptable.e2=15; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp24 = createVirtualProcedure("vsp24", pm1, null, vspqn24); //$NON-NLS-1$
        vsp24.setResultSet(vsprs1());

        QueryNode vspqn25 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 INTO #temptable FROM pm1.g1 WHERE e1 ='no match'; SELECT e1 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp25 = createVirtualProcedure("vsp25", pm1, null, vspqn25); //$NON-NLS-1$
        vsp25.setResultSet(vsprs1());

        QueryNode vspqn27 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 from (exec pm1.vsp25())as c; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp27 = createVirtualProcedure("vsp27", pm1, null, vspqn27); //$NON-NLS-1$
        vsp27.setResultSet(vsprs1());

        QueryNode vspqn28 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT 0 AS e1 ORDER BY e1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp28 = createVirtualProcedure("vsp28", pm1, null, vspqn28); //$NON-NLS-1$
        vsp28.setResultSet(vsprs1());

        QueryNode vspqn29 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM pm1.g1 ORDER BY e1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp29 = createVirtualProcedure("vsp29", pm1, null, vspqn29); //$NON-NLS-1$
        vsp29.setResultSet(vsprs1());

        ColumnSet<Procedure> vsprs30 = createResultSet("pm1.vsprs30", new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode vspqn30 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp30 = createVirtualProcedure("vsp30", pm1, null, vspqn30); //$NON-NLS-1$
        vsp30.setResultSet(vsprs30);

        ColumnSet<Procedure> vsprs31 = createResultSet("pm1.vsprs31", new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter vsp31p2 = createParameter("p1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn31 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM pm1.g1 WHERE e2 = pm1.vsp31.p1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp31 = createVirtualProcedure("vsp31", pm1, Arrays.asList(createParameter("p1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER)), vspqn31); //$NON-NLS-1$
        vsp31.setResultSet(vsprs31);

        QueryNode vspqn38 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer VARIABLES.y; VARIABLES.y=5; EXEC pm1.vsp7(VARIABLES.y); END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp38 = createVirtualProcedure("vsp38", pm1, null, vspqn38); //$NON-NLS-1$
        vsp38.setResultSet(vsprs1());

        QueryNode vspqn39 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer VARIABLES.x; VARIABLES.x=5; EXEC pm1.vsp7(VARIABLES.x); END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp39 = createVirtualProcedure("vsp39", pm1, null, vspqn39); //$NON-NLS-1$
        vsp39.setResultSet(vsprs1());

        QueryNode vspqn40 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN LOOP ON (SELECT e2 FROM pm1.g1) AS mycursor BEGIN EXEC pm1.vsp41(); END END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp40 = createVirtualProcedure("vsp40", pm1, null, vspqn40); //$NON-NLS-1$
        vsp40.setResultSet(vsprs1());

        QueryNode vspqn41 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1 FROM pm1.g1 where e2=15; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp41 = createVirtualProcedure("vsp41", pm1, null, vspqn41); //$NON-NLS-1$
        vsp41.setResultSet(vsprs1());

        QueryNode vspqn37 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; VARIABLES.x=5; INSERT INTO vm1.g1(e2) values(VARIABLES.x); SELECT cast(ROWCOUNT as string); END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp37 = createVirtualProcedure("vsp37", pm1, null, vspqn37); //$NON-NLS-1$
        vsp37.setResultSet(vsprs1());

        QueryNode vspqn33 = new QueryNode(new StringBuffer("CREATE VIRTUAL PROCEDURE")  //$NON-NLS-1$//$NON-NLS-2$
                                                            .append(" BEGIN") //$NON-NLS-1$
                                                            .append(" SELECT 3 AS temp1 INTO #myTempTable;") //$NON-NLS-1$
                                                            .append(" SELECT 2 AS temp1 INTO #myTempTable;") //$NON-NLS-1$
                                                            .append(" SELECT 1 AS temp1 INTO #myTempTable;") //$NON-NLS-1$
                                                            .append(" SELECT temp1 AS e1 FROM #myTempTable ORDER BY e1;") //$NON-NLS-1$
                                                            .append(" END").toString() //$NON-NLS-1$
                                         );
        Procedure vsp33 = createVirtualProcedure("vsp33", pm1, null, vspqn33); //$NON-NLS-1$
        vsp33.setResultSet(vsprs1());

        ColumnSet<Procedure> vsprs35 = createResultSet("pm1.vsprs31", new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode vspqn35 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer VARIABLES.ID; VARIABLES.ID = pm1.vsp35.p1; SELECT e1 FROM pm1.g1 WHERE e2 = VARIABLES.ID; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp35 = createVirtualProcedure("vsp35", pm1, Arrays.asList(vsp31p2), vspqn35); //$NON-NLS-1$
        vsp35.setResultSet(vsprs35);

        QueryNode vspqn34 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, 0 AS const FROM pm1.g1 ORDER BY const; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp34 = createVirtualProcedure("vsp34", pm1, null, vspqn34); //$NON-NLS-1$
        vsp34.setResultSet(vspp4());

        QueryNode vspqn45 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 INTO #temptable FROM pm1.g1; SELECT #temptable.e1 FROM #temptable where #temptable.e1 in (SELECT pm1.g2.e1 FROM pm1.g2 ); END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp45 = createVirtualProcedure("vsp45", pm1, null, vspqn45); //$NON-NLS-1$
        vsp45.setResultSet(vsprs1());

        // Virtual group w/ procedure in transformation, optional params, named parameter syntax
        QueryNode vspqn47 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN IF (pm1.vsp47.param1 IS NOT NULL) BEGIN SELECT 'FOO' as e1, pm1.vsp47.param1 as e2; END ELSE BEGIN SELECT pm1.vsp47.param2 as e1, 2112 as e2; END END"); //$NON-NLS-1$ //$NON-NLS-2$
        ColumnSet<Procedure> vsprs47 = createResultSet("pm1.vsprs47", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter vspp47_2 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        vspp47_2.setNullType(NullType.Nullable);
        ProcedureParameter vspp47_3 = createParameter("param2", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        vspp47_3.setNullType(NullType.Nullable);
        Procedure vsp47 = createVirtualProcedure("vsp47", pm1, Arrays.asList( vspp47_2, vspp47_3 ), vspqn47); //$NON-NLS-1$
        vsp47.setResultSet(vsprs47);

        QueryNode vgvpn7 = new QueryNode("SELECT P.e2 as ve3, P.e1 as ve4 FROM (EXEC pm1.vsp47(param1=vm1.vgvp7.ve1, param2=vm1.vgvp7.ve2)) as P"); //$NON-NLS-1$ //$NON-NLS-2$
//        QueryNode vgvpn7 = new QueryNode("vm1.vgvp7", "SELECT P.e2 as ve1, P.e1 as ve2 FROM (EXEC pm1.vsp47(vm1.vgvp7.ve1, vm1.vgvp7.ve2)) as P"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vgvp7 = createVirtualGroup("vgvp7", vm1, vgvpn7); //$NON-NLS-1$
        Column vgvp7e1 = createElement("ve1", vgvp7, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        vgvp7e1.setSelectable(false);
        Column vgvp7e2 = createElement("ve2", vgvp7, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        vgvp7e2.setSelectable(false);
        createElement("ve3", vgvp7, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        createElement("ve4", vgvp7, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$

        //invalid
        QueryNode vspqn32 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; LOOP ON (SELECT e2 FROM pm1.g1) AS #mycursor BEGIN IF(#mycursor.e2 > 10) BEGIN CONTINUE; END x=#mycursor.e2; END SELECT e1 FROM pm1.g1 WHERE x=e2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp32 = createVirtualProcedure("vsp32", pm1, null, vspqn32); //$NON-NLS-1$
        vsp32.setResultSet(vsprs1());

        //virtual group with procedure in transformation
        QueryNode vspqn26 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1 WHERE e2 >= pm1.vsp26.param1 and e1 = pm1.vsp26.param2; END"); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter vspp26_1 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        ProcedureParameter vspp26_2 = createParameter("param2", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        ColumnSet<Procedure> vsprs3 = createResultSet("pm1.vsprs3", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Procedure vsp26 = createVirtualProcedure("vsp26", pm1, Arrays.asList( vspp26_1, vspp26_2 ), vspqn26); //$NON-NLS-1$
        vsp26.setResultSet(vsprs3);

        QueryNode vgvpn1 = new QueryNode("SELECT P.e1 as ve3 FROM (EXEC pm1.vsp26(vm1.vgvp1.ve1, vm1.vgvp1.ve2)) as P"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vgvp1 = createVirtualGroup("vgvp1", vm1, vgvpn1); //$NON-NLS-1$
        Column vgvp1e1 = createElement("ve1", vgvp1, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        vgvp1e1.setSelectable(false);
        Column vgvp1e2 = createElement("ve2", vgvp1, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        vgvp1e2.setSelectable(false);
        createElement("ve3", vgvp1, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$

        QueryNode vgvpn2 = new QueryNode("SELECT P.e1 as ve3 FROM (EXEC pm1.vsp26(vm1.vgvp2.ve1, vm1.vgvp2.ve2)) as P where P.e1='a'"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vgvp2 = createVirtualGroup("vgvp2", vm1, vgvpn2); //$NON-NLS-1$
        Column vgvp2e1 = createElement("ve1", vgvp2, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        vgvp2e1.setSelectable(false);
        Column vgvp2e2 = createElement("ve2", vgvp2, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        vgvp2e2.setSelectable(false);
        createElement("ve3", vgvp2, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$

        QueryNode vgvpn3 = new QueryNode("SELECT P.e1 as ve3 FROM (EXEC pm1.vsp26(vm1.vgvp3.ve1, vm1.vgvp3.ve2)) as P, pm1.g2 where P.e1=g2.e1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vgvp3 = createVirtualGroup("vgvp3", vm1, vgvpn3); //$NON-NLS-1$
        Column vgvp3e1 = createElement("ve1", vgvp3, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        vgvp3e1.setSelectable(false);
        Column vgvp3e2 = createElement("ve2", vgvp3, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        vgvp3e2.setSelectable(false);
        createElement("ve3", vgvp3, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$

        QueryNode vgvpn4 = new QueryNode("SELECT P.e1 as ve3 FROM (EXEC pm1.vsp26(vm1.vgvp4.ve1, vm1.vgvp4.ve2)) as P, vm1.g1 where P.e1=g1.e1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vgvp4 = createVirtualGroup("vgvp4", vm1, vgvpn4); //$NON-NLS-1$
        Column vgvp4e1 = createElement("ve1", vgvp4, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        vgvp4e1.setSelectable(false);
        Column vgvp4e2 = createElement("ve2", vgvp4, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        vgvp4e2.setSelectable(false);
        createElement("ve3", vgvp4, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$

        QueryNode vgvpn5 = new QueryNode("SELECT * FROM vm1.vgvp4 where vm1.vgvp4.ve1=vm1.vgvp5.ve1 and  vm1.vgvp4.ve2=vm1.vgvp5.ve2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vgvp5 = createVirtualGroup("vgvp5", vm1, vgvpn5); //$NON-NLS-1$
        Column vgvp5e1 = createElement("ve1", vgvp5, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        vgvp5e1.setSelectable(false);
        Column vgvp5e2 = createElement("ve2", vgvp5, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        vgvp5e2.setSelectable(false);
        createElement("ve3", vgvp5, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$

        QueryNode vgvpn6 = new QueryNode("SELECT P.e1 as ve3, P.e2 as ve4 FROM (EXEC pm1.vsp26(vm1.vgvp6.ve1, vm1.vgvp6.ve2)) as P"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vgvp6 = createVirtualGroup("vgvp6", vm1, vgvpn6); //$NON-NLS-1$
        Column vgvp6e1 = createElement("ve1", vgvp6, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        vgvp6e1.setSelectable(false);
        Column vgvp6e2 = createElement("ve2", vgvp6, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        vgvp6e2.setSelectable(false);
        createElement("ve3", vgvp6, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        createElement("ve4", vgvp6, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$

        //virtual group with two elements. One selectable, one not.
        QueryNode vm1g35n1 = new QueryNode("SELECT e1, e2 FROM pm1.g1");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g35 = createVirtualGroup("g35", vm1, vm1g35n1); //$NON-NLS-1$
        Column vm1g35e1 = createElement("e1", vm1g35, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        vm1g35e1.setSelectable(false);
        createElement("e2", vm1g35, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$

        ColumnSet<Procedure> vsprs36 = createResultSet("pm1.vsprs36", new String[] { "x" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter vsp36p2 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn36 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; x = pm1.vsp36.param1 * 2; SELECT x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp36 = createVirtualProcedure("vsp36", pm1, Arrays.asList( vsp36p2 ), vspqn36); //$NON-NLS-1$
        vsp36.setResultSet(vsprs36);

        ColumnSet<Procedure> vsprs42 = createResultSet("pm1.vsprs42", new String[] { "x" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter vsp42p2 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn42 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN IF (pm1.vsp42.param1 > 0) SELECT 1 AS x; ELSE SELECT 0 AS x; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp42 = createVirtualProcedure("vsp42", pm1, Arrays.asList( vsp42p2 ), vspqn42); //$NON-NLS-1$
        vsp42.setResultSet(vsprs42);

        ProcedureParameter vspp44 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn44 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT pm1.vsp44.param1 INTO #temptable; SELECT e1 from pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp44 = createVirtualProcedure("vsp44", pm1, Arrays.asList( vspp44 ), vspqn44); //$NON-NLS-1$
        vsp44.setResultSet(vsprs1());

        ProcedureParameter vspp43 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn43 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN exec pm1.vsp44(pm1.vsp43.param1); END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp43 = createVirtualProcedure("vsp43", pm1, Arrays.asList( vspp43 ), vspqn43); //$NON-NLS-1$
        vsp43.setResultSet(vsprs1());

        QueryNode vspqn46 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN create local temporary table #temptable (e1 string, e2 string); LOOP ON (SELECT e1 FROM pm1.g1) AS mycursor BEGIN select mycursor.e1, a.e1 as e2 into #temptable from (SELECT pm1.g1.e1 FROM pm1.g1 where pm1.g1.e1 = mycursor.e1) a; END SELECT e1 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp46 = createVirtualProcedure("vsp46", pm1, null, vspqn46); //$NON-NLS-1$
        vsp46.setResultSet(vsprs1());

        ColumnSet<Procedure> vsp48rs = createResultSet("pm1vsp48.rs", new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter vsp48p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        QueryNode vspqn48 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; SELECT e1 FROM (EXEC pm1.sq2(pm1.vsp48.in)) as e; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp48 = createVirtualProcedure("vsp48", pm1, Arrays.asList( vsp48p2 ), vspqn48); //$NON-NLS-1$
        vsp48.setResultSet(vsp48rs);

        ColumnSet<Procedure> vsp49rs = createResultSet("pm1vsp49.rs", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        QueryNode vspqn49 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; x = 'b'; EXEC pm1.sq2(x); END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp49 = createVirtualProcedure("vsp49", pm1, null, vspqn49); //$NON-NLS-1$
        vsp49.setResultSet(vsp49rs);

        ColumnSet<Procedure> vsp50rs = createResultSet("pm1vsp50.rs", new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode vspqn50 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; x = 'b'; SELECT e1 FROM (EXEC pm1.sq2(x)) as e; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp50 = createVirtualProcedure("vsp50", pm1, null, vspqn50); //$NON-NLS-1$
        vsp50.setResultSet(vsp50rs);

        ColumnSet<Procedure> vsp51rs = createResultSet("pm1vsp51.rs", new String[] { "result" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode vspqn51 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; x = 'b'; LOOP ON (SELECT e1 FROM (EXEC pm1.sq2(x)) as e) AS c BEGIN x = x || 'b'; END SELECT x AS result; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp51 = createVirtualProcedure("vsp51", pm1, null, vspqn51); //$NON-NLS-1$
        vsp51.setResultSet(vsp51rs);

        ColumnSet<Procedure> vsp52rs = createResultSet("pm1vsp52.rs", new String[] { "result" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        QueryNode vspqn52 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; x = 'c'; x = SELECT e1 FROM (EXEC pm1.sq2(x)) as e; SELECT x AS result; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp52 = createVirtualProcedure("vsp52", pm1, null, vspqn52); //$NON-NLS-1$
        vsp52.setResultSet(vsp52rs);

        ColumnSet<Procedure> vsp53rs = createResultSet("pm1vsp53.rs", new String[] { "result" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter vsp53p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        QueryNode vspqn53 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; x = 'b'; LOOP ON (SELECT e1 FROM (EXEC pm1.sq2(pm1.vsp53.in)) as e) AS c BEGIN x = x || 'b'; END SELECT x AS result; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp53 = createVirtualProcedure("vsp53", pm1, Arrays.asList( vsp53p2 ), vspqn53); //$NON-NLS-1$
        vsp53.setResultSet(vsp53rs);

        ColumnSet<Procedure> vsp54rs = createResultSet("pm1vsp54.rs", new String[] { "result" }, new String[] { DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$
        ProcedureParameter vsp54p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        QueryNode vspqn54 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN DECLARE string x; x = 'c'; x = SELECT e1 FROM (EXEC pm1.sq2(pm1.vsp54.in)) as e; SELECT x AS result; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp54 = createVirtualProcedure("vsp54", pm1, Arrays.asList( vsp54p2 ), vspqn54); //$NON-NLS-1$
        vsp54.setResultSet(vsp54rs);

        ProcedureParameter vspp55 = createParameter("param1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn55 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN select e1, param1 as a from vm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp55 = createVirtualProcedure("vsp55", pm1, Arrays.asList( vspp55 ), vspqn55); //$NON-NLS-1$
        vsp55.setResultSet(vspp4());

        QueryNode vspqn56 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT * INTO #temptable FROM pm1.g1; SELECT #temptable.e1 FROM #temptable; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp56 = createVirtualProcedure("vsp56", pm1, null, vspqn56); //$NON-NLS-1$
        vsp56.setResultSet(vsprs1());

        QueryNode vspqn57 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT * INTO #temptable FROM pm1.g1; SELECT #temptable.e1 FROM #temptable order by #temptable.e1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp57 = createVirtualProcedure("vsp57", pm1, null, vspqn57); //$NON-NLS-1$
        vsp57.setResultSet(vsprs1());

        ProcedureParameter vspp58 = createParameter("inp", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        QueryNode vspqn58 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT vsp58.inp; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp58 = createVirtualProcedure("vsp58", pm1, Arrays.asList( vspp58 ), vspqn58); //$NON-NLS-1$
        vsp58.setResultSet(vsprs1());

        QueryNode vspqn59 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT * INTO #temp FROM pm5.g3;INSERT INTO #temp (e1, e2) VALUES('integer',1); END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp59 = createVirtualProcedure("vsp59", pm6, null, vspqn59); //$NON-NLS-1$
        vsp59.setResultSet(vsprs1());

        QueryNode vspqn60 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN create local temporary table temp_table (column1 string);insert into temp_table (column1) values ('First');insert into temp_table (column1) values ('Second');insert into temp_table (column1) values ('Third');select * from temp_table; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp60 = createVirtualProcedure("vsp60", pm1, null, vspqn60); //$NON-NLS-1$
        vsp60.setResultSet(vsprs1());

        QueryNode vspqn61 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN create local temporary table temp_table (column1 string);insert into temp_table (column1) values ('First');drop table temp_table;create local temporary table temp_table (column1 string);insert into temp_table (column1) values ('First');insert into temp_table (column1) values ('Second');insert into temp_table (column1) values ('Third');select * from temp_table; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp61 = createVirtualProcedure("vsp61", pm1, null, vspqn61); //$NON-NLS-1$
        vsp61.setResultSet(vsprs1());

        QueryNode vspqn62 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN create local temporary table temp_table (column1 string); select e1 as column1 into temp_table from pm1.g1;select * from temp_table; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp62 = createVirtualProcedure("vsp62", pm1, null, vspqn62); //$NON-NLS-1$
        vsp62.setResultSet(vsprs1());

        QueryNode vspqn63 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN declare string o; if(1>0) begin declare string a; a='b'; o=a; end if(1>0) begin declare string a; a='c'; o=a; end  select o; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure vsp63 = createVirtualProcedure("vsp63", pm1, null, vspqn63); //$NON-NLS-1$
        vsp63.setResultSet(vsprs1());

        return metadataStore;
    }

    public static TransformationMetadata example1() {
        return createTransformationMetadata(example1Store(), "example1");
    }

    private static ColumnSet<Procedure> vspp4() {
        return createResultSet("pm1.vsprs2", new String[] { "e1", "const" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
    }

    private static ColumnSet<Procedure> vsprs1() {
        return createResultSet("pm1.vsprs1", new String[] { "e1" }, new String[] { DataTypeManager.DefaultDataTypes.STRING });
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
        case AccessPattern:
            group.getAccessPatterns().add(key);
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
        table.setTableType(org.teiid.metadata.Table.Type.Table);
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
        table.setTableType(org.teiid.metadata.Table.Type.View);
        table.setSelectTransformation(plan.getQuery());
        return table;
    }

    public static Table createXmlStagingTable(String name, Schema model, QueryNode plan) {
        Table table = createVirtualGroup(name, model, plan);
        table.setTableType(org.teiid.metadata.Table.Type.XmlStagingTable);
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
        column.setDatatype(SystemMetadata.getInstance().getRuntimeTypeMap().get(type), true, 0);
        if (DataTypeManager.hasLength(type) && !type.equalsIgnoreCase(DataTypeManager.DefaultDataTypes.CHAR)) {
            column.setLength(100);
        }
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
     * @return Metadata object for stored procedure
     */
    public static Procedure createStoredProcedure(String name, Schema model, List<ProcedureParameter> params) {
        Procedure proc = new Procedure();
        proc.setName(name);
        proc.setNameInSource(name);
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
        Procedure proc = createStoredProcedure(name, model, params);
        proc.setVirtual(true);
        proc.setQueryPlan(queryPlan.getQuery());
        return proc;
    }

    /**
     * Create a result set.
     */
    public static ColumnSet<Procedure> createResultSet(String name, String[] colNames, String[] colTypes) {
        ColumnSet<Procedure> rs = new ColumnSet<Procedure>();
        int index = name.indexOf('.');
        if (index > 0) {
            name = name.substring(index + 1);
        }
        rs.setName(name);
        for(Column column : createElements(rs, colNames, colTypes)) {
            column.setParent(rs);
        }
        return rs;
    }

    public static KeyRecord createAccessPattern(String name, Table group, List<Column> elements) {
        return createKey(org.teiid.metadata.KeyRecord.Type.AccessPattern, name, group, elements);
    }

    public static VDBMetaData example1VDB() {
        VDBMetaData vdb = new VDBMetaData();
        vdb.setName("example1");
        vdb.setVersion(1);
        vdb.addModel(RealMetadataFactory.createModel("pm1", true));
        vdb.addModel(RealMetadataFactory.createModel("pm2", true));
        vdb.addModel(RealMetadataFactory.createModel("pm3", true));
        vdb.addModel(RealMetadataFactory.createModel("pm4", true));
        vdb.addModel(RealMetadataFactory.createModel("pm5", true));
        vdb.addModel(RealMetadataFactory.createModel("pm6", true));
        vdb.addModel(RealMetadataFactory.createModel("vm1", false));
        vdb.addModel(RealMetadataFactory.createModel("vm2", false));
        vdb.addModel(RealMetadataFactory.createModel("tm1", false));

        return vdb;
    }

    public static VDBMetaData exampleBQTVDB() {
        VDBMetaData vdb = new VDBMetaData();
        vdb.setName("example1");
        vdb.setVersion(1);
        vdb.addModel(RealMetadataFactory.createModel("BQT1", true));
        vdb.addModel(RealMetadataFactory.createModel("BQT2", true));
        vdb.addModel(RealMetadataFactory.createModel("BQT3", true));
        vdb.addModel(RealMetadataFactory.createModel("LOB", true));
        vdb.addModel(RealMetadataFactory.createModel("VQT", false));
        vdb.addModel(RealMetadataFactory.createModel("pm1", true));
        vdb.addModel(RealMetadataFactory.createModel("pm2", true));
        vdb.addModel(RealMetadataFactory.createModel("pm3", true));
        vdb.addModel(RealMetadataFactory.createModel("pm4", true));

        return vdb;
    }

    public static VDBMetaData exampleMultiBindingVDB() {
        VDBMetaData vdb = new VDBMetaData();
        vdb.setName("exampleMultiBinding");
        vdb.setVersion(1);

        ModelMetaData model = new ModelMetaData();
        model.setName("MultiModel");
        model.setModelType(Model.Type.PHYSICAL);
        model.setVisible(true);

        model.setSupportsMultiSourceBindings(true);
        vdb.addModel(model);
        vdb.addModel(RealMetadataFactory.createModel("Virt", false));

        return vdb;
    }

    public static DQPWorkContext buildWorkContext(TransformationMetadata metadata) {
        return buildWorkContext(metadata, metadata.getVdbMetaData());
    }

    public static DQPWorkContext buildWorkContext(QueryMetadataInterface metadata, VDBMetaData vdb) {
        DQPWorkContext workContext = new DQPWorkContext();
        SessionMetadata session = new SessionMetadata();
        workContext.setSession(session);
        session.setVDBName(vdb.getName());
        session.setVDBVersion(vdb.getVersion());
        session.setSessionId(String.valueOf(1));
        session.setUserName("foo"); //$NON-NLS-1$
        session.setVdb(vdb);
        workContext.getVDB().addAttachment(QueryMetadataInterface.class, metadata);
        if (metadata instanceof TransformationMetadata) {
            workContext.getVDB().addAttachment(TransformationMetadata.class, (TransformationMetadata)metadata);
        }
        DQPWorkContext.setWorkContext(workContext);
        return workContext;
    }

    public static ModelMetaData createModel(String name, boolean source) {
        ModelMetaData model = new ModelMetaData();
        model.setName(name);
        if (source) {
            model.setModelType(Model.Type.PHYSICAL);
        }
        else {
            model.setModelType(Model.Type.VIRTUAL);
        }
        model.setVisible(true);
        model.setSupportsMultiSourceBindings(false);
        model.addSourceMapping(name, name, null);

        return model;
    }

    public static TransformationMetadata exampleBitwise() {
        MetadataStore store = new MetadataStore();
        Schema phys = createPhysicalModel("phys", store); //$NON-NLS-1$
        Table t = createPhysicalGroup("t", phys); //$NON-NLS-1$
        createElements(t,
                                    new String[] { "ID", "Name", "source_bits" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                                    new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });

        Schema virt = createVirtualModel("virt", store); //$NON-NLS-1$
        ColumnSet<Procedure> rs = createResultSet("rs", new String[] { "ID", "Name", "source_bits" }, new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        QueryNode qn = new QueryNode("CREATE VIRTUAL PROCEDURE " //$NON-NLS-1$
          + "BEGIN " //$NON-NLS-1$
          + "        DECLARE integer VARIABLES.BITS;" //$NON-NLS-1$
          + "        create local temporary table #temp (id integer, name string, bits integer);" //$NON-NLS-1$
          + "        LOOP ON (SELECT DISTINCT phys.t.ID, phys.t.Name FROM phys.t) AS idCursor" //$NON-NLS-1$
          + "        BEGIN" //$NON-NLS-1$
          + "                VARIABLES.BITS = 0;" //$NON-NLS-1$
          + "                LOOP ON (SELECT phys.t.source_bits FROM phys.t WHERE phys.t.ID = idCursor.id) AS bitsCursor" //$NON-NLS-1$
          + "                BEGIN" //$NON-NLS-1$
          + "                        VARIABLES.BITS = bitor(VARIABLES.BITS, bitsCursor.source_bits);" //$NON-NLS-1$
          + "                END" //$NON-NLS-1$
          + "                SELECT idCursor.id, idCursor.name, VARIABLES.BITS INTO #temp;" //$NON-NLS-1$
          + "        END" //$NON-NLS-1$
          + "        SELECT ID, Name, #temp.BITS AS source_bits FROM #temp;" //$NON-NLS-1$
          + "END"); //$NON-NLS-1$
        Procedure proc = createVirtualProcedure("agg", virt, null, qn); //$NON-NLS-1$
        proc.setResultSet(rs);

        return createTransformationMetadata(store, "bitwise");
    }

    public static void setCardinality(String group, int cardinality, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        if (metadata instanceof TransformationMetadata) {
            Table t = (Table)metadata.getGroupID(group);
            t.setCardinality(cardinality);
        } else {
            throw new RuntimeException("unknown metadata"); //$NON-NLS-1$
        }
    }

    public static TransformationMetadata exampleAggregatesCached() {
        return CACHED_AGGREGATES;
    }

    public static TransformationMetadata example3() {
        MetadataStore metadataStore = new MetadataStore();
        // Create models
        Schema pm1 = createPhysicalModel("pm1", metadataStore); //$NON-NLS-1$
        Schema pm2 = createPhysicalModel("pm2", metadataStore); //$NON-NLS-1$
        Schema pm3 = createPhysicalModel("pm3", metadataStore); //$NON-NLS-1$

        // Create physical groups
        Table pm1g1 = createPhysicalGroup("cat1.cat2.cat3.g1", pm1); //$NON-NLS-1$
        Table pm1g2 = createPhysicalGroup("cat1.g2", pm1); //$NON-NLS-1$
        Table pm1g3 = createPhysicalGroup("cat2.g3", pm1); //$NON-NLS-1$
        Table pm2g1 = createPhysicalGroup("cat1.g1", pm2); //$NON-NLS-1$
        Table pm2g2 = createPhysicalGroup("cat2.g2", pm2); //$NON-NLS-1$
        Table pm2g3 = createPhysicalGroup("g3", pm2); //$NON-NLS-1$
        Table pm2g4 = createPhysicalGroup("g4", pm3);         //$NON-NLS-1$
        Table pm2g5 = createPhysicalGroup("cat3.g1", pm2); //$NON-NLS-1$

        // Create physical elements
        createElements(pm1g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm1g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm1g3,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm2g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm2g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm2g3,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm2g4,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm2g5,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });

        // Create the facade from the store
        return createTransformationMetadata(metadataStore, "example3");
    }

    public static TransformationMetadata exampleUpdateProc(Table.TriggerEvent event, String procedure) {
        MetadataStore metadataStore = new MetadataStore();
        // Create models
        Schema pm1 = createPhysicalModel("pm1", metadataStore); //$NON-NLS-1$
        Schema pm2 = createPhysicalModel("pm2", metadataStore); //$NON-NLS-1$
        Schema vm1 = createVirtualModel("vm1", metadataStore); //$NON-NLS-1$

        // Create physical groups
        Table pm1g1 = createPhysicalGroup("g1", pm1); //$NON-NLS-1$
        Table pm1g2 = createPhysicalGroup("g2", pm1); //$NON-NLS-1$
        Table pm2g1 = createPhysicalGroup("g1", pm2); //$NON-NLS-1$
        Table pm2g2 = createPhysicalGroup("g2", pm2); //$NON-NLS-1$

        // Create physical group elements
        createElements(pm1g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm1g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm2g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm2g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });

        // Create virtual groups
        QueryNode vm1g1n1 = new QueryNode("SELECT * FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g1 = createUpdatableVirtualGroup("g1", vm1, vm1g1n1); //$NON-NLS-1$

        QueryNode vm1g2n1 = new QueryNode("SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3 FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g2 = createUpdatableVirtualGroup("g2", vm1, vm1g2n1); //$NON-NLS-1$

        QueryNode vm1g3n1 = new QueryNode("SELECT CONCAT(e1, 'm') as x, (e2 +1) as y, 1 as e3, e4*50 as e4 FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g3 = createUpdatableVirtualGroup("g3", vm1, vm1g3n1); //$NON-NLS-1$

        QueryNode vm1g4n1 = new QueryNode("SELECT * FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g4 = createUpdatableVirtualGroup("g4", vm1, vm1g4n1); //$NON-NLS-1$

        // Create virtual elements
        createElementsWithDefaults(vm1g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE },
            new String[] { "xyz", "123", "true", "123.456"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        createElementsWithDefaults(vm1g2,
            new String[] { "e1", "e2", "e3" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN },
            new String[] { "abc", "456", "false"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        createElementsWithDefaults(vm1g3,
            new String[] { "x", "y", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER , DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.DOUBLE },
            new String[] { "mno", "789", "true", "789.012"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        createElementsWithDefaults(vm1g4,
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE },
                new String[] { "xyz", "123", "true", "123.456"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$

        setInsteadOfTriggerDefinition(vm1g1, event, procedure);
        setInsteadOfTriggerDefinition(vm1g2, event, procedure);
        setInsteadOfTriggerDefinition(vm1g3, event, procedure);
        setInsteadOfTriggerDefinition(vm1g4, event, procedure);

        // Create the facade from the store
        return createTransformationMetadata(metadataStore, "proc");
    }

    public static void setInsteadOfTriggerDefinition(Table view, TriggerEvent event, String proc) {
        switch (event) {
        case DELETE:
            view.setDeletePlan(proc);
            break;
        case INSERT:
            view.setInsertPlan(proc);
            break;
        case UPDATE:
            view.setUpdatePlan(proc);
            break;
        }
    }

    public static TransformationMetadata exampleUpdateProc(TriggerEvent procedureType, String procedure1, String procedure2) {
        MetadataStore metadataStore = new MetadataStore();
        // Create models
        Schema pm1 = createPhysicalModel("pm1", metadataStore); //$NON-NLS-1$
        Schema pm2 = createPhysicalModel("pm2", metadataStore); //$NON-NLS-1$
        Schema vm1 = createVirtualModel("vm1", metadataStore); //$NON-NLS-1$

        // Create physical groups
        Table pm1g1 = createPhysicalGroup("g1", pm1); //$NON-NLS-1$
        Table pm1g2 = createPhysicalGroup("g2", pm1); //$NON-NLS-1$
        Table pm2g1 = createPhysicalGroup("g1", pm2); //$NON-NLS-1$
        Table pm2g2 = createPhysicalGroup("g2", pm2); //$NON-NLS-1$

        // Create physical group elements
        createElements(pm1g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm1g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm2g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm2g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });

        // Create virtual groups
        QueryNode vm1g1n1 = new QueryNode("SELECT * FROM vm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g1 = createUpdatableVirtualGroup("g1", vm1, vm1g1n1); //$NON-NLS-1$

        QueryNode vm1g2n1 = new QueryNode("SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3, pm1.g2.e4 FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g2 = createUpdatableVirtualGroup("g2", vm1, vm1g2n1); //$NON-NLS-1$

        // Create virtual elements
        createElementsWithDefaults(vm1g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE },
            new String[] { "xyz", "123", "true", "123.456"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        createElementsWithDefaults(vm1g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE },
            new String[] { "abc", "456", "false", null}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        setInsteadOfTriggerDefinition(vm1g1, procedureType, procedure1);
        setInsteadOfTriggerDefinition(vm1g2, procedureType, procedure2);

        // Create the facade from the store
        return createTransformationMetadata(metadataStore, "proc");
    }

    public static TransformationMetadata exampleBusObj() {
        // Create the facade from the store
        return createTransformationMetadata(exampleBusObjStore(), "busObj");
    }

    public static MetadataStore exampleBusObjStore() {
        MetadataStore metadataStore = new MetadataStore();
        // Create db2 tables
        Schema db2Model = createPhysicalModel("db2model", metadataStore); //$NON-NLS-1$

        Table db2Table = createPhysicalGroup("DB2_TABLE", db2Model); //$NON-NLS-1$
        createElements(db2Table,
            new String[] { "PRODUCT", "REGION", "SALES"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DOUBLE});

        Table salesTable = createPhysicalGroup("SALES", db2Model); //$NON-NLS-1$
        salesTable.setCardinality(1000000);
        createElements(salesTable,
            new String[] { "CITY", "MONTH", "SALES"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DOUBLE});

        Table geographyTable2 = createPhysicalGroup("GEOGRAPHY2", db2Model); //$NON-NLS-1$
        geographyTable2.setCardinality(1000);
        List<Column> geographyElem2 = createElements(geographyTable2,
            new String[] { "CITY", "REGION"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});
        List<Column> geoPkElem2 = new ArrayList<Column>();
        geoPkElem2.add(geographyElem2.get(0));
        createKey(KeyRecord.Type.Primary, "db2model.GEOGRAPHY2.GEOGRAPHY_PK", geographyTable2, geoPkElem2); //$NON-NLS-1$

        Table db2Table2 = createPhysicalGroup("DB2TABLE", db2Model); //$NON-NLS-1$
        createElements(db2Table2,
            new String[] { "c0", "c1", "c2"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER});

        // Create oracle tables
        Schema oraModel = createPhysicalModel("oraclemodel", metadataStore); //$NON-NLS-1$

        Table oraTable = createPhysicalGroup("Oracle_table", oraModel); //$NON-NLS-1$
        createElements(oraTable,
            new String[] { "COSTS", "REGION", "YEAR"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});

        Table geographyTable = createPhysicalGroup("GEOGRAPHY", oraModel); //$NON-NLS-1$
        geographyTable.setCardinality(1000);
        List<Column> geographyElem = createElements(geographyTable,
            new String[] { "CITY", "REGION"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});
        List<Column> geoPkElem = new ArrayList<Column>();
        geoPkElem.add(geographyElem.get(0));
        createKey(KeyRecord.Type.Primary, "oraclemodel.GEOGRAPHY.GEOGRAPHY_PK", geographyTable, geoPkElem); //$NON-NLS-1$

        Table oraTable2 = createPhysicalGroup("OraTable", oraModel); //$NON-NLS-1$
        createElements(oraTable2,
            new String[] { "b0", "b1", "b2"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});

        // Create sql server tables
        Schema msModel = createPhysicalModel("msmodel", metadataStore); //$NON-NLS-1$

        Table timeTable = createPhysicalGroup("TIME", msModel); //$NON-NLS-1$
        timeTable.setCardinality(120);
        List<Column> timeElem = createElements(timeTable,
            new String[] { "MONTH", "YEAR"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});
        List<Column> timePkElem = new ArrayList<Column>();
        timePkElem.add(timeElem.get(0));
        createKey(KeyRecord.Type.Primary, "msmodel.TIME.TIME_PK", timeTable, timePkElem); //$NON-NLS-1$

        Schema virtModel = createVirtualModel("logical", metadataStore); //$NON-NLS-1$
        QueryNode n1 = new QueryNode("select sum(c0) as c0, c1, c2 from db2Table group by c1, c2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table logicalTable1 = createVirtualGroup("logicalTable1", virtModel, n1); //$NON-NLS-1$
        createElements(logicalTable1,
            new String[] { "c0", "c1", "c2"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.LONG, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER});

        QueryNode n2 = new QueryNode("select sum(c0) as c0, c1, c2 from db2Table group by c1, c2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table logicalTable2 = createVirtualGroup("logicalTable2", virtModel, n2); //$NON-NLS-1$
        createElements(logicalTable2,
            new String[] { "b0", "b1", "b2"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.LONG, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER});

        return metadataStore;
    }

    public static TransformationMetadata exampleAggregates() {
        MetadataStore store = new MetadataStore();
        addAggregateTablesToModel("m1", store); //$NON-NLS-1$
        addAggregateTablesToModel("m2", store); //$NON-NLS-1$

        // Create the facade from the store
        return createTransformationMetadata(store, "exampleAggregates");
    }

    public static void addAggregateTablesToModel(String modelName, MetadataStore metadataStore) {
        // Create db2 tables
        Schema model = createPhysicalModel(modelName, metadataStore);

        Table orders = createPhysicalGroup("order", model); //$NON-NLS-1$
        orders.setCardinality(1000000);
        createElements(orders,
            new String[] { "O_OrderID", "O_ProductID", "O_DealerID", "O_Amount", "O_Date"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BIG_DECIMAL, DataTypeManager.DefaultDataTypes.DATE });

        Table products = createPhysicalGroup("product", model); //$NON-NLS-1$
        products.setCardinality(1000);
        createElements(products,
            new String[] { "P_ProductID", "P_Overhead", "P_DivID"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BIG_DECIMAL, DataTypeManager.DefaultDataTypes.INTEGER});

        Table divisions = createPhysicalGroup("division", model); //$NON-NLS-1$
        divisions.setCardinality(100);
        createElements(divisions,
            new String[] { "V_DIVID", "V_SectorID"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER});

        Table dealers = createPhysicalGroup("dealer", model); //$NON-NLS-1$
        dealers.setCardinality(1000);
        createElements(dealers,
            new String[] { "D_DealerID", "D_State", "D_Address"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING});
    }

    /**
     * Metadata for Multi-Binding models
     * @return example
     * @since 4.2
     */
    public static TransformationMetadata exampleMultiBinding() {
        MetadataStore metadataStore = new MetadataStore();
        Schema virtModel = createVirtualModel("Virt", metadataStore); //$NON-NLS-1$
        Schema physModel = createPhysicalModel("MultiModel", metadataStore); //$NON-NLS-1$

        Table physGroup = createPhysicalGroup("Phys", physModel); //$NON-NLS-1$
        createElements(physGroup,
                                      new String[] { "a", "b" }, //$NON-NLS-1$ //$NON-NLS-2$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        Table physGroup1 = createPhysicalGroup("Phys1", physModel); //$NON-NLS-1$
        List<Column> cols = createElements(physGroup1,
                new String[] { "a", "b", "c" }, //$NON-NLS-1$ //$NON-NLS-2$
                new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });
        cols.get(2).setProperty(MultiSourceMetadataWrapper.MULTISOURCE_PARTITIONED_PROPERTY, Boolean.TRUE.toString());

        Table physGroup2 = createPhysicalGroup("Phys2", physModel); //$NON-NLS-1$
        createElements(physGroup2,
                                      new String[] { "a", "b" }, //$NON-NLS-1$ //$NON-NLS-2$
                                      new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });

        QueryNode virtTrans = new QueryNode("SELECT * FROM MultiModel.Phys");         //$NON-NLS-1$ //$NON-NLS-2$
        Table virtGroup = createVirtualGroup("view", virtModel, virtTrans); //$NON-NLS-1$
        createElements(virtGroup,
                                           new String[] { "a", "b" }, //$NON-NLS-1$ //$NON-NLS-2$
                                           new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });

        ColumnSet<Procedure> rs2 = createResultSet("Virt.rs1", new String[] { "a", "b" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter rs2p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        rs2p2.setNullType(org.teiid.metadata.BaseColumn.NullType.Nullable);
        QueryNode sq2n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN\n" //$NON-NLS-1$ //$NON-NLS-2$
                                        + "execute string 'SELECT a, b FROM MultiModel.Phys where SOURCE_NAME = Virt.sq1.in'; END"); //$NON-NLS-1$
        Procedure sq1 = createVirtualProcedure("sq1", virtModel, Arrays.asList(rs2p2), sq2n1);  //$NON-NLS-1$
        sq1.setResultSet(rs2);

        ColumnSet<Procedure> rs3 = createResultSet("MultiModel.rs1", new String[] { "a", "b" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter rs3p2 = createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        ProcedureParameter rs3p3 = createParameter("source_name", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        rs3p3.setNullType(org.teiid.metadata.BaseColumn.NullType.Nullable);
        Procedure sq2 = createStoredProcedure("proc", physModel, Arrays.asList(rs3p2, rs3p3));
        sq2.setResultSet(rs3);
        return createTransformationMetadata(metadataStore, "multiBinding");
    }

    /**
     * This example is for testing static costing using cardinality information from
     * metadata, as well as key information and maybe access patterns
     */
    public static TransformationMetadata example4() {
        MetadataStore metadataStore = new MetadataStore();
        // Create models - physical ones will support joins
        Schema pm1 = createPhysicalModel("pm1", metadataStore); //$NON-NLS-1$
        Schema pm2 = createPhysicalModel("pm2", metadataStore); //$NON-NLS-1$
        Schema pm3 = createPhysicalModel("pm3", metadataStore); //$NON-NLS-1$
        Schema pm4 = createPhysicalModel("pm4", metadataStore); //$NON-NLS-1$
        Schema vm1 = createVirtualModel("vm1", metadataStore);     //$NON-NLS-1$

        // Create physical groups
        Table pm1g1 = createPhysicalGroup("g1", pm1); //$NON-NLS-1$
        Table pm1g2 = createPhysicalGroup("g2", pm1); //$NON-NLS-1$
        Table pm1g3 = createPhysicalGroup("g3", pm1); //$NON-NLS-1$
        Table pm2g1 = createPhysicalGroup("g1", pm2); //$NON-NLS-1$
        Table pm2g2 = createPhysicalGroup("g2", pm2); //$NON-NLS-1$
        Table pm2g3 = createPhysicalGroup("g3", pm2); //$NON-NLS-1$
        Table pm3g1 = createPhysicalGroup("g1", pm3); //$NON-NLS-1$
        Table pm3g2 = createPhysicalGroup("g2", pm3); //$NON-NLS-1$
        Table pm3g3 = createPhysicalGroup("g3", pm3); //$NON-NLS-1$
        Table pm4g1 = createPhysicalGroup("g1", pm4); //$NON-NLS-1$
        Table pm4g2 = createPhysicalGroup("g2", pm4); //$NON-NLS-1$
        // Add group cardinality metadata
        pm1g1.setCardinality(10);
        pm1g2.setCardinality(10);
        pm1g3.setCardinality(10);
        pm2g1.setCardinality(1000);
        pm2g2.setCardinality(1000);
        pm3g1.setCardinality(100000);
        pm3g2.setCardinality(100000);
        pm3g3.setCardinality(100000);
        // leave pm4.g1 as unknown

        // Create physical elements
        List<Column> pm1g1e = createElements(pm1g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm1g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List<Column> pm1g3e = createElements(pm1g3,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List<Column> pm2g1e = createElements(pm2g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm2g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm2g3,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List<Column> pm3g1e = createElements(pm3g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(pm3g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List<Column> pm3g3e = createElements(pm3g3,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List<Column> pm4g1e = createElements(pm4g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        List<Column> pm4g2e = createElements(pm4g2,
                new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });

        // Add key metadata
        createKey(KeyRecord.Type.Primary, "pm1.g1.key1", pm1g1, pm1g1e.subList(0, 1)); //e1 //$NON-NLS-1$
        createKey(KeyRecord.Type.Primary, "pm3.g1.key1", pm3g1, pm3g1e.subList(0, 1)); //e1 //$NON-NLS-1$
        createKey(KeyRecord.Type.Primary, "pm3.g3.key1", pm3g3, pm3g3e.subList(0, 1)); //e1 //$NON-NLS-1$
        KeyRecord pm4g1key1= createKey(KeyRecord.Type.Primary, "pm4.g1.key1", pm4g1, pm4g1e.subList(0, 2)); //e1, e2 //$NON-NLS-1$
        createForeignKey("pm4.g2.fk", pm4g2, pm4g2e.subList(0, 2), pm4g1key1); //$NON-NLS-1$
        // Add access pattern metadata
        // Create access patterns - pm1
        List<Column> elements = new ArrayList<Column>(1);
        elements.add(pm1g1e.iterator().next());
        createAccessPattern("pm1.g1.ap1", pm1g1, elements); //e1 //$NON-NLS-1$
        elements = new ArrayList<Column>(2);
        Iterator<Column> iter = pm1g3e.iterator();
        elements.add(iter.next());
        elements.add(iter.next());
        createAccessPattern("pm1.g3.ap1", pm1g3, elements); //e1,e2 //$NON-NLS-1$
        // Create access patterns - pm2
        elements = new ArrayList<Column>(1);
        elements.add(pm2g1e.iterator().next());
        createAccessPattern("pm2.g1.ap1", pm2g1, elements); //e1 //$NON-NLS-1$

        // Create virtual groups
        QueryNode vm1g1n1 = new QueryNode("SELECT * FROM pm1.g1"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g1 = createUpdatableVirtualGroup("g1", vm1, vm1g1n1); //$NON-NLS-1$

        QueryNode vm1g2n1 = new QueryNode("SELECT pm1.g2.e1, pm1.g2.e2, pm1.g2.e3 FROM pm1.g2"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g2 = createUpdatableVirtualGroup("g2", vm1, vm1g2n1); //$NON-NLS-1$

        QueryNode vm1g3n1 = new QueryNode("SELECT pm1.g3.e1 AS x, pm1.g3.e2 AS y from pm1.g3"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g3 = createUpdatableVirtualGroup("g3", vm1, vm1g3n1); //$NON-NLS-1$

        QueryNode vm1g4n1 = new QueryNode("SELECT distinct pm1.g2.e1 as ve1, pm1.g1.e1 as ve2 FROM pm1.g2 LEFT OUTER JOIN /* optional */ pm1.g1 on pm1.g1.e1 = pm1.g2.e1");         //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g4 = createVirtualGroup("g4", vm1, vm1g4n1); //$NON-NLS-1$
        createElements(vm1g4,
                  new String[] { "ve1", "ve2" }, //$NON-NLS-1$ //$NON-NLS-2$
                  new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });

        // Create virtual elements
        createElements(vm1g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        createElements(vm1g3,
            new String[] { "e1", "e2","x", "y" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER });

        return createTransformationMetadata(metadataStore, "example4");
    }

    public static TransformationMetadata fromDDL(String ddl, String vdbName, String modelName) throws Exception {
        return fromDDL(vdbName, new DDLHolder(modelName, ddl));
    }

    public static class DDLHolder {
        String name;
        String ddl;
        public DDLHolder(String name, String ddl) {
            this.name = name;
            this.ddl = ddl;
        }
    }

    public static TransformationMetadata fromDDL(String vdbName, DDLHolder... schemas) throws Exception {
        CompositeMetadataStore cms = new CompositeMetadataStore(Collections.EMPTY_LIST);
        for (DDLHolder schema : schemas) {
            MetadataFactory mf = TestDDLParser.helpParse(schema.ddl, schema.name);
            cms.merge(mf.asMetadataStore());
        }

        TransformationMetadata tm = createTransformationMetadata(cms, vdbName);
        ValidatorReport report = new MetadataValidator().validate(tm.getVdbMetaData(), tm.getMetadataStore());
        if (report.hasItems()) {
            throw new RuntimeException(report.getFailureMessage());
        }
        return tm;
    }
}
