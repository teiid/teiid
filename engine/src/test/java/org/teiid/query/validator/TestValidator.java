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

package org.teiid.query.validator;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.metadata.ParameterInfo;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.process.multisource.MultiSourceMetadataWrapper;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestValidator {

    public static TransformationMetadata exampleMetadata() {
        MetadataStore metadataStore = new MetadataStore();
        // Create metadata objects
        Schema modelObj = RealMetadataFactory.createPhysicalModel("test", metadataStore); //$NON-NLS-1$
        Schema vModelObj2 = RealMetadataFactory.createVirtualModel("vTest", metadataStore);  //$NON-NLS-1$
        Table groupObj = RealMetadataFactory.createPhysicalGroup("group", modelObj);         //$NON-NLS-1$
        Column elemObj0 = RealMetadataFactory.createElement("e0", groupObj, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        elemObj0.setNullType(NullType.No_Nulls);
        Column elemObj1 = RealMetadataFactory.createElement("e1", groupObj, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        elemObj1.setSelectable(false);
        Column elemObj2 = RealMetadataFactory.createElement("e2", groupObj, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        elemObj2.setSearchType(SearchType.Like_Only);
        Column elemObj3 = RealMetadataFactory.createElement("e3", groupObj, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        elemObj3.setSearchType(SearchType.All_Except_Like);

        Table group2Obj = RealMetadataFactory.createPhysicalGroup("group2", modelObj);         //$NON-NLS-1$
        Column elemObj2_0 = RealMetadataFactory.createElement("e0", group2Obj, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        elemObj2_0.setUpdatable(false);
        RealMetadataFactory.createElement("e1", group2Obj, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        Column elemObj2_2 = RealMetadataFactory.createElement("e2", group2Obj, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        elemObj2_2.setUpdatable(false);

        Table group3Obj = RealMetadataFactory.createPhysicalGroup("group3", modelObj);         //$NON-NLS-1$
        group3Obj.setSupportsUpdate(false);
        RealMetadataFactory.createElement("e0", group3Obj, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        RealMetadataFactory.createElement("e1", group3Obj, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        RealMetadataFactory.createElement("e2", group3Obj, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$

        // Create virtual group & elements.
        QueryNode vNode = new QueryNode("SELECT * FROM test.group WHERE e2 = 'x'"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vGroup = RealMetadataFactory.createVirtualGroup("vGroup", vModelObj2, vNode);         //$NON-NLS-1$
        RealMetadataFactory.createElements(vGroup,
            new String[] { "e0", "e1", "e2", "e3" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });

        QueryNode vNode2 = new QueryNode("SELECT * FROM test.group"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vGroup2 = RealMetadataFactory.createVirtualGroup("vMap", vModelObj2, vNode2);         //$NON-NLS-1$
        List<Column> vGroupE2 = RealMetadataFactory.createElements(vGroup2,
            new String[] { "e0", "e1", "e2", "e3" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING });
        vGroupE2.get(0).setNullType(NullType.No_Nulls);
        vGroupE2.get(1).setSelectable(false);
        vGroupE2.get(2).setSearchType(SearchType.Like_Only);
        vGroupE2.get(3).setSearchType(SearchType.All_Except_Like);

        return RealMetadataFactory.createTransformationMetadata(metadataStore, "example");
    }

    public TransformationMetadata exampleMetadata1() {
        MetadataStore metadataStore = new MetadataStore();
        // Create metadata objects
        Schema modelObj = RealMetadataFactory.createPhysicalModel("test", metadataStore); //$NON-NLS-1$
        Table groupObj = RealMetadataFactory.createPhysicalGroup("group", modelObj);         //$NON-NLS-1$

        Column elemObj0 = RealMetadataFactory.createElement("e0", groupObj, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        Column elemObj1 = RealMetadataFactory.createElement("e1", groupObj, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        Column elemObj2 = RealMetadataFactory.createElement("e2", groupObj, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        RealMetadataFactory.createElement("e3", groupObj, DataTypeManager.DefaultDataTypes.STRING);         //$NON-NLS-1$

        elemObj0.setNullType(NullType.No_Nulls);

        elemObj1.setNullType(NullType.Nullable);
        elemObj1.setDefaultValue(Boolean.TRUE.toString());

        elemObj2.setNullType(NullType.Nullable);
        elemObj2.setDefaultValue(Boolean.FALSE.toString());

        return RealMetadataFactory.createTransformationMetadata(metadataStore, "example1");
    }

    /**
     * Group has element with type object
     * @return QueryMetadataInterface
     */
    public static TransformationMetadata exampleMetadata2() {
        MetadataStore metadataStore = new MetadataStore();
        // Create metadata objects
        Schema modelObj = RealMetadataFactory.createPhysicalModel("test", metadataStore); //$NON-NLS-1$
        Table groupObj = RealMetadataFactory.createPhysicalGroup("group", modelObj); //$NON-NLS-1$

        RealMetadataFactory.createElements(groupObj, new String[] {
            "e0", "e1", "e2", "e3", "e4", "e5" //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
        }, new String[] {
            DataTypeManager.DefaultDataTypes.INTEGER,
            DataTypeManager.DefaultDataTypes.STRING,
            DataTypeManager.DefaultDataTypes.OBJECT,
            DataTypeManager.DefaultDataTypes.BLOB,
            DataTypeManager.DefaultDataTypes.CLOB,
            DataTypeManager.DefaultDataTypes.XML,
        });

        return RealMetadataFactory.createTransformationMetadata(metadataStore, "example2");
    }

    public static TransformationMetadata exampleMetadata3() {
        MetadataStore metadataStore = new MetadataStore();
        // Create metadata objects
        Schema modelObj = RealMetadataFactory.createPhysicalModel("test", metadataStore); //$NON-NLS-1$
        Table groupObj = RealMetadataFactory.createPhysicalGroup("group", modelObj);         //$NON-NLS-1$

        RealMetadataFactory.createElement("e0", groupObj, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        Column elemObj1 = RealMetadataFactory.createElement("e1", groupObj, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$

        elemObj1.setNullType(NullType.No_Nulls);
        elemObj1.setDefaultValue(Boolean.FALSE.toString());
        elemObj1.setAutoIncremented(true);
        elemObj1.setNameInSource("e1:SEQUENCE=MYSEQUENCE.nextVal"); //$NON-NLS-1$

        return RealMetadataFactory.createTransformationMetadata(metadataStore, "example3");
    }

    public static TransformationMetadata exampleMetadata4() {
        MetadataStore metadataStore = new MetadataStore();
        // Create metadata objects
        Schema modelObj = RealMetadataFactory.createPhysicalModel("test", metadataStore);  //$NON-NLS-1$
        Table groupObj = RealMetadataFactory.createPhysicalGroup("group", modelObj); //$NON-NLS-1$
        RealMetadataFactory.createElement("e0", groupObj, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        RealMetadataFactory.createElement("e1", groupObj, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        RealMetadataFactory.createElement("e2", groupObj, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        Schema vModelObj = RealMetadataFactory.createVirtualModel("vTest", metadataStore);  //$NON-NLS-1$
        QueryNode vNode = new QueryNode("SELECT * FROM test.group"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vGroupObj = RealMetadataFactory.createVirtualGroup("vGroup", vModelObj, vNode); //$NON-NLS-1$
        Column vElemObj0 = RealMetadataFactory.createElement("e0", vGroupObj, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        Column vElemObj1 = RealMetadataFactory.createElement("e1", vGroupObj, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        RealMetadataFactory.createElement("e2", vGroupObj, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        List<Column> elements = new ArrayList<Column>(2);
        elements.add(vElemObj0);
        elements.add(vElemObj1);
        RealMetadataFactory.createAccessPattern("ap1", vGroupObj, elements); //e1 //$NON-NLS-1$

        QueryNode vNode2 = new QueryNode("SELECT * FROM vTest.vGroup"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vGroupObj2 = RealMetadataFactory.createVirtualGroup("vGroup2", vModelObj, vNode2); //$NON-NLS-1$
        Column vElemObj20 = RealMetadataFactory.createElement("e0", vGroupObj2, DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        Column vElemObj21 = RealMetadataFactory.createElement("e1", vGroupObj2, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        RealMetadataFactory.createElement("e2", vGroupObj2, DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
        elements = new ArrayList<Column>(2);
        elements.add(vElemObj20);
        elements.add(vElemObj21);
        RealMetadataFactory.createAccessPattern("vTest.vGroup2.ap1", vGroupObj2, elements); //e1 //$NON-NLS-1$

        return RealMetadataFactory.createTransformationMetadata(metadataStore, "example4");
    }

    // ################################## TEST HELPERS ################################

    static Command helpResolve(String sql, QueryMetadataInterface metadata) {
        Command command = null;

        try {
            command = QueryParser.getQueryParser().parseCommand(sql);
            QueryResolver.resolveCommand(command, metadata);
        } catch(Exception e) {
            throw new TeiidRuntimeException(e);
        }

        return command;
    }

    static Command helpResolve(String sql, GroupSymbol container, int type, QueryMetadataInterface metadata) throws QueryParserException, QueryResolverException, TeiidComponentException {
        Command command = QueryParser.getQueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, container, type, metadata, false);
        return command;
    }

    public static ValidatorReport helpValidate(String sql, String[] expectedStringArray, QueryMetadataInterface metadata) {
        Command command = helpResolve(sql, metadata);

        return helpRunValidator(command, expectedStringArray, metadata);
    }

    public static ValidatorReport helpRunValidator(Command command, String[] expectedStringArray, QueryMetadataInterface metadata) {
        try {
            ValidatorReport report = Validator.validate(command, metadata);

            examineReport(command, expectedStringArray, report);
            return report;
        } catch(TeiidException e) {
            throw new TeiidRuntimeException(e);
        }
    }

    private static void examineReport(Object command,
            String[] expectedStringArray, ValidatorReport report) {
        // Get invalid objects from report
        Collection<LanguageObject> actualObjs = new ArrayList<LanguageObject>();
        report.collectInvalidObjects(actualObjs);

        // Compare expected and actual objects
        Set<String> expectedStrings = new HashSet<String>(Arrays.asList(expectedStringArray));
        Set<String> actualStrings = new HashSet<String>();
        for (LanguageObject obj : actualObjs) {
            actualStrings.add(SQLStringVisitor.getSQLString(obj));
        }

        if(expectedStrings.size() == 0 && actualStrings.size() > 0) {
            fail("Expected no failures but got some: " + report.getFailureMessage()); //$NON-NLS-1$
        } else if(actualStrings.size() == 0 && expectedStrings.size() > 0) {
            fail("Expected some failures but got none for sql = " + command); //$NON-NLS-1$
        } else {
            assertEquals("Expected and actual sets of strings are not the same: ", expectedStrings, actualStrings); //$NON-NLS-1$
        }
    }

    private void helpValidateProcedure(String procedure, String userUpdateStr, Table.TriggerEvent procedureType) {

        QueryMetadataInterface metadata = RealMetadataFactory.exampleUpdateProc(procedureType, procedure);

        try {
            validateProcedure(userUpdateStr, metadata);
        } catch(TeiidException e) {
            throw new TeiidRuntimeException(e);
        }
    }

    private void validateProcedure(String userUpdateStr,
            QueryMetadataInterface metadata) throws QueryResolverException,
            QueryMetadataException, TeiidComponentException,
            QueryValidatorException {
        ProcedureContainer command = (ProcedureContainer)helpResolve(userUpdateStr, metadata);

        Command proc = QueryResolver.expandCommand(command, metadata, AnalysisRecord.createNonRecordingRecord());

        ValidatorReport report = Validator.validate(proc, metadata);
        if(report.hasItems()) {
            throw new QueryValidatorException(report.getFailureMessage());
        }

        report = Validator.validate(command, metadata);
        if(report.hasItems()) {
            throw new QueryValidatorException(report.getFailureMessage());
        }
    }

    private void helpFailProcedure(String procedure, String userUpdateStr, Table.TriggerEvent procedureType) {

        QueryMetadataInterface metadata = RealMetadataFactory.exampleUpdateProc(procedureType, procedure);

        try {
            validateProcedure(userUpdateStr, metadata);
            fail("Expected failures for " + procedure);
        } catch (QueryValidatorException e) {
        } catch(TeiidException e) {
            throw new RuntimeException(e);
        }
    }

    // ################################## ACTUAL TESTS ################################


    @Test public void testSelectStarWhereNoElementsAreNotSelectable() {
        helpValidate("SELECT * FROM pm1.g5", new String[] {"SELECT * FROM pm1.g5"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateSelect1() {
        helpValidate("SELECT e1, e2 FROM test.group", new String[] {"e1"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateSelect2() {
        helpValidate("SELECT e2 FROM test.group", new String[] {}, exampleMetadata()); //$NON-NLS-1$
    }

    @Test public void testValidateCompare1() {
        helpValidate("SELECT e2 FROM vTest.vMap WHERE e2 = 'a'", new String[] {}, exampleMetadata()); //$NON-NLS-1$
    }

    @Test public void testValidateCompare4() {
        helpValidate("SELECT e3 FROM vTest.vMap WHERE e3 LIKE 'a'", new String[] {}, exampleMetadata()); //$NON-NLS-1$
    }

    @Test public void testValidateCompare6() {
        helpValidate("SELECT e0 FROM vTest.vMap WHERE e0 BETWEEN 1000 AND 2000", new String[] {}, exampleMetadata()); //$NON-NLS-1$
    }

    @Test public void testValidateCompareInHaving2() {
        helpValidate("SELECT e2 FROM vTest.vMap GROUP BY e2 HAVING e2 IS NULL", new String[] {}, exampleMetadata()); //$NON-NLS-1$
    }

    @Test public void testValidateCompareInHaving3() {
        helpValidate("SELECT e2 FROM vTest.vMap GROUP BY e2 HAVING e2 IN ('a')", new String[] {}, exampleMetadata()); //$NON-NLS-1$
    }

    @Test public void testValidateCompareInHaving4() {
        helpValidate("SELECT e3 FROM vTest.vMap GROUP BY e3 HAVING e3 LIKE 'a'", new String[] {}, exampleMetadata()); //$NON-NLS-1$
    }

    @Test public void testValidateCompareInHaving5() {
        helpValidate("SELECT e2 FROM vTest.vMap GROUP BY e2 HAVING e2 BETWEEN 1000 AND 2000", new String[] {}, exampleMetadata()); //$NON-NLS-1$
    }

    @Test public void testInvalidAggregate1() {
        helpValidate("SELECT SUM(e3) FROM test.group GROUP BY e2", new String[] {"SUM(e3)"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidAggregate2() {
        helpValidate("SELECT e3 FROM test.group GROUP BY e2", new String[] {"e3"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidAggregate3() {
        helpValidate("SELECT SUM(e2) FROM test.group GROUP BY e2", new String[] {"SUM(e2)"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidAggregate4() {
        helpValidate("SELECT AVG(e2) FROM test.group GROUP BY e2", new String[] {"AVG(e2)"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidAggregate5() {
        helpValidate("SELECT e1 || 'x' frOM pm1.g1 GROUP BY e2 + 1", new String[] {"e1"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidAggregate6() {
        helpValidate("SELECT e2 + 1 frOM pm1.g1 GROUP BY e2 + 1 HAVING e1 || 'x' > 0", new String[] {"e1"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidAggregate7() {
        helpValidate("SELECT StringKey, SUM(length(StringKey || 'x')) + 1 AS x FROM BQT1.SmallA GROUP BY StringKey || 'x' HAVING space(MAX(length((StringKey || 'x') || 'y'))) = '   '", //$NON-NLS-1$
                     new String[] {"StringKey"}, RealMetadataFactory.exampleBQTCached() ); //$NON-NLS-1$
    }

    @Test public void testInvalidAggregate8() {
        helpValidate("SELECT max(ObjectValue) FROM BQT1.SmallA GROUP BY StringKey", //$NON-NLS-1$
                     new String[] {"MAX(ObjectValue)"}, RealMetadataFactory.exampleBQTCached() ); //$NON-NLS-1$
    }

    @Test public void testInvalidAggregate9() {
        helpValidate("SELECT count(distinct ObjectValue) FROM BQT1.SmallA GROUP BY StringKey", //$NON-NLS-1$
                     new String[] {"COUNT(DISTINCT ObjectValue)"}, RealMetadataFactory.exampleBQTCached() ); //$NON-NLS-1$
    }

    /**
     * previously failed on stringkey, which is not entirely correct
     */
    @Test public void testInvalidAggregate10() {
        helpValidate("SELECT xmlparse(document stringkey) FROM BQT1.SmallA GROUP BY xmlparse(document stringkey)", //$NON-NLS-1$
                     new String[] {"XMLPARSE(DOCUMENT stringkey)"}, RealMetadataFactory.exampleBQTCached() ); //$NON-NLS-1$
    }

    @Test public void testInvalidAggregateIssue190644() {
        helpValidate("SELECT e3 + 1 from pm1.g1 GROUP BY e2 + 1 HAVING e2 + 1 = 5", new String[] {"e3"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidAggregate1() {
        helpValidate("SELECT (e2 + 1) * 2 frOM pm1.g1 GROUP BY e2 + 1", new String[] {}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testValidAggregate2() {
        helpValidate("SELECT e2 + 1 frOM pm1.g1 GROUP BY e2 + 1", new String[] {}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testValidAggregate3() {
        helpValidate("SELECT sum (IntKey), case when IntKey>=5000 then '5000 +' else '0-999' end " + //$NON-NLS-1$
            "FROM BQT1.SmallA GROUP BY case when IntKey>=5000 then '5000 +' else '0-999' end", //$NON-NLS-1$
            new String[] {}, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testValidAggregate4() {
        helpValidate("SELECT max(e1), e2 is null from pm1.g1 GROUP BY e2 is null", new String[] {}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testInvalidHaving1() {
        helpValidate("SELECT e3 FROM test.group HAVING e3 > 0", new String[] {"e3"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidHaving2() {
        helpValidate("SELECT e3 FROM test.group HAVING concat(e3,'a') > 0", new String[] {"e3"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testNestedAggregateInHaving() {
        helpValidate("SELECT e0 FROM test.group GROUP BY e0 HAVING SUM(COUNT(e0)) > 0", new String[] {"COUNT(e0)"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testNestedAggregateInSelect() {
        helpValidate("SELECT SUM(COUNT(e0)) FROM test.group GROUP BY e0", new String[] {"COUNT(e0)"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateCaseInGroupBy() {
        helpValidate("SELECT SUM(e2) FROM pm1.g1 GROUP BY CASE e2 WHEN 0 THEN 1 ELSE 2 END", new String[] {}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testValidateFunctionInGroupBy() {
        helpValidate("SELECT SUM(e2) FROM pm1.g1 GROUP BY (e2 + 1)", new String[] {}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testInvalidScalarSubqueryInGroupBy() {
        helpValidate("SELECT COUNT(*) FROM pm1.g1 GROUP BY (SELECT 1)", new String[] { "(SELECT 1)" }, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidConstantInGroupBy() {
        helpValidate("SELECT COUNT(*) FROM pm1.g1 GROUP BY 1", new String[] { "1" }, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidReferenceInGroupBy() {
        helpValidate("SELECT COUNT(*) FROM pm1.g1 GROUP BY ?", new String[] { "?" }, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateObjectType1() {
        helpValidate("SELECT DISTINCT * FROM test.group", new String[] {"test.\"group\".e2", "test.\"group\".e3", "test.\"group\".e4", "test.\"group\".e5"}, exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }

    @Test public void testValidateObjectType2() {
        helpValidate("SELECT * FROM test.group ORDER BY e1, e2", new String[] {"e2"}, exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateObjectType3() {
        helpValidate("SELECT e2 AS x FROM test.group ORDER BY x", new String[] {"x"}, exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateNonComparableType() {
        helpValidate("SELECT e3 FROM test.group ORDER BY e3", new String[] {"e3"}, exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateNonComparableType1() {
        helpValidate("SELECT e3 FROM test.group union SELECT e3 FROM test.group", new String[] {"e3"}, exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateNonComparableType2() {
        helpValidate("SELECT e3 FROM test.group GROUP BY e3", new String[] {"e3"}, exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateNonComparableType3() {
        helpValidate("SELECT e3 FROM test.group intersect SELECT e3 FROM test.group", new String[] {"e3"}, exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateNonComparableType4() {
        helpValidate("SELECT e3 FROM test.group except SELECT e3 FROM test.group", new String[] {"e3"}, exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateIntersectAll() {
        helpValidate("SELECT e3 FROM pm1.g1 intersect all SELECT e3 FROM pm1.g1", new String[] {"SELECT e3 FROM pm1.g1 INTERSECT ALL SELECT e3 FROM pm1.g1"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateSetSelectInto() {
        helpValidate("SELECT e3 into #temp FROM pm1.g1 intersect all SELECT e3 FROM pm1.g1", new String[] {"SELECT e3 INTO #temp FROM pm1.g1 INTERSECT ALL SELECT e3 FROM pm1.g1"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInsert1() {
        helpValidate("INSERT INTO test.group (e0) VALUES (null)", new String[] {"e0"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // non-null, no-default elements not left
    @Test public void testInsert4() throws Exception {
        QueryMetadataInterface metadata = exampleMetadata1();

        Command command = QueryParser.getQueryParser().parseCommand("INSERT INTO test.group (e0) VALUES (2)"); //$NON-NLS-1$

        QueryResolver.resolveCommand(command, metadata);

        helpRunValidator(command, new String[] {}, metadata);
    }

    // non-null, no-default elements left
    @Test public void testInsert5() throws Exception {
        QueryMetadataInterface metadata = exampleMetadata1();

        Command command = QueryParser.getQueryParser().parseCommand("INSERT INTO test.group (e1, e2) VALUES ('x', 'y')"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, metadata);

        helpRunValidator(command, new String[] {"test.\"group\".e0"}, metadata); //$NON-NLS-1$
    }

    /**
     * Non-updatable columns should not be required
     * @throws Exception
     */
    @Test public void testInsert6() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.fromDDL("create foreign table contacts "
                + "(name string options (updatable false), first string)"
                + " options (updatable true);", "x", "y");

        Command command = QueryParser.getQueryParser().parseCommand("INSERT INTO contacts (first) VALUES ('x')"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, metadata);

        helpRunValidator(command, new String[] {}, metadata); //$NON-NLS-1$
    }

    @Test public void testValidateInsertElements1() throws Exception {
        QueryMetadataInterface metadata = exampleMetadata();

        Command command = QueryParser.getQueryParser().parseCommand("INSERT INTO test.group2 (e0, e1, e2) VALUES (5, 'x', 'y')"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, metadata);

        helpRunValidator(command, new String[] {"e2", "e0"}, metadata); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateInsertElements2() throws Exception {
        QueryMetadataInterface metadata = exampleMetadata();

        Command command = QueryParser.getQueryParser().parseCommand("INSERT INTO test.group2 (e1) VALUES ('y')"); //$NON-NLS-1$

        QueryResolver.resolveCommand(command, metadata);

        helpRunValidator(command, new String[] {}, metadata);
    }

    @Test public void testValidateInsertElements3_autoIncNotRequired() throws Exception {
        helpValidate("INSERT INTO test.group (e0) VALUES (1)", new String[] {}, exampleMetadata3()); //$NON-NLS-1$
    }

    @Test public void testUpdate1() {
        helpValidate("UPDATE test.group SET e0=null", new String[] {"e0"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testUpdate2() {
        helpValidate("UPDATE test.group SET e0=1, e0=2", new String[] {"e0"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateUpdateElements1() throws Exception {
        QueryMetadataInterface metadata = exampleMetadata();

        Command command = QueryParser.getQueryParser().parseCommand("UPDATE test.group2 SET e0 = 5, e1 = 'x', e2 = 'y'"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, metadata);

        helpRunValidator(command, new String[] {"e2", "e0"}, metadata); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateUpdateElements2() throws Exception {
        QueryMetadataInterface metadata = exampleMetadata();

        Command command = QueryParser.getQueryParser().parseCommand("UPDATE test.group2 SET e1 = 'x'"); //$NON-NLS-1$
        QueryResolver.resolveCommand(command, metadata);

        helpRunValidator(command, new String[] {}, metadata);
    }

    @Test public void testXMLSerializeEncoding() {
        helpValidate("SELECT xmlserialize(? AS CLOB ENCODING \"UTF-8\")", new String[] {"XMLSERIALIZE(? AS CLOB ENCODING \"UTF-8\")"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testXMLSerializeEncoding1() {
        helpValidate("SELECT xmlserialize(? AS BLOB ENCODING \"UTF-8\" INCLUDING XMLDECLARATION)", new String[] {}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testXMLSerializeEncoding2() {
        helpValidate("SELECT xmlserialize(? AS BLOB ENCODING \"UTF-75\" INCLUDING XMLDECLARATION)", new String[] {"XMLSERIALIZE(? AS BLOB ENCODING \"UTF-75\" INCLUDING XMLDECLARATION)"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testValidateSubquery1() {
        helpValidate("SELECT e2 FROM (SELECT e2 FROM vTest.vMap WHERE e2 = 'a') AS x", new String[] {}, exampleMetadata()); //$NON-NLS-1$
    }

    @Test public void testValidateSubquery2() {
        helpValidate("SELECT e2 FROM (SELECT e3 FROM vTest.vMap) AS x, vTest.vMap WHERE e2 = 'a'", new String[] {}, exampleMetadata()); //$NON-NLS-1$
    }

    @Test public void testValidateSubquery3() {
        helpValidate("SELECT * FROM pm1.g1, (EXEC pm1.sq1( )) AS alias", new String[] {}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testValidateUnionWithSubquery() {
        helpValidate("SELECT e2 FROM test.group2 union all SELECT e3 FROM test.group union all select * from (SELECT e1 FROM test.group) as subquery1", new String[] {"e1"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateExistsSubquery() {
        helpValidate("SELECT e2 FROM test.group2 WHERE EXISTS (SELECT e2 FROM vTest.vMap WHERE e2 = 'a')", new String[] {}, exampleMetadata()); //$NON-NLS-1$
    }

    @Test public void testValidateScalarSubquery() {
        helpValidate("SELECT e2, (SELECT e1 FROM vTest.vMap WHERE e2 = '3') FROM test.group2", new String[] {"e1"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateAnyCompareSubquery() {
        helpValidate("SELECT e2 FROM test.group2 WHERE e1 < ANY (SELECT e1 FROM test.group)", new String[] {"e1"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateAllCompareSubquery() {
        helpValidate("SELECT e2 FROM test.group2 WHERE e1 = ALL (SELECT e1 FROM test.group)", new String[] {"e1"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateSomeCompareSubquery() {
        helpValidate("SELECT e2 FROM test.group2 WHERE e1 <= SOME (SELECT e1 FROM test.group)", new String[] {"e1"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateCompareSubquery() {
        helpValidate("SELECT e2 FROM test.group2 WHERE e1 >= (SELECT e1 FROM test.group WHERE e1 = 1)", new String[] {"e1"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateInClauseSubquery() {
        helpValidate("SELECT e2 FROM test.group2 WHERE e1 IN (SELECT e1 FROM test.group)", new String[] {"e1"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateInClauseSubqueryPasses() {
        helpValidate("SELECT e2 FROM test.group2 WHERE e1 IN (SELECT e2 FROM test.group)", new String[] {}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateInClauseSubqueryArray() {
        helpValidate("SELECT e2 FROM pm1.g1 WHERE (e2, e3) IN (SELECT e2, e3 FROM pm1.g2)", new String[] {}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateExec1() {
        helpValidate("EXEC pm1.sq1()", new String[] {}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    // valid variable declared
    @Test public void testCreateUpdateProcedure4() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpValidateProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // validating AssignmentStatement, more than one project symbol on the
    // command
    @Test public void testCreateUpdateProcedure11() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2, pm1.g1.e1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // validating AssignmentStatement, more than one project symbol on the
    // command
    @Test public void testCreateUpdateProcedure12() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = Select pm1.g1.e2, pm1.g1.e1 from pm1.g1;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userUpdateStr = "UPDATE vm1.g1 SET e1='x'"; //$NON-NLS-1$

        helpFailProcedure(procedure, userUpdateStr,
                                     Table.TriggerEvent.UPDATE);
    }

    // using aggregate function within a procedure - defect #8394
    @Test public void testCreateUpdateProcedure31() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string MaxTran;\n"; //$NON-NLS-1$
        procedure = procedure + "MaxTran = SELECT MAX(e1) FROM pm1.g1;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where y = 1"; //$NON-NLS-1$

        helpValidateProcedure(procedure, userQuery,
                Table.TriggerEvent.UPDATE);
    }

    // assigning null values to known datatype variable
    @Test public void testCreateUpdateProcedure32() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE string var;\n"; //$NON-NLS-1$
        procedure = procedure + "var = null;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where y = 1"; //$NON-NLS-1$

        helpValidateProcedure(procedure, userQuery,
                Table.TriggerEvent.UPDATE);
    }

    @Test public void testDefect13643() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "DECLARE integer var1;\n"; //$NON-NLS-1$
        procedure = procedure + "LOOP ON (SELECT * FROM pm1.g1) AS myCursor\n"; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "var1 = SELECT COUNT(*) FROM myCursor;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where y = 1"; //$NON-NLS-1$

        helpFailProcedure(procedure, userQuery,
                Table.TriggerEvent.UPDATE);
    }

    @Test public void testValidHaving() {
        helpValidate(
            "SELECT intnum " + //$NON-NLS-1$
            "FROM bqt1.smalla " + //$NON-NLS-1$
            "GROUP BY intnum " + //$NON-NLS-1$
            "HAVING SUM(floatnum) > 1",  //$NON-NLS-1$
            new String[] { }, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testValidHaving2() {
        String sql =  "SELECT intkey FROM bqt1.smalla WHERE intkey = 1 " + //$NON-NLS-1$
            "GROUP BY intkey HAVING intkey = 1";         //$NON-NLS-1$
        helpValidate(sql, new String[] {}, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testVirtualProcedure(){
          helpValidate("EXEC pm1.vsp1()", new String[] { }, RealMetadataFactory.example1Cached());  //$NON-NLS-1$
    }

    @Test public void testSelectWithNoFrom() {
        helpValidate("SELECT 5", new String[] {}, exampleMetadata()); //$NON-NLS-1$
    }

    @Test public void testSelectIntoTempGroup() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1, e2, e3, e4 INTO #myTempTable FROM pm1.g2;\n";         //$NON-NLS-1$
        procedure = procedure + "SELECT COUNT(*) FROM #myTempTable;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where y = 1"; //$NON-NLS-1$

        helpValidateProcedure(procedure, userQuery,
                Table.TriggerEvent.UPDATE);
    }

    /**
     * Defect 24346
     */
    @Test public void testInvalidSelectIntoTempGroup() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1, e2, e3, e4 INTO #myTempTable FROM pm1.g2;\n";         //$NON-NLS-1$
        procedure = procedure + "SELECT e1, e2, e3 INTO #myTempTable FROM pm1.g2;\n";         //$NON-NLS-1$
        procedure = procedure + "SELECT COUNT(*) FROM #myTempTable;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where y = 1"; //$NON-NLS-1$

        helpFailProcedure(procedure, userQuery,
                Table.TriggerEvent.UPDATE);
    }

    @Test public void testInvalidSelectIntoTempGroup1() {
        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "create local temporary table #myTempTable (e1 integer);\n";         //$NON-NLS-1$
        procedure = procedure + "SELECT e1 INTO #myTempTable FROM pm1.g2;\n";         //$NON-NLS-1$
        procedure = procedure + "SELECT COUNT(*) FROM #myTempTable;\n"; //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where y = 1"; //$NON-NLS-1$

        helpFailProcedure(procedure, userQuery,
                Table.TriggerEvent.UPDATE);
    }


    @Test public void testSelectIntoPhysicalGroup() {
        helpValidate("SELECT e1, e2, e3, e4 INTO pm1.g1 FROM pm1.g2", new String[] { }, RealMetadataFactory.example1Cached()); //$NON-NLS-1$

        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1, e2, e3, e4 INTO pm1.g1 FROM pm1.g2;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where y = 1"; //$NON-NLS-1$

        helpValidateProcedure(procedure, userQuery,
                Table.TriggerEvent.UPDATE);
    }

    @Test public void testSelectIntoPhysicalGroupNotUpdateable_Defect16857() {
        helpValidate("SELECT e0, e1, e2 INTO test.group3 FROM test.group2", new String[] {"test.group3"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSelectIntoElementsNotUpdateable() {
        helpValidate("SELECT e0, e1, e2 INTO test.group2 FROM test.group3", new String[] {"test.group2"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidSelectIntoTooManyElements() {
        helpValidate("SELECT e1, e2, e3, e4, 'val' INTO pm1.g1 FROM pm1.g2", new String[] {"SELECT e1, e2, e3, e4, 'val' INTO pm1.g1 FROM pm1.g2"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$

        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1, e2, e3, e4, 'val' INTO pm1.g1 FROM pm1.g2;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where y = 1"; //$NON-NLS-1$

        helpFailProcedure(procedure, userQuery,
                Table.TriggerEvent.UPDATE);
    }

    @Test public void testInvalidSelectIntoTooFewElements() {
        helpValidate("SELECT e1, e2, e3 INTO pm1.g1 FROM pm1.g2", new String[] {"SELECT e1, e2, e3 INTO pm1.g1 FROM pm1.g2"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$

        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1, e2, e3 INTO pm1.g1 FROM pm1.g2;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where y = 1"; //$NON-NLS-1$

        helpFailProcedure(procedure, userQuery,
                Table.TriggerEvent.UPDATE);
    }

    @Test public void testInvalidSelectIntoIncorrectTypes() {
        helpValidate("SELECT e1, convert(e2, string), e3, e4 INTO pm1.g1 FROM pm1.g2", new String[] {"SELECT e1, convert(e2, string), e3, e4 INTO pm1.g1 FROM pm1.g2"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$

        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1, convert(e2, string), e3, e4 INTO pm1.g1 FROM pm1.g2;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where y = 1"; //$NON-NLS-1$

        helpFailProcedure(procedure, userQuery,
                Table.TriggerEvent.UPDATE);
    }

    @Test public void testSelectIntoWithStar() {
        helpResolve("SELECT * INTO pm1.g1 FROM pm1.g2", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testInvalidSelectIntoWithStar() {
        helpValidate("SELECT * INTO pm1.g1 FROM pm1.g2, pm1.g1", new String[] {"SELECT * INTO pm1.g1 FROM pm1.g2, pm1.g1"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$

        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT * INTO pm1.g1 FROM pm1.g2, pm1.g1;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where y = 1"; //$NON-NLS-1$

        helpFailProcedure(procedure, userQuery,
                Table.TriggerEvent.UPDATE);
    }

    @Test public void testSelectIntoVirtualGroup() {
        helpValidate("SELECT e1, e2, e3, e4 INTO vm1.g1 FROM pm1.g2", new String[] {}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$

        String procedure = "FOR EACH ROW "; //$NON-NLS-1$
        procedure = procedure + "BEGIN\n"; //$NON-NLS-1$
        procedure = procedure + "SELECT e1, e2, e3, e4 INTO vm1.g1 FROM pm1.g2;\n";         //$NON-NLS-1$
        procedure = procedure + "END\n"; //$NON-NLS-1$

        String userQuery = "UPDATE vm1.g3 SET x='x' where y = 1"; //$NON-NLS-1$

        helpValidateProcedure(procedure, userQuery,
                Table.TriggerEvent.UPDATE);
    }

    @Test public void testVirtualProcedure2(){
          helpValidate("EXEC pm1.vsp13()", new String[] { }, RealMetadataFactory.example1Cached());  //$NON-NLS-1$
    }

    //procedure that has another procedure in the transformation
    @Test public void testVirtualProcedure3(){
        helpValidate("EXEC pm1.vsp27()", new String[] { }, RealMetadataFactory.example1Cached());  //$NON-NLS-1$
    }

    @Test public void testNonEmbeddedSubcommand_defect11000() {
        helpValidate("SELECT e0 FROM vTest.vGroup", new String[0], exampleMetadata()); //$NON-NLS-1$
    }

    @Test public void testValidateObjectInComparison() throws Exception {
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE ObjectValue = 5";   //$NON-NLS-1$
        ValidatorReport report = helpValidate(sql, new String[] {"ObjectValue = 5"}, RealMetadataFactory.exampleBQTCached()); //$NON-NLS-1$
        assertEquals("Non-comparable expression of type object cannot be used in comparison: ObjectValue = 5.", report.toString()); //$NON-NLS-1$
    }

    @Test public void testValidateAssignmentWithFunctionOnParameter_InServer() throws Exception{
        String sql = "EXEC pm1.vsp36(5)";  //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        Command command = new QueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);

        // Validate
        ValidatorReport report = Validator.validate(command, metadata);
        assertEquals(0, report.getItems().size());
    }

    @Test public void testDefect9917() throws Exception{
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        String sql = "SELECT lookup('pm1.g1', 'e1a', 'e2', e2) AS x, lookup('pm1.g1', 'e4', 'e3', e3) AS y FROM pm1.g1"; //$NON-NLS-1$
        Command command = new QueryParser().parseCommand(sql);
        try{
            QueryResolver.resolveCommand(command, metadata);
            fail("Did not get exception"); //$NON-NLS-1$
        }catch(QueryResolverException e){
            //expected
        }

        sql = "SELECT lookup('pm1.g1a', 'e1', 'e2', e2) AS x, lookup('pm1.g1', 'e4', 'e3', e3) AS y FROM pm1.g1"; //$NON-NLS-1$
        command = new QueryParser().parseCommand(sql);
        try{
            QueryResolver.resolveCommand(command, metadata);
            fail("Did not get exception"); //$NON-NLS-1$
        }catch(QueryResolverException e){
            //expected
        }
    }

    @Test public void testLookupKeyElementComparable() throws Exception {
        QueryMetadataInterface metadata = exampleMetadata2();
        String sql = "SELECT lookup('test.group', 'e2', 'e3', convert(e2, blob)) AS x FROM test.group"; //$NON-NLS-1$
        Command command = QueryParser.getQueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);

        ValidatorReport report = Validator.validate(command, metadata);
        assertEquals("Non-comparable expression of type blob cannot be used as LOOKUP key columns: test.\"group\".e3.", report.toString()); //$NON-NLS-1$
    }

    @Test public void testDefect12107() throws Exception{
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        String sql = "SELECT SUM(DISTINCT lookup('pm1.g1', 'e2', 'e2', e2)) FROM pm1.g1"; //$NON-NLS-1$
        Command command = helpResolve(sql, metadata);
        sql = "SELECT SUM(DISTINCT lookup('pm1.g1', 'e3', 'e2', e2)) FROM pm1.g1"; //$NON-NLS-1$
        command = helpResolve(sql, metadata);
        ValidatorReport report = Validator.validate(command, metadata);
        assertEquals("The aggregate function SUM cannot be used with non-numeric expressions: SUM(DISTINCT lookup('pm1.g1', 'e3', 'e2', e2))", report.toString()); //$NON-NLS-1$
    }

    private ValidatorReport helpValidateInModeler(String procName, String procSql, QueryMetadataInterface metadata) throws Exception {
        Command command = QueryParser.getQueryParser().parseCommand(procSql);

        GroupSymbol group = new GroupSymbol(procName);
        QueryResolver.resolveCommand(command, group, Command.TYPE_STORED_PROCEDURE, metadata, true);

        // Validate
        return Validator.validate(command, metadata);
    }

    @Test public void testValidateDynamicCommandWithNonTempGroup_InModeler() throws Exception{
        // SQL is same as pm1.vsp36() in example1
        String sql = "CREATE VIRTUAL PROCEDURE BEGIN execute string 'select ' || '1' as X integer into pm1.g3; END";  //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        // Validate
        ValidatorReport report = helpValidateInModeler("pm1.vsp36", sql, metadata);  //$NON-NLS-1$
        assertEquals(1, report.getItems().size());
        assertEquals("Wrong number of elements being SELECTed INTO the target table. Expected 4 elements, but was 1.", report.toString()); //$NON-NLS-1$
    }

    @Test public void testValidateInModeler() throws Exception{
        // SQL is same as pm1.vsp36() in example1
        String sql = "CREATE VIRTUAL PROCEDURE BEGIN select 1, 2; END";  //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        Command command = QueryParser.getQueryParser().parseCommand(sql);
        GroupSymbol group = new GroupSymbol("pm1.vsp36");
        QueryResolver.resolveCommand(command, group, Command.TYPE_STORED_PROCEDURE, metadata, true);

        assertEquals(2, command.getResultSetColumns().size());
    }

    @Test public void testDynamicDupUsing() throws Exception {
        String sql = "CREATE VIRTUAL PROCEDURE BEGIN execute string 'select ' || '1' as X integer into #temp using id=1, id=2; END";  //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        // Validate
        ValidatorReport report = helpValidateInModeler("pm1.vsp36", sql, metadata);  //$NON-NLS-1$
        assertEquals(1, report.getItems().size());
        assertEquals("Elements cannot appear more than once in a SET or USING clause.  The following elements are duplicated: [DVARS.id]", report.toString()); //$NON-NLS-1$
    }

    @Test public void testValidateAssignmentWithFunctionOnParameter_InModeler() throws Exception{
        // SQL is same as pm1.vsp36() in example1
        String sql = "CREATE VIRTUAL PROCEDURE BEGIN DECLARE integer x; x = pm1.vsp36.param1 * 2; SELECT x; END";  //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        // Validate
        ValidatorReport report = helpValidateInModeler("pm1.vsp36", sql, metadata);  //$NON-NLS-1$
        assertEquals(0, report.getItems().size());
    }

    @Test public void testDefect12533() {
        String sql = "SELECT BQT1.SmallA.DateValue, BQT2.SmallB.ObjectValue FROM BQT1.SmallA, BQT2.SmallB " +  //$NON-NLS-1$
            "WHERE BQT1.SmallA.DateValue = BQT2.SmallB.DateValue AND BQT1.SmallA.ObjectValue = BQT2.SmallB.ObjectValue " + //$NON-NLS-1$
            "AND BQT1.SmallA.IntKey < 30 AND BQT2.SmallB.IntKey < 30 ORDER BY BQT1.SmallA.DateValue"; //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        // Validate
        helpValidate(sql, new String[] {"BQT1.SmallA.ObjectValue = BQT2.SmallB.ObjectValue"}, metadata);  //$NON-NLS-1$
    }

    @Test public void testDefect16772() throws Exception{
        String sql = "CREATE VIRTUAL PROCEDURE BEGIN IF (pm1.vsp42.param1 > 0) SELECT 1 AS x; ELSE SELECT 0 AS x; END"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        // Validate
        ValidatorReport report = helpValidateInModeler("pm1.vsp42", sql, metadata);  //$NON-NLS-1$
        assertEquals("Expected report to have no validation failures", false, report.hasItems()); //$NON-NLS-1$
    }

    @Test public void testDupLabel() throws Exception{
        String sql = "CREATE VIRTUAL PROCEDURE BEGIN IF (pm1.vsp42.param1 > 0) x : begin SELECT 1 AS x; x: begin atomic select 2 as x; end end END"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        // Validate
        ValidatorReport report = helpValidateInModeler("pm1.vsp42", sql, metadata);  //$NON-NLS-1$
        examineReport(sql, new String[] {"x : BEGIN ATOMIC\nSELECT 2 AS x;\nEND"}, report);
    }

    @Test public void testInvalidContinue() throws Exception{
        String sql = "CREATE VIRTUAL PROCEDURE BEGIN continue; END"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        // Validate
        ValidatorReport report = helpValidateInModeler("pm1.vsp42", sql, metadata);  //$NON-NLS-1$
        examineReport(sql, new String[] {"CONTINUE;"}, report);
    }

    @Test public void testInvalidLabel() throws Exception{
        String sql = "CREATE VIRTUAL PROCEDURE BEGIN IF (pm1.vsp42.param1 > 0) x : begin continue y; end END"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        // Validate
        ValidatorReport report = helpValidateInModeler("pm1.vsp42", sql, metadata);  //$NON-NLS-1$
        examineReport(sql, new String[] {"CONTINUE y;"}, report);
    }

    @Test public void testNonQueryAgg() throws Exception{
        String sql = "CREATE VIRTUAL PROCEDURE BEGIN IF (max(pm1.vsp42.param1) > 0) SELECT 1 AS x; ELSE SELECT 0 AS x; END"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        // Validate
        ValidatorReport report = helpValidateInModeler("pm1.vsp42", sql, metadata);  //$NON-NLS-1$
        examineReport(sql, new String[] {"MAX(pm1.vsp42.param1)"}, report);
    }

    @Test public void testDefect14886() throws Exception{
        String sql = "CREATE VIRTUAL PROCEDURE BEGIN END";  //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        Command command = new QueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);

        // Validate
        ValidatorReport report = Validator.validate(command, metadata);
        // Validate
        assertEquals(0, report.getItems().size());
    }

    @Test public void testDefect21389() throws Exception{
        String sql = "CREATE VIRTUAL PROCEDURE BEGIN SELECT * INTO #temptable FROM pm1.g1; INSERT INTO #temptable (e1) VALUES ('a'); END"; //$NON-NLS-1$
        TransformationMetadata metadata = RealMetadataFactory.example1();
        Column c = metadata.getElementID("pm1.g1.e1"); //$NON-NLS-1$
        c.setUpdatable(false);

        Command command = new QueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);

        // Validate
        ValidatorReport report = Validator.validate(command, metadata);
        // Validate
        assertEquals(0, report.getItems().size());
    }

    @Test public void testMakeNotDep() {
        helpValidate("select group2.e1, group3.e2 from group2, group3 WHERE group2.e0 = group3.e0 OPTION MAKENOTDEP group2, group3", new String[0], exampleMetadata()); //$NON-NLS-1$
    }
    @Test public void testInvalidMakeNotDep() {
        helpValidate("select group2.e1, group3.e2 from group2, group3 WHERE group2.e0 = group3.e0 OPTION MAKEDEP group2 MAKENOTDEP group2, group3", new String[] {"OPTION MAKEDEP group2 MAKENOTDEP group2, group3"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     * Test case 4237.  This test simulates the way the modeler transformation
     * panel uses the query resolver and validator to validate a transformation for
     * a virtual procedure.  The modeler has to supply external metadata for the
     * virtual procedure group and parameter names (simulated in this test).
     *
     * This virtual procedure calls a physical stored procedure directly.
     */
    @Test public void testCase4237() throws Exception {

        QueryMetadataInterface metadata = helpCreateCase4237VirtualProcedureMetadata();

        String sql = "CREATE VIRTUAL PROCEDURE BEGIN EXEC pm1.sp(vm1.sp.in1); END"; //$NON-NLS-1$
        Command command = helpResolve(sql, new GroupSymbol("vm1.sp"), Command.TYPE_STORED_PROCEDURE, metadata);
        helpRunValidator(command, new String[0], metadata);
    }

    /**
     * This test was already working before the case was logged, due for some reason
     * to the exec() statement being inside an inline view.  This is a control test.
     */
    @Test public void testCase4237InlineView() throws Exception {

        QueryMetadataInterface metadata = helpCreateCase4237VirtualProcedureMetadata();

        String sql = "CREATE VIRTUAL PROCEDURE BEGIN SELECT * FROM (EXEC pm1.sp(vm1.sp.in1)) AS FOO; END"; //$NON-NLS-1$
        Command command = helpResolve(sql, new GroupSymbol("vm1.sp"), Command.TYPE_STORED_PROCEDURE, metadata);
        helpRunValidator(command, new String[0], metadata);
    }

    /**
     * Create fake metadata for this case.  Need a physical stored procedure and
     * a virtual stored procedure which calls the physical one.
     * @return
     */
    private TransformationMetadata helpCreateCase4237VirtualProcedureMetadata() {
        MetadataStore metadataStore = new MetadataStore();
        Schema physicalModel = RealMetadataFactory.createPhysicalModel("pm1", metadataStore); //$NON-NLS-1$
        ColumnSet<Procedure> resultSet = RealMetadataFactory.createResultSet("pm1.rs", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter inParam = RealMetadataFactory.createParameter("in", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        Procedure storedProcedure = RealMetadataFactory.createStoredProcedure("sp", physicalModel, Arrays.asList(inParam));  //$NON-NLS-1$ //$NON-NLS-2$
        storedProcedure.setResultSet(resultSet);

        Schema virtualModel = RealMetadataFactory.createVirtualModel("vm1", metadataStore); //$NON-NLS-1$
        ColumnSet<Procedure> virtualResultSet = RealMetadataFactory.createResultSet("vm1.rs", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ProcedureParameter virtualInParam = RealMetadataFactory.createParameter("in1", ParameterInfo.IN, DataTypeManager.DefaultDataTypes.STRING);  //$NON-NLS-1$
        QueryNode queryNode = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN EXEC pm1.sp(vm1.sp.in1); END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure virtualStoredProcedure = RealMetadataFactory.createVirtualProcedure("sp", virtualModel, Arrays.asList(virtualInParam), queryNode);  //$NON-NLS-1$
        virtualStoredProcedure.setResultSet(virtualResultSet);

        return RealMetadataFactory.createTransformationMetadata(metadataStore, "case4237");
    }

    @Test public void testSelectIntoWithNull() {
        helpValidate("SELECT null, null, null, null INTO pm1.g1 FROM pm1.g2", new String[] {}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testCreateWithNonSortablePrimaryKey() {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        Command command = helpResolve("create local temporary table x (column1 string, column2 clob, primary key (column2))", metadata); //$NON-NLS-1$
        helpRunValidator(command, new String[] {"column2"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testDropNonTemporary() {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        Command command = helpResolve("drop table pm1.g1", metadata); //$NON-NLS-1$
        helpRunValidator(command, new String[] {command.toString()}, RealMetadataFactory.example1Cached());
    }

    @Test public void testInsertIntoVirtualWithQuery() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        Command command = helpResolve("insert into vm1.g1 select 1, 2, true, 3", metadata); //$NON-NLS-1$
        ValidatorReport report = Validator.validate(command, metadata);
        assertTrue(report.getItems().isEmpty());
    }

    @Test public void testDynamicIntoDeclaredTemp() throws Exception {
        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE  ") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("CREATE LOCAL TEMPORARY TABLE x (column1 string);") //$NON-NLS-1$
                                .append("execute string 'SELECT e1 FROM pm1.g2' as e1 string INTO x;\n") //$NON-NLS-1$
                                .append("select cast(column1 as integer) from x;\n") //$NON-NLS-1$
                                .append("END\n"); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        // Validate
        ValidatorReport report = helpValidateInModeler("pm1.vsp36", procedure.toString(), metadata);  //$NON-NLS-1$
        assertEquals(report.toString(), 0, report.getItems().size());
    }

    @Test public void testVariablesGroupSelect() {
        String procedure = "CREATE VIRTUAL PROCEDURE "; //$NON-NLS-1$
        procedure += "BEGIN\n"; //$NON-NLS-1$
        procedure += "DECLARE integer VARIABLES.var1 = 1;\n"; //$NON-NLS-1$
        procedure += "select * from variables;\n"; //$NON-NLS-1$
        procedure += "END\n"; //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        Command command = helpResolve(procedure, metadata);
        helpRunValidator(command, new String[] {"variables"}, metadata); //$NON-NLS-1$
    }

    @Test public void testClobEquals() {
        TestValidator.helpValidate("SELECT * FROM test.group where e4 = '1'", new String[] {"e4 = '1'"}, TestValidator.exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /**
     *  Should not fail since the update changing set is not really criteria
     */
    @Test public void testUpdateWithClob() {
        TestValidator.helpValidate("update test.group set e4 = ?", new String[] {}, TestValidator.exampleMetadata2()); //$NON-NLS-1$
    }

    @Test public void testBlobLessThan() {
        TestValidator.helpValidate("SELECT * FROM test.group where e3 < ?", new String[] {"e3 < ?"}, TestValidator.exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateCompare2() {
        helpValidate("SELECT e2 FROM test.group WHERE e4 IS NULL", new String[] {}, exampleMetadata2()); //$NON-NLS-1$
    }

    @Test public void testValidateCompare3() {
        helpValidate("SELECT e2 FROM test.group WHERE e4 IN ('a')", new String[] {"e4 IN ('a')"}, exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateCompare5() {
        helpValidate("SELECT e2 FROM test.group WHERE e4 BETWEEN '1' AND '2'", new String[] {"e4 BETWEEN '1' AND '2'"}, exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateCompareInHaving1() {
        helpValidate("SELECT e1 FROM test.group GROUP BY e1 HAVING convert(e1, clob) = 'a'", new String[] {"convert(e1, clob) = 'a'"}, exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateNoExpressionName1() {
        helpValidate("SELECT xmlforest('1')", new String[] {"XMLFOREST('1')"}, exampleMetadata2()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTextTableNoWidth() {
        helpValidate("SELECT * from texttable(null columns x string width 1, y integer) as x", new String[] {"TEXTTABLE(null COLUMNS x string WIDTH 1, y integer) AS x"}, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testTextTableInvalidDelimiter() {
        helpValidate("SELECT * from texttable(null columns x string width 1 DELIMITER 'z') as x", new String[] {"TEXTTABLE(null COLUMNS x string WIDTH 1 DELIMITER 'z') AS x"}, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testTextTableNoRowDelimiter() {
        helpValidate("SELECT * from texttable(null columns x string NO ROW DELIMITER) as x", new String[] {"TEXTTABLE(null COLUMNS x string NO ROW DELIMITER) AS x"}, RealMetadataFactory.exampleBQTCached());
    }

    @Test public void testTextTableFixedSelector() {
        helpValidate("SELECT * from texttable(null SELECTOR 'a' columns x string width 1) as x", new String[] {}, RealMetadataFactory.exampleBQTCached());
    }

    @Ignore("this is actually handled by saxon and will show up during resolving")
    @Test public void testXMLTablePassingSameName() {
        helpValidate("select * from pm1.g1, xmltable('/' passing {x '<a/>'} as a, {x '<b/>'} as a) as x", new String[] {"xmltable('/' passing e1, e1 || 'x') as x"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testObjectTablePassing() {
        helpValidate("select * from objecttable('x' passing 'a' columns c integer 'row') as x", new String[] {"OBJECTTABLE('x' PASSING 'a' COLUMNS c integer 'row') AS x"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testObjectTablePassingSameName() {
        helpValidate("select * from objecttable('x' passing 'a' AS X, 'b' AS x columns c integer 'row') as x", new String[] {"OBJECTTABLE('x' PASSING 'a' AS X, 'b' AS x COLUMNS c integer 'row') AS x"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testObjectTableLanguage() {
        helpValidate("select * from objecttable(language 'foo!' 'x' columns c integer 'row') as x", new String[] {"OBJECTTABLE(LANGUAGE 'foo!' 'x' COLUMNS c integer 'row') AS x"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testObjectTableScript() {
        helpValidate("select * from objecttable('this. is not valid' columns c integer 'row') as x", new String[] {"OBJECTTABLE('this. is not valid' COLUMNS c integer 'row') AS x"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testQueryString() {
        helpValidate("select querystring('/', '1')", new String[] {"QUERYSTRING('/', '1')"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testXmlParse() throws Exception {
        helpValidate("select xmlparse(content e2) from pm1.g1", new String[] {"XMLPARSE(CONTENT e2)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testDecode() throws Exception {
        helpValidate("select to_bytes(e1, '?') from pm1.g1", new String[] {"to_bytes(e1, '?')"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testValidateXMLAGG() {
        helpValidate("SELECT XMLAGG(e1) from pm1.g1", new String[] {"XMLAGG(e1)"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateBooleanAgg() {
        helpValidate("SELECT EVERY(e1) from pm1.g1", new String[] {"EVERY(e1)"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateStatAgg() {
        helpValidate("SELECT stddev_pop(distinct e2) from pm1.g1", new String[] {"STDDEV_POP(DISTINCT e2)"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testValidateScalarSubqueryTooManyColumns() {
        helpValidate("SELECT e2, (SELECT e1, e2 FROM pm1.g1 WHERE e2 = '3') FROM pm1.g2", new String[] {"SELECT e1, e2 FROM pm1.g1 WHERE e2 = '3'"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidIntoSubquery() {
        helpValidate("SELECT e2, (SELECT e1, e2 INTO #x FROM pm1.g1 WHERE e2 = '3') FROM pm1.g2", new String[] {"SELECT e1, e2 INTO #x FROM pm1.g1 WHERE e2 = '3'"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidIntoSubquery1() {
        helpValidate("SELECT e2 FROM pm1.g2 WHERE EXISTS (SELECT e1, e2 INTO #x FROM pm1.g1 WHERE e2 = '3')", new String[] {"SELECT e1, e2 INTO #x FROM pm1.g1 WHERE e2 = '3'"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidIntoSubquery2() {
        helpValidate("SELECT * FROM (SELECT e1, e2 INTO #x FROM pm1.g1 WHERE e2 = '3') x", new String[] {"SELECT e1, e2 INTO #x FROM pm1.g1 WHERE e2 = '3'"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidIntoSubquery3() {
        helpValidate("SELECT e2 FROM pm1.g2 WHERE e2 in (SELECT e1, e2 INTO #x FROM pm1.g1 WHERE e2 = '3')", new String[] {"SELECT e1, e2 INTO #x FROM pm1.g1 WHERE e2 = '3'"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidIntoSubquery4() throws Exception {
        StringBuffer procedure = new StringBuffer("CREATE VIRTUAL PROCEDURE\n") //$NON-NLS-1$
                                .append("BEGIN\n") //$NON-NLS-1$
                                .append("loop on (SELECT e1, e2 INTO #x FROM pm1.g1 WHERE e2 = '3') as x\n") //$NON-NLS-1$
                                .append("BEGIN\nSELECT 1;\nEND\nSELECT 1\n;END\n"); //$NON-NLS-1$

        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        // Validate
        ValidatorReport report = helpValidateInModeler("pm1.vsp36", procedure.toString(), metadata);  //$NON-NLS-1$
        examineReport(procedure, new String[] {"SELECT e1, e2 INTO #x FROM pm1.g1 WHERE e2 = '3'"}, report);
    }

    @Test public void testDisallowUpdateOnMultisourceElement() throws Exception {
        Set<String> models = new HashSet<String>();
        models.add("pm1");
        ValidatorReport report = helpValidateInModeler("pm1.vsp36", "UPDATE PM1.G1 set SOURCE_NAME='blah'", new MultiSourceMetadataWrapper(RealMetadataFactory.example1(), models));  //$NON-NLS-1$
        assertEquals(report.toString(), 1, report.getItems().size());
    }

    @Test public void testMultiSourceInsert() throws Exception {
        Set<String> models = new HashSet<String>();
        models.add("pm1");
        helpValidate("insert into pm1.g1 (e1) values (1)", new String[] {"pm1.g1", "pm1.g1.SOURCE_NAME"}, new MultiSourceMetadataWrapper(RealMetadataFactory.example1(), models));  //$NON-NLS-1$
    }

    /**
     * TODO: this should be allowable
     */
    @Test public void testDisallowProjectIntoMultiSource() throws Exception {
        Set<String> models = new HashSet<String>();
        models.add("pm1");
        helpValidate("insert into pm1.g1 select pm1.g1.*, 1 from pm1.g1", new String[] {"pm1.g1"}, new MultiSourceMetadataWrapper(RealMetadataFactory.example1(), models));  //$NON-NLS-1$
    }

    @Test public void testTextAggEncoding() throws Exception {
        helpValidate("select textagg(for e1 encoding abc) from pm1.g1", new String[] {"TEXTAGG(FOR e1 ENCODING abc)"}, RealMetadataFactory.example1Cached());  //$NON-NLS-1$
    }

    @Test public void testTextAggHeader() throws Exception {
        helpValidate("select textagg(for e1 || 1 HEADER) from pm1.g1", new String[] {"TEXTAGG(FOR (e1 || 1) HEADER)"}, RealMetadataFactory.example1Cached());  //$NON-NLS-1$
    }

    @Test public void testMultiSourceProcValue() throws Exception {
        Set<String> models = new HashSet<String>();
        models.add("MultiModel");
        helpValidate("exec MultiModel.proc('a', (select 1))", new String[] {}, new MultiSourceMetadataWrapper(RealMetadataFactory.exampleMultiBinding(), models));  //$NON-NLS-1$
    }

    @Test public void testFailAggregateInGroupBy() {
        helpValidate("SELECT max(e1) FROM pm1.g1 GROUP BY count(e2)", new String[] {"COUNT(e2)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testFailAggregateInWhere() {
        helpValidate("SELECT e1 FROM pm1.g1 where count(e2) = 1", new String[] {"COUNT(e2)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testFailAggregateInFrom() {
        helpValidate("SELECT g2.e1 FROM pm1.g1 inner join pm1.g2 on (avg(g1.e2) = g2.e2)", new String[] {"AVG(g1.e2)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testFailAggregateFilterSubquery() {
        helpValidate("SELECT min(g1.e1) filter (where (select 1) = 1) from pm1.g1", new String[] {"(SELECT 1) = 1"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testNestedAgg() {
        helpValidate("SELECT min(g1.e1) filter (where max(e2) = 1) from pm1.g1", new String[] {"MAX(e2)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunction() {
        helpValidate("SELECT e1 from pm1.g1 where row_number() over (order by e2) = 1", new String[] {"ROW_NUMBER() OVER (ORDER BY e2)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunctionFrameOtherAnalytical() {
        helpValidate("SELECT row_number() over (order by e2 RANGE 1 FOLLOWING) from pm1.g1", new String[] {"ROW_NUMBER() OVER (ORDER BY e2 RANGE 1 FOLLOWING)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunctionFrameWithoutOrderBy() {
        helpValidate("SELECT FIRST_VALUE(e1) over (ROWS 1 FOLLOWING) from pm1.g1", new String[] {"FIRST_VALUE(e1) OVER (ROWS 1 FOLLOWING)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunctionFrameStart() {
        helpValidate("SELECT FIRST_VALUE(e1) over (order by e2 RANGE 1 FOLLOWING) from pm1.g1", new String[] {"FIRST_VALUE(e1) OVER (ORDER BY e2 RANGE 1 FOLLOWING)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunctionFrameEnd() {
        helpValidate("SELECT FIRST_VALUE(e1) over (order by e2 ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) from pm1.g1", new String[] {"FIRST_VALUE(e1) OVER (ORDER BY e2 ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunctionFrameEnd1() {
        helpValidate("SELECT FIRST_VALUE(e1) over (order by e2 ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) from pm1.g1", new String[] {"FIRST_VALUE(e1) OVER (ORDER BY e2 ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunctionFrameEnd2() {
        helpValidate("SELECT FIRST_VALUE(e1) over (order by e2 ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW) from pm1.g1", new String[] {"FIRST_VALUE(e1) OVER (ORDER BY e2 ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunctionFrameValid() {
        helpValidate("SELECT FIRST_VALUE(e1) over (order by e2 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) from pm1.g1", new String[] {}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunctionFrameRange() {
        helpValidate("SELECT FIRST_VALUE(e1) over (order by e2 RANGE 1 PRECEDING) from pm1.g1", new String[] {"FIRST_VALUE(e1) OVER (ORDER BY e2 RANGE 1 PRECEDING)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunctionFilter() {
        helpValidate("SELECT row_number() FILTER (WHERE e2 is not null) over (order by e2) = 1 from pm1.g1", new String[] {"ROW_NUMBER() FILTER(WHERE e2 IS NOT NULL) OVER (ORDER BY e2)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunction1() {
        helpValidate("SELECT 1 from pm1.g1 having row_number() over (order by e2) = 1", new String[] {"e2", "ROW_NUMBER() OVER (ORDER BY e2)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunctionWithoutOrdering() {
        helpValidate("SELECT row_number() over () from pm1.g1", new String[] {"ROW_NUMBER() OVER ()"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunctionWithNestedaggAllowed() {
        helpValidate("SELECT max(e1) over (order by max(e2)) from pm1.g1 group by e1", new String[] {}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunctionWithNestedaggAllowed1() {
        helpValidate("SELECT max(min(e1)) over (order by max(e2)) from pm1.g1 group by e1", new String[] {}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunctionWithoutFrom() {
        helpValidate("select count(*) over () as y", new String[] {"COUNT(*) OVER ()"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testWindowFunctionOrderedDistinct() {
        helpValidate("select count(distinct e1) over (order by e2) as y from pm1.g1", new String[] {"COUNT(DISTINCT e1) OVER (ORDER BY e2)"}, RealMetadataFactory.example1Cached());
    }

    @Test public void testUpdateError() {
        String userUpdateStr = "UPDATE vm1.g2 SET e1='x'"; //$NON-NLS-1$

        helpValidate(userUpdateStr, new String[]{"vm1.g2", "UPDATE vm1.g2 SET e1 = 'x'"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testInsertError() {
        String userUpdateStr = "INSERT into vm1.g2 (e1) values ('x')"; //$NON-NLS-1$

        helpValidate(userUpdateStr, new String[]{"vm1.g2", "INSERT INTO vm1.g2 (e1) VALUES ('x')"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testMergeNoKey() {
        String userUpdateStr = "MERGE into pm1.g2 (e1) values ('x')"; //$NON-NLS-1$

        helpValidate(userUpdateStr, new String[]{"UPSERT INTO pm1.g2 (e1) VALUES ('x')"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testUpsertWithNonUpdatableKey() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table g1 (e1 integer not null auto_increment primary key options (updatable false), e2 integer) options (updatable true)", "x", "y");

        String userUpdateStr = "UPSERT into g1 (e1, e2) values (1, 1)"; //$NON-NLS-1$

        helpValidate(userUpdateStr, new String[]{}, metadata); //$NON-NLS-1$

        userUpdateStr = "UPSERT into g1 (e1, e2) values (null, 1)"; //$NON-NLS-1$

        helpValidate(userUpdateStr, new String[]{}, metadata); //$NON-NLS-1$
    }

    @Test public void testDeleteError() {
        String userUpdateStr = "DELETE from vm1.g2 where e1='x'"; //$NON-NLS-1$

        helpValidate(userUpdateStr, new String[]{"vm1.g2"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testJsonArrayBlob() {
        String sql = "select jsonArray(to_bytes('hello', 'us-ascii'))"; //$NON-NLS-1$

        helpValidate(sql, new String[]{"jsonArray(to_bytes('hello', 'us-ascii'))"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testJsonArrayClob() {
        String sql = "select jsonArray(cast('hello' as clob))"; //$NON-NLS-1$

        helpValidate(sql, new String[]{}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testJsonObject() {
        String sql = "select jsonObject(to_bytes('hello', 'us-ascii'))"; //$NON-NLS-1$

        helpValidate(sql, new String[]{"JSONOBJECT(to_bytes('hello', 'us-ascii'))"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testWithValidation() {
        String sql = "with a as (select jsonObject(to_bytes('hello', 'us-ascii')) as x) select a.x from a"; //$NON-NLS-1$

        helpValidate(sql, new String[]{"JSONOBJECT(to_bytes('hello', 'us-ascii'))"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testInsertIntoVirtualWithQueryExpression() {

        QueryMetadataInterface qmi = RealMetadataFactory.example1Cached();

        String sql = "select * from vm1.g1 as x"; //$NON-NLS-1$

        TestProcessor.helpGetPlan(sql, qmi);

        sql = "insert into vm1.g1 (e1, e2, e3, e4) select * from pm1.g1"; //$NON-NLS-1$

        helpValidate(sql, new String[] {}, qmi);

    }

    @Test public void testInsertDuplicateVariable() {
        QueryMetadataInterface qmi = RealMetadataFactory.example1Cached();
        String sql = "insert into pm1.g1 (e1, e1) values ('a', 'b')"; //$NON-NLS-1$
        helpValidate(sql, new String[] {"e1"}, qmi);
    }

    @Test public void testAlterAfterTrigger() {
        String userUpdateStr = "ALTER TRIGGER tr ON pm1.g2 AFTER INSERT DISABLED;"; //$NON-NLS-1$

        helpValidate(userUpdateStr, new String[]{"ALTER TRIGGER tr ON pm1.g2 AFTER INSERT DISABLED"}, RealMetadataFactory.example1Cached()); //$NON-NLS-1$
    }

    @Test public void testInvalidValueFunctions() {
        helpValidate("SELECT FIRST_VALUE(e3) over () FROM test.group", new String[] {"FIRST_VALUE(e3) OVER ()"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
        helpValidate("SELECT FIRST_VALUE(e3 order by e2) over (order by e2) FROM test.group", new String[] {"FIRST_VALUE(e3 ORDER BY e2) OVER (ORDER BY e2)"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
        helpValidate("SELECT LAST_VALUE(e3, e2) over (order by e2) FROM test.group", new String[] {"LAST_VALUE(e3, e2) OVER (ORDER BY e2)"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
        helpValidate("SELECT LAST_VALUE() over (order by e2) FROM test.group", new String[] {"LAST_VALUE() OVER (ORDER BY e2)"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
        helpValidate("SELECT LAG() over (order by e2) FROM test.group", new String[] {"LAG() OVER (ORDER BY e2)"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
        //should succeed
        helpValidate("SELECT LAG(e3) over (order by e2) FROM test.group", new String[] {}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInvalidNtile() {
        helpValidate("SELECT ntile(2) over () FROM test.group", new String[] {"ntile(2) OVER ()"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
        helpValidate("SELECT ntile(2 order by e2) over (order by e2) FROM test.group", new String[] {"ntile(2 ORDER BY e2) OVER (ORDER BY e2)"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
        helpValidate("SELECT ntile(-1) over (order by e2) FROM test.group", new String[] {"ntile(-1) OVER (ORDER BY e2)"}, exampleMetadata()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testEnvAllowed() {
        TransformationMetadata tm = RealMetadataFactory.example1();
        String sql = "select env('abc')"; //$NON-NLS-1$
        helpValidate(sql, new String[] {}, tm);
        sql = "select env_var('abc')"; //$NON-NLS-1$
        helpValidate(sql, new String[] {}, tm);
        sql = "select sys_prop('abc')"; //$NON-NLS-1$
        helpValidate(sql, new String[] {}, tm);

        tm.setAllowENV(false);
        sql = "select env('abc')"; //$NON-NLS-1$
        helpValidate(sql, new String[] {"env('abc')"}, tm);
        sql = "select env_var('abc')"; //$NON-NLS-1$
        helpValidate(sql, new String[] {"env_var('abc')"}, tm);
        sql = "select sys_prop('abc')"; //$NON-NLS-1$
        helpValidate(sql, new String[] {"sys_prop('abc')"}, tm);
    }

}
