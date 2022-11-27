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

import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.KeyRecord.Type;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.UpdateValidator.UpdateInfo;
import org.teiid.query.validator.UpdateValidator.UpdateType;

@SuppressWarnings("nls")
public class TestUpdateValidator {

    private UpdateValidator helpTest(String sql, TransformationMetadata md, boolean shouldFail) {
        return helpTest(sql, md, shouldFail, shouldFail, shouldFail);
    }

    private UpdateValidator helpTest(String sql, TransformationMetadata md, boolean failInsert, boolean failUpdate, boolean failDelete) {
        try {
            String vGroup = "gx";
            Command command = createView(sql, md, vGroup);

            UpdateValidator uv = new UpdateValidator(md, UpdateType.INHERENT, UpdateType.INHERENT, UpdateType.INHERENT);
            GroupSymbol gs = new GroupSymbol(vGroup);
            ResolverUtil.resolveGroup(gs, md);
            uv.validate(command, ResolverUtil.resolveElementsInGroup(gs, md));
            UpdateInfo info = uv.getUpdateInfo();
            assertEquals(uv.getReport().getFailureMessage(), failInsert, info.getInsertValidationError() != null);
            assertEquals(uv.getReport().getFailureMessage(), failUpdate, info.getUpdateValidationError() != null);
            assertEquals(uv.getReport().getFailureMessage(), failDelete, info.getDeleteValidationError() != null);
            return uv;
        } catch (TeiidException e) {
            throw new RuntimeException(e);
        }
    }

    public static Command createView(String sql, TransformationMetadata md, String vGroup)
            throws QueryParserException, QueryResolverException,
            TeiidComponentException {
        QueryNode vm1g1n1 = new QueryNode(sql);
        Table vm1g1 = RealMetadataFactory.createUpdatableVirtualGroup(vGroup, md.getMetadataStore().getSchema("VM1"), vm1g1n1);

        Command command = QueryParser.getQueryParser().parseCommand(sql);
        QueryResolver.resolveCommand(command, md);

        List<Expression> symbols = command.getProjectedSymbols();
        String[] names = new String[symbols.size()];
        String[] types = new String[symbols.size()];
        int i = 0;
        for (Expression singleElementSymbol : symbols) {
            names[i] = Symbol.getShortName(singleElementSymbol);
            types[i++] = DataTypeManager.getDataTypeName(singleElementSymbol.getType());
        }

        RealMetadataFactory.createElements(vm1g1, names, types);
        return command;
    }

     public static TransformationMetadata example1() {
         return example1(true);
     }

     public static TransformationMetadata example1(boolean allUpdatable) {
         MetadataStore metadataStore = new MetadataStore();

         // Create models
        Schema pm1 = RealMetadataFactory.createPhysicalModel("pm1", metadataStore); //$NON-NLS-1$
        Schema vm1 = RealMetadataFactory.createVirtualModel("vm1", metadataStore);     //$NON-NLS-1$

        // Create physical groups
        Table pm1g1 = RealMetadataFactory.createPhysicalGroup("g1", pm1); //$NON-NLS-1$
        Table pm1g2 = RealMetadataFactory.createPhysicalGroup("g2", pm1); //$NON-NLS-1$
        Table pm1g3 = RealMetadataFactory.createPhysicalGroup("g3", pm1); //$NON-NLS-1$

        // Create physical elements
        List<Column> pm1g1e = RealMetadataFactory.createElements(pm1g1,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        if (!allUpdatable) {
            pm1g1e.get(0).setUpdatable(false);
        }

        KeyRecord pk = RealMetadataFactory.createKey(Type.Primary, "pk", pm1g1, pm1g1e.subList(0, 1));

        List<Column> pm1g2e = RealMetadataFactory.createElements(pm1g2,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });

        RealMetadataFactory.createKey(Type.Primary, "pk", pm1g2, pm1g1e.subList(1, 2));
        RealMetadataFactory.createForeignKey("fk", pm1g2, pm1g2e.subList(0, 1), pk);

        List<Column> pm1g3e = RealMetadataFactory.createElements(pm1g3,
            new String[] { "e1", "e2", "e3", "e4" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        pm1g3e.get(0).setNullType(NullType.No_Nulls);
        pm1g3e.get(0).setDefaultValue(null);

        pm1g3e.get(1).setNullType(NullType.No_Nulls);
        pm1g3e.get(1).setAutoIncremented(true);
        pm1g3e.get(1).setDefaultValue(null);

        pm1g3e.get(2).setNullType(NullType.No_Nulls);
        pm1g3e.get(2).setDefaultValue("xyz"); //$NON-NLS-1$

        RealMetadataFactory.createKey(Type.Primary, "pk", pm1g3, pm1g3e.subList(0, 1));

        // Create virtual groups
        QueryNode vm1g1n1 = new QueryNode("SELECT e1 as a, e2 FROM pm1.g1 WHERE e3 > 5"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g1 = RealMetadataFactory.createUpdatableVirtualGroup("g1", vm1, vm1g1n1); //$NON-NLS-1$
        QueryNode vm1g2n1 = new QueryNode("SELECT e1, e2, e3, e4 FROM pm1.g2 WHERE e3 > 5"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g2 = RealMetadataFactory.createUpdatableVirtualGroup("g2", vm1, vm1g2n1); //$NON-NLS-1$
        QueryNode vm1g3n1 = new QueryNode("SELECT e1, e3 FROM pm1.g3"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g3 = RealMetadataFactory.createUpdatableVirtualGroup("g3", vm1, vm1g3n1); //$NON-NLS-1$
        QueryNode vm1g4n1 = new QueryNode("SELECT e1, e2 FROM pm1.g3"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g4 = RealMetadataFactory.createUpdatableVirtualGroup("g4", vm1, vm1g4n1); //$NON-NLS-1$
        QueryNode vm1g5n1 = new QueryNode("SELECT e2, e3 FROM pm1.g3"); //$NON-NLS-1$ //$NON-NLS-2$
        Table vm1g5 = RealMetadataFactory.createVirtualGroup("g5", vm1, vm1g5n1); //$NON-NLS-1$

        // Create virtual elements
        RealMetadataFactory.createElements(vm1g1,
            new String[] { "a", "e2"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER});
        RealMetadataFactory.createElements(vm1g2,
            new String[] { "e1", "e2","e3", "e4"}, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN, DataTypeManager.DefaultDataTypes.DOUBLE });
        RealMetadataFactory.createElements(vm1g3,
            new String[] { "e1", "e2"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER,  });
        RealMetadataFactory.createElements(vm1g4,
            new String[] { "e1", "e3"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.BOOLEAN });
        RealMetadataFactory.createElements(vm1g5,
            new String[] { "e2","e3"}, //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BOOLEAN });

        // Stored queries
        ColumnSet<Procedure> rs1 = RealMetadataFactory.createResultSet("rs1", new String[] { "e1", "e2" }, new String[] { DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        QueryNode sq1n1 = new QueryNode("CREATE VIRTUAL PROCEDURE BEGIN SELECT e1, e2 FROM pm1.g1; END"); //$NON-NLS-1$ //$NON-NLS-2$
        Procedure sq1 = RealMetadataFactory.createVirtualProcedure("sq1", pm1, Collections.EMPTY_LIST, sq1n1);  //$NON-NLS-1$
        sq1.setResultSet(rs1);
        // Create the facade from the store
        return RealMetadataFactory.createTransformationMetadata(metadataStore, "example");
    }

    //actual tests
    @Test public void testCreateInsertCommand(){
        helpTest("select e1 as a, e2 from pm1.g1 where e4 > 5",
            example1(), false); //$NON-NLS-1$
    }

    @Test public void testCreateInsertCommand2(){ //put a constant in select statement
        helpTest("select e1 as a, 5 from pm1.g1 where e4 > 5",
            example1(), false); //$NON-NLS-1$
    }

    @Test public void testCreateInsertCommand3(){
        helpTest("select * from pm1.g2 where e4 > 5",
            example1(), false); //$NON-NLS-1$
    }

    @Test public void testCreateInsertCommand4(){ //test group alias
        helpTest("select * from pm1.g2 as g_alias",
            example1(), false); //$NON-NLS-1$
    }

    @Test public void testCreateInsertCommand5(){
        helpTest("select e1 as a, e2 from pm1.g1 as g_alias where e4 > 5",
            example1(), false); //$NON-NLS-1$
    }

    @Test public void testCreateUpdateCommand(){
        helpTest("select e1 as a, e2 from pm1.g1 where e4 > 5",
            example1(), false); //$NON-NLS-1$
    }

    @Test public void testCreateDeleteCommand(){
        helpTest("select e1 as a, e2 from pm1.g1 where e4 > 5",
            example1(), false); //$NON-NLS-1$
    }

    @Test public void testCreateInsertCommand1(){
        helpTest("SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2",
            example1(), true);
    }

    @Test public void testCreateInsertCommand14(){
        helpTest("SELECT pm1.g2.e1 FROM pm1.g1, pm1.g2 where g1.e1 = g2.e1",
            example1(), false);
    }

    @Test public void testCreateInsertCommand2_fail(){
        helpTest("SELECT CONCAT(pm1.g1.e1, convert(pm1.g2.e1, string)) as x FROM pm1.g1, pm1.g2",
            example1(), true);
    }

    @Test public void testCreateInsertCommand3_fail(){
        helpTest("SELECT e1 FROM pm1.g1 UNION SELECT e1 FROM pm1.g2",
            example1(), true);
    }

    @Test public void testCreateInsertCommand4_fail(){
        helpTest("SELECT COUNT(*) FROM pm1.g1",
            example1(), true);
    }

    @Test public void testCreateInsertCommand5_fail(){
        helpTest("SELECT * FROM pm1.g1 GROUP BY e1",
            example1(), true);
    }

    @Test public void testCreateInsertCommand6_fail(){
        helpTest("EXEC pm1.sq1()",
            example1(), true);
    }

    @Test public void testCreateInsertCommand7_fail(){
        helpTest("INSERT INTO pm1.g1 (e1) VALUES ('x')",
            example1(), true);
    }

    @Test public void testCreateInsertCommand8_fail(){
        helpTest("UPDATE pm1.g1 SET e1='x'",
            example1(), true);
    }

    @Test public void testCreateInsertCommand9_fail(){
        helpTest("DELETE FROM pm1.g1",
            example1(), true);
    }

    @Test public void testCreateInsertCommand10_fail(){
        helpTest("SELECT COUNT(*) FROM pm1.g1",
            example1(), true);
    }

    @Test public void testCreateInsertCommand11_fail(){
        helpTest("SELECT COUNT(e1) as x FROM pm1.g1",
            example1(), true);
    }

    @Test public void testCreateInsertCommand12_fail(){
        helpTest("SELECT * FROM (EXEC pm1.sq1()) AS a",
            example1(), true);
    }

    @Test public void testCreateInsertCommand13_fail(){
        helpTest("SELECT 1",
            example1(), true);
    }

    @Test public void testRequiredElements1() {
        helpTest("SELECT e1, e2 FROM pm1.g3",
            example1(), false); //$NON-NLS-1$
    }

    @Test public void testRequiredElements2() {
        helpTest("SELECT e1, e3 FROM pm1.g3",
            example1(), false); //$NON-NLS-1$
    }

    @Test public void testRequiredElements3() {
        helpTest("SELECT e2, e3 FROM pm1.g3",
            example1(), true, false, false);
    }

    @Test public void testNonUpdateableElements() {
        helpTest("select e1 as a, e2 from pm1.g1 where e4 > 5",
                    example1(false), false); //$NON-NLS-1$
    }

    @Test public void testNonUpdateableElements2() {
        helpTest("SELECT e1, e2 FROM pm1.g1",
            example1(false), false); //$NON-NLS-1$
    }

    @Test public void testSelectDistinct() {
        helpTest("SELECT distinct e1, e2 FROM pm1.g1",
            example1(), true); //$NON-NLS-1$
    }

    @Test public void testNonUpdatable() {
        helpTest("SELECT e2 FROM vm1.g5",
            example1(), true); //$NON-NLS-1$
    }

    @Test public void testAnsiJoin() {
        helpTest("SELECT g1.e1, x.e2 FROM pm1.g2 x inner join pm1.g1 on (x.e1 = g1.e1)",
            example1(), false); //$NON-NLS-1$
    }

    @Test public void testUnionAll() {
        helpTest("SELECT g1.e1, x.e2 FROM pm1.g2 x inner join pm1.g1 on (x.e1 = g1.e1) union all select pm1.g2.e1, pm1.g2.e2 from pm1.g2",
            example1(), true, false, false); //$NON-NLS-1$
    }

    @Test public void testParitionedUnionAll() {
        helpTest("SELECT g1.e1, x.e2 FROM pm1.g2 x inner join pm1.g1 on (x.e1 = g1.e1) where x.e2 in (1, 2) union all select pm1.g2.e1, pm1.g2.e2 from pm1.g2 where pm1.g2.e2 in (3, 4)",
            example1(), false, false, false); //$NON-NLS-1$
    }

}

