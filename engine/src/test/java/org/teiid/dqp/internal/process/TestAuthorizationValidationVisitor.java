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

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.PermissionType;
import org.teiid.adminapi.DataPolicy.ResourceType;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.dqp.internal.process.AuthorizationValidator.CommandType;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.TempTableTestHarness;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;

@SuppressWarnings("nls")
public class TestAuthorizationValidationVisitor {

    public static final String CONN_ID = "connID"; //$NON-NLS-1$
    private CommandContext context;
    private static DataPolicyMetadata exampleAuthSvc1;
    private static DataPolicyMetadata exampleAuthSvc2;

    @Before public void setup() {
        context = new CommandContext();
        context.setDQPWorkContext(new DQPWorkContext());
        context.setSession(context.getDQPWorkContext().getSession());
    }

    @BeforeClass static public void oneTimeSetup() {
        exampleAuthSvc1 = exampleAuthSvc1();
        exampleAuthSvc2 = exampleAuthSvc2();
    }

    static PermissionMetaData createPermission(PermissionType type, boolean flag, String resource, ResourceType resourceType) {
        PermissionMetaData p = new PermissionMetaData();
        p.setResourceName(resource);
        switch(type) {
        case CREATE:
            p.setAllowCreate(flag);
            break;
        case DELETE:
            p.setAllowDelete(flag);
            break;
        case READ:
            p.setAllowRead(flag);
            break;
        case UPDATE:
            p.setAllowUpdate(flag);
            break;
        case ALTER:
            p.setAllowAlter(flag);
            break;
        case EXECUTE:
            p.setAllowExecute(flag);
            break;
        case LANGUAGE:
            p.setAllowLanguage(flag);
        }
        p.setResourceType(resourceType);
        return p;
    }

    static PermissionMetaData createPermission(PermissionType type, String resource) {
        return createPermission(type, true, resource, ResourceType.DATABASE);
    }

    private static DataPolicyMetadata exampleAuthSvc1() {
        DataPolicyMetadata svc = new DataPolicyMetadata();
        svc.setName("test"); //$NON-NLS-1$

        // pm1.g1
        svc.addPermission(createPermission(PermissionType.DELETE, "pm1.g1")); //$NON-NLS-1$

        svc.addPermission(createPermission(DataPolicy.PermissionType.READ, "pm1.g1")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.READ, "pm1.g1.e1")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.READ, false, "pm1.g1.e2", ResourceType.COLUMN)); //$NON-NLS-1$

        svc.addPermission(createPermission(DataPolicy.PermissionType.CREATE, "pm1.g1")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.CREATE, "pm1.g1.e1")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.CREATE, "pm1.g1.e2")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.CREATE, "pm1.g1.e3")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.CREATE, "pm1.g1.e4")); //$NON-NLS-1$

        svc.addPermission(createPermission(DataPolicy.PermissionType.UPDATE, "pm1.g1")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.UPDATE, false, "pm1.g1.e1", ResourceType.COLUMN)); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.UPDATE, "pm1.g1.e2")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.UPDATE, "pm1.g1.e3")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.UPDATE, "pm1.g1.e4")); //$NON-NLS-1$

        svc.addPermission(createPermission(PermissionType.EXECUTE, "pm1.sp1"));

        // pm1.g2
        svc.addPermission(createPermission(DataPolicy.PermissionType.CREATE, "pm1.g2")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.CREATE, false, "pm1.g2.e1", ResourceType.COLUMN)); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.CREATE, "pm1.g2.e2")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.CREATE, "pm1.g2.e3")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.CREATE, "pm1.g2.e4")); //$NON-NLS-1$

        svc.addPermission(createPermission(DataPolicy.PermissionType.UPDATE, "pm1.g2")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.UPDATE, false, "pm1.g2.e1", ResourceType.COLUMN)); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.UPDATE, "pm1.g2.e2")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.UPDATE, "pm1.g2.e3")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.UPDATE, "pm1.g2.e4")); //$NON-NLS-1$

        // pm1.g4
        svc.addPermission(createPermission(DataPolicy.PermissionType.DELETE, "pm1.g4")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.DELETE, "pm1.g4.e1")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.DELETE, "pm1.g4.e2")); //$NON-NLS-1$

        svc.addPermission(createPermission(DataPolicy.PermissionType.READ, "pm1.sq1")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.READ, "pm1.xyz")); //$NON-NLS-1$
        svc.setAllowCreateTemporaryTables(true);
        return svc;
    }

    //allow by default
    private static DataPolicyMetadata exampleAuthSvc2() {
        DataPolicyMetadata svc = new DataPolicyMetadata();
        svc.setName("test"); //$NON-NLS-1$

        svc.addPermission(createPermission(DataPolicy.PermissionType.CREATE, "pm1.g2")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.READ, "pm1.g2")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.READ, "pm1.g1")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.READ, "pm2.g1")); //$NON-NLS-1$

        // pm2.g2
        svc.addPermission(createPermission(DataPolicy.PermissionType.CREATE, "pm2.g2.e1")); //$NON-NLS-1$

        // pm3.g2
        svc.addPermission(createPermission(DataPolicy.PermissionType.CREATE, "pm3.g2.e1")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.CREATE, "pm3.g2.e2")); //$NON-NLS-1$

        svc.addPermission(createPermission(DataPolicy.PermissionType.READ, "xmltest.doc1")); //$NON-NLS-1$

        svc.setAllowCreateTemporaryTables(false);
        return svc;
    }

    private DataPolicyMetadata examplePolicyBQT() {
        DataPolicyMetadata svc = new DataPolicyMetadata();
        svc.setName("test"); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.ALTER, "VQT.SmallA_2589")); //$NON-NLS-1$
        svc.addPermission(createPermission(DataPolicy.PermissionType.CREATE, "bqt1")); //$NON-NLS-1$
        svc.setAllowCreateTemporaryTables(true);
        return svc;
    }

    private void helpTest(String sql, QueryMetadataInterface metadata, String[] expectedInaccesible, VDBMetaData vdb, DataPolicyMetadata... roles) throws QueryParserException, QueryResolverException, TeiidComponentException {
        QueryParser parser = QueryParser.getQueryParser();
        Command command = parser.parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);

        DataRolePolicyDecider dataRolePolicyDecider = createPolicyDecider(metadata, vdb, roles);
        AuthorizationValidationVisitor visitor = new AuthorizationValidationVisitor(dataRolePolicyDecider, context); //$NON-NLS-1$
        ValidatorReport report = Validator.validate(command, metadata, visitor);
        if(report.hasItems()) {
            ValidatorFailure firstFailure = report.getItems().iterator().next();

            // strings
            Set<String> expected = new HashSet<String>(Arrays.asList(expectedInaccesible));
            // elements
            Set<String> actual = new HashSet<String>();
            for (LanguageObject obj : firstFailure.getInvalidObjects()) {
                if (obj instanceof ElementSymbol) {
                    actual.add(((ElementSymbol)obj).getName());
                } else {
                    actual.add(obj.toString());
                }
            }
            assertEquals(expected, actual);
        } else if(expectedInaccesible.length > 0) {
            fail("Expected inaccessible objects, but got none.");                 //$NON-NLS-1$
        }
    }

    private DataRolePolicyDecider createPolicyDecider(
            QueryMetadataInterface metadata, VDBMetaData vdb,
            DataPolicyMetadata... roles) {
        vdb.addAttachment(QueryMetadataInterface.class, metadata);

        HashMap<String, DataPolicy> policies = new HashMap<String, DataPolicy>();
        for (DataPolicyMetadata dataPolicyMetadata : roles) {
            policies.put(dataPolicyMetadata.getName(), dataPolicyMetadata);
        }
        vdb.setDataPolicies(new ArrayList<DataPolicy>(policies.values()));
        this.context.getDQPWorkContext().setPolicies(policies);
        this.context.getSession().setVdb(vdb);
        this.context.setMetadata(metadata);
        DataRolePolicyDecider dataRolePolicyDecider = new DataRolePolicyDecider();
        dataRolePolicyDecider.setAllowFunctionCallsByDefault(false);
        return dataRolePolicyDecider;
    }

    @Test public void testProcRelational() throws Exception {
        helpTest("select * from sp1", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$
        helpTest("select * from pm1.sp1", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$
        helpTest("select * from sp1", RealMetadataFactory.example1Cached(), new String[] {"sp1"}, RealMetadataFactory.example1VDB(), exampleAuthSvc2); //$NON-NLS-1$
    }

    @Test public void testTemp() throws Exception {
        //allowed by default
        helpTest("create local temporary table x (y string)", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$
        //explicitly denied
        helpTest("create local temporary table x (y string)", RealMetadataFactory.example1Cached(), new String[] {"x"}, RealMetadataFactory.example1VDB(), exampleAuthSvc2); //$NON-NLS-1$
    }

    @Test public void testFunction() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        helpTest("SELECT e1 FROM pm1.g1 where xyz() > 0", metadata, new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$
        helpTest("SELECT e1, curdate() FROM pm1.g2 where xyz() > 0", metadata, new String[] {"xyz()"}, RealMetadataFactory.example1VDB(), exampleAuthSvc2); //$NON-NLS-1$
    }

    @Test public void testEverythingAccessible() throws Exception {
        helpTest("SELECT e1 FROM pm1.g1", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$
    }

    @Test public void testAccessibleCombination() throws Exception {
        DataPolicyMetadata svc = new DataPolicyMetadata();
        svc.setName("test"); //$NON-NLS-1$

        svc.addPermission(createPermission(DataPolicy.PermissionType.READ, "pm1")); //$NON-NLS-1$
        PermissionMetaData p = createPermission(DataPolicy.PermissionType.READ, "pm1.g1");
        p.setAllowRead(false);
        svc.addPermission(p); //$NON-NLS-1$

        DataPolicyMetadata svc1 = new DataPolicyMetadata();
        svc1.setName("test1"); //$NON-NLS-1$

        svc1.addPermission(createPermission(DataPolicy.PermissionType.READ, "pm1")); //$NON-NLS-1$

        helpTest("SELECT e1 FROM pm1.g1", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), svc, svc1); //$NON-NLS-1$

        svc1.addPermission(p);

        helpTest("SELECT e1 FROM pm1.g1", RealMetadataFactory.example1Cached(), new String[] {"pm1.g1.e1", "pm1.g1"}, RealMetadataFactory.example1VDB(), svc, svc1); //$NON-NLS-1$
    }

    @Test public void testEverythingAccessible1() throws Exception {
        helpTest("SELECT e1 FROM (select e1 from pm1.g1) x", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$
    }

    @Test public void testEverythingAccessible2() throws Exception {
        helpTest("SELECT lookup('pm1.g1', 'e1', 'e1', '1'), e1 FROM (select e1 from pm1.g1) x", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$
    }

    @Test public void testInaccesibleElement() throws Exception {
        helpTest("SELECT e2 FROM pm1.g1", RealMetadataFactory.example1Cached(), new String[] {"pm1.g1.e2"}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInaccesibleElement2() throws Exception {
        helpTest("SELECT lookup('pm1.g1', 'e1', 'e2', '1')", RealMetadataFactory.example1Cached(), new String[] {"pm1.g1.e2"}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInaccesibleGroup() throws Exception {
        helpTest("SELECT e1 FROM pm1.g2", RealMetadataFactory.example1Cached(), new String[] {"pm1.g2", "pm1.g2.e1"}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInsert() throws Exception {
        helpTest("INSERT INTO pm1.g1 (e1, e2, e3, e4) VALUES ('x', 5, {b'true'}, 1.0)", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$
    }

    @Test public void testInsertInaccessible() throws Exception {
        helpTest("INSERT INTO pm1.g2 (e1, e2, e3, e4) VALUES ('x', 5, {b'true'}, 1.0)", RealMetadataFactory.example1Cached(), new String[] {"pm1.g2.e1"}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testUpdate() throws Exception {
        helpTest("UPDATE pm1.g1 SET e2 = 5", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$
    }

    @Test public void testUpdateCriteriaInaccessibleForRead() throws Exception {
        helpTest("UPDATE pm1.g2 SET e2 = 5 WHERE e1 = 'x'", RealMetadataFactory.example1Cached(), new String[] {"pm1.g2.e1"}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testUpdateCriteriaInaccessibleForRead1() throws Exception {
        helpTest("UPDATE pm1.g2 SET e2 = cast(e1 as integer)", RealMetadataFactory.example1Cached(), new String[] {"pm1.g2.e1"}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testUpdateElementInaccessibleForUpdate() throws Exception {
        helpTest("UPDATE pm1.g1 SET e1 = 5 WHERE e1 = 'x'", RealMetadataFactory.example1Cached(), new String[] {"pm1.g1.e1"}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDelete() throws Exception {
        helpTest("DELETE FROM pm1.g1", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$
    }

    @Test public void testDeleteCriteriaInaccesibleForRead() throws Exception {
        helpTest("DELETE FROM pm1.g2 WHERE e1 = 'x'", RealMetadataFactory.example1Cached(), new String[] {"pm1.g2.e1"}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDeleteInaccesibleGroup() throws Exception {
        helpTest("DELETE FROM pm1.g3", RealMetadataFactory.example1Cached(), new String[] {"pm1.g3"}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testProc() throws Exception {
        helpTest("EXEC pm1.sq1()", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc1);         //$NON-NLS-1$
    }

    @Test public void testProcInaccesible() throws Exception {
        helpTest("EXEC pm1.sq2('xyz')", RealMetadataFactory.example1Cached(), new String[] {"pm1.sq2"}, RealMetadataFactory.example1VDB(), exampleAuthSvc1);         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSelectIntoEverythingAccessible() throws Exception {
        helpTest("SELECT e1, e2, e3, e4 INTO pm1.g2 FROM pm2.g1", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc2); //$NON-NLS-1$
    }

    @Test public void testSelectIntoTarget_e1_NotAccessible() throws Exception {
        helpTest("SELECT e1, e2, e3, e4 INTO pm2.g2 FROM pm2.g1", RealMetadataFactory.example1Cached(), new String[] {"pm2.g2", "pm2.g2.e2","pm2.g2.e4","pm2.g2.e3"}, RealMetadataFactory.example1VDB(), exampleAuthSvc2); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testSelectIntoTarget_e1e2_NotAccessible() throws Exception {
        helpTest("SELECT e1, e2, e3, e4 INTO pm3.g2 FROM pm2.g1", RealMetadataFactory.example1Cached(), new String[] {"pm3.g2", "pm3.g2.e4", "pm3.g2.e3"}, RealMetadataFactory.example1VDB(),exampleAuthSvc2); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testTempTableSelectInto() throws Exception {
        helpTest("SELECT e1 INTO #temp FROM pm1.g1", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$
        helpTest("SELECT e1 INTO #temp FROM pm1.g1", RealMetadataFactory.example1Cached(), new String[] {"#temp"}, RealMetadataFactory.example1VDB(), exampleAuthSvc2); //$NON-NLS-1$
        helpTest("SELECT e1 INTO #temp FROM pm1.g1", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc2, exampleAuthSvc1); //$NON-NLS-1$
    }

    @Test public void testCommonTable() throws Exception {
        helpTest("WITH X AS (SELECT e1 from pm1.g2) SELECT e1 from x", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc2); //$NON-NLS-1$
    }

    @Test public void testTempTableSelectInto1() throws Exception {
        helpTest("SELECT e1, e2 INTO #temp FROM pm1.g1", RealMetadataFactory.example1Cached(), new String[] {"pm1.g1.e2"}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testTempTableInsert() throws Exception {
        helpTest("insert into #temp (e1, e2, e3, e4) values ('1', '2', '3', '4')", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), exampleAuthSvc1); //$NON-NLS-1$
        helpTest("insert into #temp (e1, e2, e3, e4) values ('1', '2', '3', '4')", RealMetadataFactory.example1Cached(), new String[] {"#temp"}, RealMetadataFactory.example1VDB(), exampleAuthSvc2); //$NON-NLS-1$
    }

    @Test public void testAlter() throws Exception {
        helpTest("alter view SmallA_2589 as select * from bqt1.smalla", RealMetadataFactory.exampleBQTCached(), new String[] {"SmallA_2589"}, RealMetadataFactory.exampleBQTVDB(), exampleAuthSvc1); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest("alter view SmallA_2589 as select * from bqt1.smalla", RealMetadataFactory.exampleBQTCached(), new String[] {}, RealMetadataFactory.exampleBQTVDB(), examplePolicyBQT()); //$NON-NLS-1$ //$NON-NLS-2$

        helpTest("alter trigger on SmallA_2589 INSTEAD OF UPDATE enabled", RealMetadataFactory.exampleBQTCached(), new String[] {"SmallA_2589"}, RealMetadataFactory.exampleBQTVDB(), exampleAuthSvc1); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest("alter trigger on SmallA_2589 INSTEAD OF UPDATE enabled", RealMetadataFactory.exampleBQTCached(), new String[] {}, RealMetadataFactory.exampleBQTVDB(), examplePolicyBQT()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testObjectTable() throws Exception {
        helpTest("select * from objecttable(language 'javascript' 'teiid_context' columns x string 'teiid_row.userName') as x", RealMetadataFactory.exampleBQTCached(), new String[] {"OBJECTTABLE(LANGUAGE 'javascript' 'teiid_context' COLUMNS x string 'teiid_row.userName') AS x"}, RealMetadataFactory.exampleBQTVDB(), exampleAuthSvc1); //$NON-NLS-1$ //$NON-NLS-2$
        DataPolicyMetadata policy = exampleAuthSvc1();
        policy.addPermission(createPermission(PermissionType.LANGUAGE, "javascript"));
        helpTest("select * from objecttable(language 'javascript' 'teiid_context' columns x string 'teiid_row.userName') as x", RealMetadataFactory.exampleBQTCached(), new String[] {}, RealMetadataFactory.exampleBQTVDB(), policy); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testCreateForeignTemp() throws Exception {
        DataPolicyMetadata dpm = exampleAuthSvc1();
        dpm.setAllowCreateTemporaryTables(false);
        helpTest("create foreign temporary table x (id string) on bqt1", RealMetadataFactory.exampleBQTCached(), new String[] {"x"}, RealMetadataFactory.exampleBQTVDB(), dpm); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest("create foreign temporary table x (id string) on bqt1", RealMetadataFactory.exampleBQTCached(), new String[] {}, RealMetadataFactory.exampleBQTVDB(), examplePolicyBQT()); //$NON-NLS-1$ //$NON-NLS-2$

        TempTableTestHarness harness = new TempTableTestHarness();
        harness.setUp(RealMetadataFactory.exampleBQTCached(), new HardcodedDataManager());
        harness.execute("create foreign temporary table x (id string) on bqt1", new List[] {Arrays.asList(0)});
        helpTest("insert into x (id) values ('a')", harness.getMetadata(), new String[]{"x.id"}, RealMetadataFactory.exampleBQTVDB(), dpm);
        //we have create on bqt1
        helpTest("insert into x (id) values ('a')", harness.getMetadata(), new String[]{}, RealMetadataFactory.exampleBQTVDB(), examplePolicyBQT());
        //we don't have read on bqt1
        helpTest("select * from x", harness.getMetadata(), new String[]{"x.id"}, RealMetadataFactory.exampleBQTVDB(), examplePolicyBQT());
    }

    @Test public void testGrantAll() throws Exception {
        DataPolicyMetadata svc = new DataPolicyMetadata();
        svc.setGrantAll(true);
        helpTest("create foreign temporary table x (id string) on bqt1", RealMetadataFactory.exampleBQTCached(), new String[] {}, RealMetadataFactory.exampleBQTVDB(), svc); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testPruneSelectAll() throws Exception {
        String sql = "select * from pm1.g1";
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

        DataPolicyMetadata svc = new DataPolicyMetadata();
        svc.setName("test"); //$NON-NLS-1$

        svc.addPermission(createPermission(DataPolicy.PermissionType.READ, "pm1")); //$NON-NLS-1$
        PermissionMetaData p = createPermission(DataPolicy.PermissionType.READ, "pm1.g1.e1");
        p.setAllowRead(false);
        svc.addPermission(p); //$NON-NLS-1$

        DataRolePolicyDecider dataRolePolicyDecider = createPolicyDecider(metadata, RealMetadataFactory.example1VDB(), svc);

        DefaultAuthorizationValidator dav = new DefaultAuthorizationValidator();
        dav.setPolicyDecider(dataRolePolicyDecider);
        this.context.setSessionVariable(DefaultAuthorizationValidator.IGNORE_UNAUTHORIZED_ASTERISK, "true");

        QueryParser parser = QueryParser.getQueryParser();
        Command command = parser.parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);

        assertEquals(4, command.getProjectedSymbols().size());

        boolean modified = dav.validate(new String[] {}, command, metadata, this.context, CommandType.USER);
        assertTrue(modified);

        assertEquals(3, command.getProjectedSymbols().size());

        p = createPermission(DataPolicy.PermissionType.READ, "pm1.g1");
        p.setAllowRead(false);
        svc.addPermission(p); //$NON-NLS-1$

        command = parser.parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);

        assertEquals(4, command.getProjectedSymbols().size());

        try {
            dav.validate(new String[] {}, command, metadata, this.context, CommandType.USER);
            fail();
        } catch (QueryValidatorException e) {

        }
    }

    @Test public void testInheritedGrantAll() throws Exception {
        String sql = "select * from pm1.g1";

        DataPolicyMetadata svc = new DataPolicyMetadata();
        svc.setName("test"); //$NON-NLS-1$

        svc.setGrantAll(true);

        svc.setSchemas(Collections.singleton("pm1"));

        helpTest(sql, RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB(), svc); //$NON-NLS-1$ //$NON-NLS-2$

        sql = "select e1 from pm2.g1";

        helpTest(sql, RealMetadataFactory.example1Cached(), new String[] {"pm2.g1.e1", "pm2.g1"}, RealMetadataFactory.example1VDB(), svc); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testNameConflict() throws Exception {
        helpTest("SELECT * FROM pm1.\"g1.e1\"", RealMetadataFactory.fromDDL("create foreign table \"g1.e1\" (col string)", "x", "pm1"), new String[] {"pm1.g1.e1", "pm1.g1.e1.col"}, RealMetadataFactory.example1VDB(), exampleAuthSvc2); //$NON-NLS-1$
    }
}
