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

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.PermissionType;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.QueryParser;
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
    private static CommandContext context = new CommandContext();
    
    @BeforeClass public static void oneTimeSetup() {
    	context.setSession(new SessionMetadata());
    }

    PermissionMetaData addResource(PermissionType type, boolean flag, String resource) {
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
    	}
    	return p;    	
    }
    PermissionMetaData addResource(PermissionType type, String resource) {
    	return addResource(type, true, resource);
    }

    private DataPolicyMetadata exampleAuthSvc1() {
    	DataPolicyMetadata svc = new DataPolicyMetadata();
    	svc.setName("test"); //$NON-NLS-1$
        
        // pm1.g1
        svc.addPermission(addResource(PermissionType.DELETE, "pm1.g1")); //$NON-NLS-1$
        
        svc.addPermission(addResource(DataPolicy.PermissionType.READ, "pm1.g1")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.READ, "pm1.g1.e1")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.READ, false, "pm1.g1.e2")); //$NON-NLS-1$

        svc.addPermission(addResource(DataPolicy.PermissionType.CREATE, "pm1.g1")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.CREATE, "pm1.g1.e1")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.CREATE, "pm1.g1.e2")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.CREATE, "pm1.g1.e3")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.CREATE, "pm1.g1.e4")); //$NON-NLS-1$

        svc.addPermission(addResource(DataPolicy.PermissionType.UPDATE, "pm1.g1")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.UPDATE, false, "pm1.g1.e1")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.UPDATE, "pm1.g1.e2")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.UPDATE, "pm1.g1.e3")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.UPDATE, "pm1.g1.e4")); //$NON-NLS-1$
        

        // pm1.g2
        svc.addPermission(addResource(DataPolicy.PermissionType.CREATE, "pm1.g2")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.CREATE, false, "pm1.g2.e1")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.CREATE, "pm1.g2.e2")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.CREATE, "pm1.g2.e3")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.CREATE, "pm1.g2.e4")); //$NON-NLS-1$

        svc.addPermission(addResource(DataPolicy.PermissionType.UPDATE, "pm1.g2")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.UPDATE, false, "pm1.g2.e1")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.UPDATE, "pm1.g2.e2")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.UPDATE, "pm1.g2.e3")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.UPDATE, "pm1.g2.e4")); //$NON-NLS-1$

        // pm1.g4
        svc.addPermission(addResource(DataPolicy.PermissionType.DELETE, "pm1.g4")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.DELETE, "pm1.g4.e1")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.DELETE, "pm1.g4.e2")); //$NON-NLS-1$

        svc.addPermission(addResource(DataPolicy.PermissionType.READ, "pm1.sq1")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.READ, "pm1.xyz")); //$NON-NLS-1$
        
        return svc;
    }
    
    //allow by default
    private DataPolicyMetadata exampleAuthSvc2() {
    	DataPolicyMetadata svc = new DataPolicyMetadata();
    	svc.setName("test"); //$NON-NLS-1$
    	
    	svc.addPermission(addResource(DataPolicy.PermissionType.CREATE, "pm1.g2")); //$NON-NLS-1$
    	svc.addPermission(addResource(DataPolicy.PermissionType.READ, "pm1.g2")); //$NON-NLS-1$
    	svc.addPermission(addResource(DataPolicy.PermissionType.READ, "pm2.g1")); //$NON-NLS-1$
        
    	// pm2.g2
        svc.addPermission(addResource(DataPolicy.PermissionType.CREATE, "pm2.g2.e1")); //$NON-NLS-1$
        
        // pm3.g2
        svc.addPermission(addResource(DataPolicy.PermissionType.CREATE, "pm3.g2.e1")); //$NON-NLS-1$
        svc.addPermission(addResource(DataPolicy.PermissionType.CREATE, "pm3.g2.e2")); //$NON-NLS-1$
        
        svc.addPermission(addResource(DataPolicy.PermissionType.READ, "xmltest.doc1")); //$NON-NLS-1$
        
        svc.setAllowCreateTemporaryTables(false);
        return svc;
    }
    
    private DataPolicyMetadata examplePolicyBQT() {
    	DataPolicyMetadata svc = new DataPolicyMetadata();
    	svc.setName("test"); //$NON-NLS-1$
    	svc.addPermission(addResource(DataPolicy.PermissionType.ALTER, "VQT.SmallA_2589")); //$NON-NLS-1$
        return svc;
    }

    private void helpTest(DataPolicyMetadata policy, String sql, QueryMetadataInterface metadata, String[] expectedInaccesible, VDBMetaData vdb) throws QueryParserException, QueryResolverException, TeiidComponentException {
        QueryParser parser = QueryParser.getQueryParser();
        Command command = parser.parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);
        
        vdb.addAttchment(QueryMetadataInterface.class, metadata);
        
        HashMap<String, DataPolicy> policies = new HashMap<String, DataPolicy>();
        policies.put(policy.getName(), policy);
        
        AuthorizationValidationVisitor visitor = new AuthorizationValidationVisitor(policies, context); //$NON-NLS-1$
        visitor.setAllowFunctionCallsByDefault(false);
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
    
    @Test public void testTemp() throws Exception {
    	//allowed by default
    	helpTest(exampleAuthSvc1(), "create local temporary table x (y string)", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$
    	//explicitly denied
        helpTest(exampleAuthSvc2(), "create local temporary table x (y string)", RealMetadataFactory.example1Cached(), new String[] {"x"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$
    }
    
    @Test public void testFunction() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
    	helpTest(exampleAuthSvc1(), "SELECT e1 FROM pm1.g1 where xyz() > 0", metadata, new String[] {}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$
        helpTest(exampleAuthSvc2(), "SELECT e1, curdate() FROM pm1.g2 where xyz() > 0", metadata, new String[] {"xyz()"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$ 
    }
    
    @Test public void testEverythingAccessible() throws Exception {
        helpTest(exampleAuthSvc1(), "SELECT e1 FROM pm1.g1", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$
    }
    
    @Test public void testEverythingAccessible1() throws Exception {
        helpTest(exampleAuthSvc1(), "SELECT e1 FROM (select e1 from pm1.g1) x", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$
    }
    
    @Test public void testEverythingAccessible2() throws Exception {
        helpTest(exampleAuthSvc1(), "SELECT lookup('pm1.g1', 'e1', 'e1', '1'), e1 FROM (select e1 from pm1.g1) x", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$
    }

    @Test public void testInaccesibleElement() throws Exception {        
        helpTest(exampleAuthSvc1(), "SELECT e2 FROM pm1.g1", RealMetadataFactory.example1Cached(), new String[] {"pm1.g1.e2"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testInaccesibleElement2() throws Exception {        
        helpTest(exampleAuthSvc1(), "SELECT lookup('pm1.g1', 'e1', 'e2', '1')", RealMetadataFactory.example1Cached(), new String[] {"pm1.g1.e2"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testInaccesibleGroup() throws Exception {        
        helpTest(exampleAuthSvc1(), "SELECT e1 FROM pm1.g2", RealMetadataFactory.example1Cached(), new String[] {"pm1.g2", "pm1.g2.e1"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testInsert() throws Exception {        
        helpTest(exampleAuthSvc1(), "INSERT INTO pm1.g1 (e1, e2, e3, e4) VALUES ('x', 5, {b'true'}, 1.0)", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$
    }

    @Test public void testInsertInaccessible() throws Exception {        
        helpTest(exampleAuthSvc1(), "INSERT INTO pm1.g2 (e1, e2, e3, e4) VALUES ('x', 5, {b'true'}, 1.0)", RealMetadataFactory.example1Cached(), new String[] {"pm1.g2.e1"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testUpdate() throws Exception {        
        helpTest(exampleAuthSvc1(), "UPDATE pm1.g1 SET e2 = 5", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$
    }

    @Test public void testUpdateCriteriaInaccessibleForRead() throws Exception {        
        helpTest(exampleAuthSvc1(), "UPDATE pm1.g2 SET e2 = 5 WHERE e1 = 'x'", RealMetadataFactory.example1Cached(), new String[] {"pm1.g2.e1"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testUpdateCriteriaInaccessibleForRead1() throws Exception {        
        helpTest(exampleAuthSvc1(), "UPDATE pm1.g2 SET e2 = cast(e1 as integer)", RealMetadataFactory.example1Cached(), new String[] {"pm1.g2.e1"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testUpdateElementInaccessibleForUpdate() throws Exception {        
        helpTest(exampleAuthSvc1(), "UPDATE pm1.g1 SET e1 = 5 WHERE e1 = 'x'", RealMetadataFactory.example1Cached(), new String[] {"pm1.g1.e1"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDelete() throws Exception {        
        helpTest(exampleAuthSvc1(), "DELETE FROM pm1.g1", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$
    }

    @Test public void testDeleteCriteriaInaccesibleForRead() throws Exception {        
        helpTest(exampleAuthSvc1(), "DELETE FROM pm1.g2 WHERE e1 = 'x'", RealMetadataFactory.example1Cached(), new String[] {"pm1.g2.e1"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testDeleteInaccesibleGroup() throws Exception {        
        helpTest(exampleAuthSvc1(), "DELETE FROM pm1.g3", RealMetadataFactory.example1Cached(), new String[] {"pm1.g3"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testProc() throws Exception {
        helpTest(exampleAuthSvc1(), "EXEC pm1.sq1()", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB());         //$NON-NLS-1$
    }

    @Test public void testProcInaccesible() throws Exception {
        helpTest(exampleAuthSvc1(), "EXEC pm1.sq2('xyz')", RealMetadataFactory.example1Cached(), new String[] {"pm1.sq2"}, RealMetadataFactory.example1VDB());         //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSelectIntoEverythingAccessible() throws Exception {
        helpTest(exampleAuthSvc2(), "SELECT e1, e2, e3, e4 INTO pm1.g2 FROM pm2.g1", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$
    }

    @Test public void testSelectIntoTarget_e1_NotAccessible() throws Exception {
        helpTest(exampleAuthSvc2(), "SELECT e1, e2, e3, e4 INTO pm2.g2 FROM pm2.g1", RealMetadataFactory.example1Cached(), new String[] {"pm2.g2.e2","pm2.g2.e4","pm2.g2.e3"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    @Test public void testSelectIntoTarget_e1e2_NotAccessible() throws Exception {
        helpTest(exampleAuthSvc2(), "SELECT e1, e2, e3, e4 INTO pm3.g2 FROM pm2.g1", RealMetadataFactory.example1Cached(), new String[] {"pm3.g2.e4", "pm3.g2.e3"},RealMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    @Test public void testTempTableSelectInto() throws Exception {
        helpTest(exampleAuthSvc1(), "SELECT e1 INTO #temp FROM pm1.g1", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$
        helpTest(exampleAuthSvc2(), "SELECT e1 INTO #temp FROM pm1.g1", RealMetadataFactory.example1Cached(), new String[] {"#temp"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$
    }
    
    @Test public void testTempTableSelectInto1() throws Exception {
        helpTest(exampleAuthSvc1(), "SELECT e1, e2 INTO #temp FROM pm1.g1", RealMetadataFactory.example1Cached(), new String[] {"pm1.g1.e2"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testTempTableInsert() throws Exception {
        helpTest(exampleAuthSvc1(), "insert into #temp (e1, e2, e3, e4) values ('1', '2', '3', '4')", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$
        helpTest(exampleAuthSvc2(), "insert into #temp (e1, e2, e3, e4) values ('1', '2', '3', '4')", RealMetadataFactory.example1Cached(), new String[] {"#temp"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$
    }
    
    @Test public void testXMLAccessible() throws Exception {
        helpTest(exampleAuthSvc2(), "select * from xmltest.doc1", RealMetadataFactory.example1Cached(), new String[] {}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testXMLInAccessible() throws Exception {
        helpTest(exampleAuthSvc1(), "select * from xmltest.doc1", RealMetadataFactory.example1Cached(), new String[] {"xmltest.doc1"}, RealMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testAlter() throws Exception {
        helpTest(exampleAuthSvc1(), "alter view SmallA_2589 as select * from bqt1.smalla", RealMetadataFactory.exampleBQTCached(), new String[] {"SmallA_2589"}, RealMetadataFactory.exampleBQTVDB()); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(examplePolicyBQT(), "alter view SmallA_2589 as select * from bqt1.smalla", RealMetadataFactory.exampleBQTCached(), new String[] {}, RealMetadataFactory.exampleBQTVDB()); //$NON-NLS-1$ //$NON-NLS-2$
        
        helpTest(exampleAuthSvc1(), "alter trigger on SmallA_2589 INSTEAD OF UPDATE enabled", RealMetadataFactory.exampleBQTCached(), new String[] {"SmallA_2589"}, RealMetadataFactory.exampleBQTVDB()); //$NON-NLS-1$ //$NON-NLS-2$
        helpTest(examplePolicyBQT(), "alter trigger on SmallA_2589 INSTEAD OF UPDATE enabled", RealMetadataFactory.exampleBQTCached(), new String[] {}, RealMetadataFactory.exampleBQTVDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
	private void helpTestLookupVisibility(boolean visible) throws QueryParserException, QueryValidatorException, TeiidComponentException {
		VDBMetaData vdb = RealMetadataFactory.example1VDB();
		vdb.getModel("pm1").setVisible(visible); //$NON-NLS-1$
		AuthorizationValidationVisitor mvvv = new AuthorizationValidationVisitor(new HashMap<String, DataPolicy>(), context); //$NON-NLS-1$
		String sql = "select lookup('pm1.g1', 'e1', 'e2', 1)"; //$NON-NLS-1$
		Command command = QueryParser.getQueryParser().parseCommand(sql);
		Request.validateWithVisitor(mvvv, RealMetadataFactory.example1Cached(), command);
	}
	
	@Ignore("visibility no longer ristricts access")
	@Test public void testLookupVisibility() throws Exception {
		helpTestLookupVisibility(true);
	}
	
	@Ignore("visibility no longer ristricts access")
	@Test public void testLookupVisibilityFails() throws Exception {
		try {
			helpTestLookupVisibility(false);
			fail("expected exception"); //$NON-NLS-1$
		} catch (QueryValidatorException e) {
			assertEquals("Group does not exist: pm1.g1", e.getMessage()); //$NON-NLS-1$
		}
	}

}
