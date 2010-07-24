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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import junit.framework.TestCase;

import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.PermissionType;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.dqp.internal.process.AuthorizationValidationVisitor;
import org.teiid.dqp.internal.process.Request;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.Symbol;
import org.teiid.query.unittest.FakeMetadataFactory;
import org.teiid.query.validator.Validator;
import org.teiid.query.validator.ValidatorFailure;
import org.teiid.query.validator.ValidatorReport;


public class TestAuthorizationValidationVisitor extends TestCase {

    public static final String CONN_ID = "connID"; //$NON-NLS-1$

    /**
     * Constructor for TestAuthorizationValidationVisitor.
     * @param name
     */
    public TestAuthorizationValidationVisitor(String name) {
        super(name);
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

        // pm1.sq2
        svc.addPermission(addResource(DataPolicy.PermissionType.READ, "pm1.sq1")); //$NON-NLS-1$
        
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
        
        return svc;
    }

    private void helpTest(DataPolicyMetadata policy, String sql, QueryMetadataInterface metadata, String[] expectedInaccesible, VDBMetaData vdb) throws QueryParserException, QueryResolverException, TeiidComponentException {
        QueryParser parser = QueryParser.getQueryParser();
        Command command = parser.parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);
        
        vdb.addAttchment(QueryMetadataInterface.class, metadata);
        
        HashMap<String, DataPolicy> policies = new HashMap<String, DataPolicy>();
        policies.put(policy.getName(), policy);
        
        AuthorizationValidationVisitor visitor = new AuthorizationValidationVisitor(vdb, true, policies, "test"); //$NON-NLS-1$
        ValidatorReport report = Validator.validate(command, metadata, visitor);
        if(report.hasItems()) {
            ValidatorFailure firstFailure = (ValidatorFailure) report.getItems().iterator().next();
            
            // strings
            Set expected = new HashSet(Arrays.asList(expectedInaccesible));
            // elements
            Set actual = new HashSet();
            Iterator iter = firstFailure.getInvalidObjects().iterator();
            while(iter.hasNext()) {
                Symbol symbol = (Symbol) iter.next();
                actual.add(symbol.getName());
            }
            assertEquals(expected, actual);
        } else if(expectedInaccesible.length > 0) {
            fail("Expected inaccessible objects, but got none.");                 //$NON-NLS-1$
        }
    }
    
    public void testEverythingAccessible() throws Exception {
        helpTest(exampleAuthSvc1(), "SELECT e1 FROM pm1.g1", FakeMetadataFactory.example1Cached(), new String[] {}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
    }
    
    public void testEverythingAccessible1() throws Exception {
        helpTest(exampleAuthSvc1(), "SELECT e1 FROM (select e1 from pm1.g1) x", FakeMetadataFactory.example1Cached(), new String[] {}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
    }
    
    public void testEverythingAccessible2() throws Exception {
        helpTest(exampleAuthSvc1(), "SELECT lookup('pm1.g1', 'e1', 'e1', '1'), e1 FROM (select e1 from pm1.g1) x", FakeMetadataFactory.example1Cached(), new String[] {}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
    }

    public void testInaccesibleElement() throws Exception {        
        helpTest(exampleAuthSvc1(), "SELECT e2 FROM pm1.g1", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g1.e2"}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testInaccesibleElement2() throws Exception {        
        helpTest(exampleAuthSvc1(), "SELECT lookup('pm1.g1', 'e1', 'e2', '1')", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g1.e2"}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testInaccesibleGroup() throws Exception {        
        helpTest(exampleAuthSvc1(), "SELECT e1 FROM pm1.g2", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g2", "pm1.g2.e1"}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testInsert() throws Exception {        
        helpTest(exampleAuthSvc1(), "INSERT INTO pm1.g1 (e1, e2, e3, e4) VALUES ('x', 5, {b'true'}, 1.0)", FakeMetadataFactory.example1Cached(), new String[] {}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
    }

    public void testInsertInaccessible() throws Exception {        
        helpTest(exampleAuthSvc1(), "INSERT INTO pm1.g2 (e1, e2, e3, e4) VALUES ('x', 5, {b'true'}, 1.0)", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g2.e1"}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testUpdate() throws Exception {        
        helpTest(exampleAuthSvc1(), "UPDATE pm1.g1 SET e2 = 5", FakeMetadataFactory.example1Cached(), new String[] {}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
    }

    public void testUpdateCriteriaInaccessibleForRead() throws Exception {        
        helpTest(exampleAuthSvc1(), "UPDATE pm1.g2 SET e2 = 5 WHERE e1 = 'x'", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g2.e1"}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testUpdateElementInaccessibleForUpdate() throws Exception {        
        helpTest(exampleAuthSvc1(), "UPDATE pm1.g1 SET e1 = 5 WHERE e1 = 'x'", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g1.e1"}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDelete() throws Exception {        
        helpTest(exampleAuthSvc1(), "DELETE FROM pm1.g1", FakeMetadataFactory.example1Cached(), new String[] {}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
    }

    public void testDeleteCriteriaInaccesibleForRead() throws Exception {        
        helpTest(exampleAuthSvc1(), "DELETE FROM pm1.g2 WHERE e1 = 'x'", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g2.e1"}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDeleteInaccesibleGroup() throws Exception {        
        helpTest(exampleAuthSvc1(), "DELETE FROM pm1.g3", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g3"}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testProc() throws Exception {
        helpTest(exampleAuthSvc1(), "EXEC pm1.sq1()", FakeMetadataFactory.example1Cached(), new String[] {}, FakeMetadataFactory.example1VDB());         //$NON-NLS-1$
    }

    public void testProcInaccesible() throws Exception {
        helpTest(exampleAuthSvc1(), "EXEC pm1.sq2('xyz')", FakeMetadataFactory.example1Cached(), new String[] {"pm1.sq2"}, FakeMetadataFactory.example1VDB());         //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSelectIntoEverythingAccessible() throws Exception {
        helpTest(exampleAuthSvc2(), "SELECT e1, e2, e3, e4 INTO pm1.g2 FROM pm2.g1", FakeMetadataFactory.example1Cached(), new String[] {}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
    }

    public void testSelectIntoTarget_e1_NotAccessible() throws Exception {
        helpTest(exampleAuthSvc2(), "SELECT e1, e2, e3, e4 INTO pm2.g2 FROM pm2.g1", FakeMetadataFactory.example1Cached(), new String[] {"pm2.g2.e2","pm2.g2.e4","pm2.g2.e3"}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testSelectIntoTarget_e1e2_NotAccessible() throws Exception {
        helpTest(exampleAuthSvc2(), "SELECT e1, e2, e3, e4 INTO pm3.g2 FROM pm2.g1", FakeMetadataFactory.example1Cached(), new String[] {"pm3.g2.e4", "pm3.g2.e3"},FakeMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public void testTempTableSelectInto() throws Exception {
        helpTest(exampleAuthSvc1(), "SELECT e1 INTO #temp FROM pm1.g1", FakeMetadataFactory.example1Cached(), new String[] {}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
    }
    
    public void testTempTableSelectInto1() throws Exception {
        helpTest(exampleAuthSvc1(), "SELECT e1, e2 INTO #temp FROM pm1.g1", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g1.e2"}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testTempTableInsert() throws Exception {
        helpTest(exampleAuthSvc2(), "insert into #temp (e1, e2, e3, e4) values ('1', '2', '3', '4')", FakeMetadataFactory.example1Cached(), new String[] {}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$
    }

    public void testXMLAccessible() throws Exception {
        helpTest(exampleAuthSvc2(), "select * from xmltest.doc1", FakeMetadataFactory.example1Cached(), new String[] {"xmltest.doc1"}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testXMLInAccessible() throws Exception {
        helpTest(exampleAuthSvc1(), "select * from xmltest.doc1", FakeMetadataFactory.example1Cached(), new String[] {"xmltest.doc1"}, FakeMetadataFactory.example1VDB()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
	private void helpTestLookupVisibility(boolean visible) throws QueryParserException, QueryValidatorException, TeiidComponentException {
		VDBMetaData vdb = FakeMetadataFactory.example1VDB();
		vdb.getModel("pm1").setVisible(visible); //$NON-NLS-1$
		AuthorizationValidationVisitor mvvv = new AuthorizationValidationVisitor(vdb, false, new HashMap<String, DataPolicy>(), "test"); //$NON-NLS-1$
		String sql = "select lookup('pm1.g1', 'e1', 'e2', 1)"; //$NON-NLS-1$
		Command command = QueryParser.getQueryParser().parseCommand(sql);
		Request.validateWithVisitor(mvvv, FakeMetadataFactory.example1Cached(), command);
	}
	
	public void testLookupVisibility() throws Exception {
		helpTestLookupVisibility(true);
	}
	
	public void testLookupVisibilityFails() throws Exception {
		try {
			helpTestLookupVisibility(false);
			fail("expected exception"); //$NON-NLS-1$
		} catch (QueryValidatorException e) {
			assertEquals("Group does not exist: pm1.g1", e.getMessage()); //$NON-NLS-1$
		}
	}

}
