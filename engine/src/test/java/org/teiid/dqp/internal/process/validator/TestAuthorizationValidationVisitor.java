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

package org.teiid.dqp.internal.process.validator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.teiid.dqp.internal.process.validator.AuthorizationValidationVisitor;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.dqp.service.AuthorizationService;
import com.metamatrix.dqp.service.FakeAuthorizationService;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.symbol.Symbol;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.validator.Validator;
import com.metamatrix.query.validator.ValidatorFailure;
import com.metamatrix.query.validator.ValidatorReport;

public class TestAuthorizationValidationVisitor extends TestCase {

    public static final String CONN_ID = "connID"; //$NON-NLS-1$

    /**
     * Constructor for TestAuthorizationValidationVisitor.
     * @param name
     */
    public TestAuthorizationValidationVisitor(String name) {
        super(name);
    }

    private AuthorizationService exampleAuthSvc1() {
        FakeAuthorizationService svc = new FakeAuthorizationService(false);
        
        // pm1.g1
        svc.addResource(CONN_ID, AuthorizationService.ACTION_DELETE, "pm1.g1"); //$NON-NLS-1$
        
        svc.addResource(CONN_ID, AuthorizationService.ACTION_READ, "pm1.g1"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_READ, "pm1.g1.e1"); //$NON-NLS-1$

        svc.addResource(CONN_ID, AuthorizationService.ACTION_CREATE, "pm1.g1"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_CREATE, "pm1.g1.e1"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_CREATE, "pm1.g1.e2"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_CREATE, "pm1.g1.e3"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_CREATE, "pm1.g1.e4"); //$NON-NLS-1$

        svc.addResource(CONN_ID, AuthorizationService.ACTION_UPDATE, "pm1.g1"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_UPDATE, "pm1.g1.e2"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_UPDATE, "pm1.g1.e3"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_UPDATE, "pm1.g1.e4"); //$NON-NLS-1$

        // pm1.g2
        svc.addResource(CONN_ID, AuthorizationService.ACTION_CREATE, "pm1.g2"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_CREATE, "pm1.g2.e2"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_CREATE, "pm1.g2.e3"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_CREATE, "pm1.g2.e4"); //$NON-NLS-1$

        svc.addResource(CONN_ID, AuthorizationService.ACTION_UPDATE, "pm1.g2"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_UPDATE, "pm1.g2.e2"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_UPDATE, "pm1.g2.e3"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_UPDATE, "pm1.g2.e4"); //$NON-NLS-1$

        // pm1.g4
        svc.addResource(CONN_ID, AuthorizationService.ACTION_DELETE, "pm1.g4"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_DELETE, "pm1.g4.e1"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.ACTION_DELETE, "pm1.g4.e2"); //$NON-NLS-1$

        // pm1.sq2
        svc.addResource(CONN_ID, AuthorizationService.ACTION_READ, "pm1.sq1"); //$NON-NLS-1$
        
        return svc;
    }
    
    //allow by default
    private AuthorizationService exampleAuthSvc2() {
        FakeAuthorizationService svc = new FakeAuthorizationService(true);
        
        // pm2.g2
        svc.addResource(CONN_ID, AuthorizationService.CONTEXT_INSERT, "pm2.g2.e1"); //$NON-NLS-1$
        
        // pm3.g2
        svc.addResource(CONN_ID, AuthorizationService.CONTEXT_INSERT, "pm3.g2.e1"); //$NON-NLS-1$
        svc.addResource(CONN_ID, AuthorizationService.CONTEXT_INSERT, "pm3.g2.e2"); //$NON-NLS-1$
        
        return svc;
    }

    private void helpTest(AuthorizationService svc, String sql, QueryMetadataInterface metadata, String[] expectedInaccesible) throws QueryParserException, QueryResolverException, MetaMatrixComponentException {
        QueryParser parser = QueryParser.getQueryParser();
        Command command = parser.parseCommand(sql);
        QueryResolver.resolveCommand(command, metadata);
        
        AuthorizationValidationVisitor visitor = new AuthorizationValidationVisitor(CONN_ID, svc);
        ValidatorReport report = Validator.validate(command, metadata, visitor, true);
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
        helpTest(exampleAuthSvc1(), "SELECT e1 FROM pm1.g1", FakeMetadataFactory.example1Cached(), new String[] {}); //$NON-NLS-1$
    }
    
    public void testEverythingAccessible1() throws Exception {
        helpTest(exampleAuthSvc1(), "SELECT e1 FROM (select e1 from pm1.g1) x", FakeMetadataFactory.example1Cached(), new String[] {}); //$NON-NLS-1$
    }
    
    public void testEverythingAccessible2() throws Exception {
        helpTest(exampleAuthSvc1(), "SELECT lookup('pm1.g1', 'e1', 'e1', '1'), e1 FROM (select e1 from pm1.g1) x", FakeMetadataFactory.example1Cached(), new String[] {}); //$NON-NLS-1$
    }

    public void testInaccesibleElement() throws Exception {        
        helpTest(exampleAuthSvc1(), "SELECT e2 FROM pm1.g1", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g1.e2"}); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testInaccesibleElement2() throws Exception {        
        helpTest(exampleAuthSvc1(), "SELECT lookup('pm1.g1', 'e1', 'e2', '1')", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g1.e2"}); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testInaccesibleGroup() throws Exception {        
        helpTest(exampleAuthSvc1(), "SELECT e1 FROM pm1.g2", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g2", "pm1.g2.e1"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testInsert() throws Exception {        
        helpTest(exampleAuthSvc1(), "INSERT INTO pm1.g1 (e1, e2, e3, e4) VALUES ('x', 5, {b'true'}, 1.0)", FakeMetadataFactory.example1Cached(), new String[] {}); //$NON-NLS-1$
    }

    public void testInsertInaccessible() throws Exception {        
        helpTest(exampleAuthSvc1(), "INSERT INTO pm1.g2 (e1, e2, e3, e4) VALUES ('x', 5, {b'true'}, 1.0)", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g2.e1"}); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testUpdate() throws Exception {        
        helpTest(exampleAuthSvc1(), "UPDATE pm1.g1 SET e2 = 5", FakeMetadataFactory.example1Cached(), new String[] {}); //$NON-NLS-1$
    }

    public void testUpdateCriteriaInaccessibleForRead() throws Exception {        
        helpTest(exampleAuthSvc1(), "UPDATE pm1.g2 SET e2 = 5 WHERE e1 = 'x'", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g2.e1"}); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testUpdateElementInaccessibleForUpdate() throws Exception {        
        helpTest(exampleAuthSvc1(), "UPDATE pm1.g1 SET e1 = 5 WHERE e1 = 'x'", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g1.e1"}); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDelete() throws Exception {        
        helpTest(exampleAuthSvc1(), "DELETE FROM pm1.g1", FakeMetadataFactory.example1Cached(), new String[] {}); //$NON-NLS-1$
    }

    public void testDeleteCriteriaInaccesibleForRead() throws Exception {        
        helpTest(exampleAuthSvc1(), "DELETE FROM pm1.g2 WHERE e1 = 'x'", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g2.e1"}); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDeleteInaccesibleGroup() throws Exception {        
        helpTest(exampleAuthSvc1(), "DELETE FROM pm1.g3", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g3"}); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testProc() throws Exception {
        helpTest(exampleAuthSvc1(), "EXEC pm1.sq1()", FakeMetadataFactory.example1Cached(), new String[] {});         //$NON-NLS-1$
    }

    public void testProcInaccesible() throws Exception {
        helpTest(exampleAuthSvc1(), "EXEC pm1.sq2('xyz')", FakeMetadataFactory.example1Cached(), new String[] {"pm1.sq2"});         //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSelectIntoEverythingAccessible() throws Exception {
        helpTest(exampleAuthSvc2(), "SELECT e1, e2, e3, e4 INTO pm1.g2 FROM pm2.g1", FakeMetadataFactory.example1Cached(), new String[] {}); //$NON-NLS-1$
    }

    public void testSelectIntoTarget_e1_NotAccessible() throws Exception {
        helpTest(exampleAuthSvc2(), "SELECT e1, e2, e3, e4 INTO pm2.g2 FROM pm2.g1", FakeMetadataFactory.example1Cached(), new String[] {"pm2.g2.e1"}); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSelectIntoTarget_e1e2_NotAccessible() throws Exception {
        helpTest(exampleAuthSvc2(), "SELECT e1, e2, e3, e4 INTO pm3.g2 FROM pm2.g1", FakeMetadataFactory.example1Cached(), new String[] {"pm3.g2.e1", "pm3.g2.e2"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
    public void testProcGloballyAccessible() throws Exception {
        AuthorizationValidationVisitor.addGloballyAccessibleProcedure("pm1.sq2");  //$NON-NLS-1$ 
        helpTest(exampleAuthSvc1(), "EXEC pm1.sq2('xyz')", FakeMetadataFactory.example1Cached(), new String[] {});  //$NON-NLS-1$ 
        
        AuthorizationValidationVisitor.removeGloballyAccessibleProcedure("pm1.sq2");  //$NON-NLS-1$
        helpTest(exampleAuthSvc1(), "EXEC pm1.sq2('xyz')", FakeMetadataFactory.example1Cached(), new String[] {"pm1.sq2"});  //$NON-NLS-1$//$NON-NLS-2$
    }
    
    public void testTempTableSelectInto() throws Exception {
        helpTest(exampleAuthSvc1(), "SELECT e1 INTO #temp FROM pm1.g1", FakeMetadataFactory.example1Cached(), new String[] {}); //$NON-NLS-1$
    }
    
    public void testTempTableSelectInto1() throws Exception {
        helpTest(exampleAuthSvc1(), "SELECT e1, e2 INTO #temp FROM pm1.g1", FakeMetadataFactory.example1Cached(), new String[] {"pm1.g1.e2"}); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testTempTableInsert() throws Exception {
        helpTest(exampleAuthSvc2(), "insert into #temp (e1, e2, e3, e4) values ('1', '2', '3', '4')", FakeMetadataFactory.example1Cached(), new String[] {}); //$NON-NLS-1$
    }

    public void testXMLAccessible() throws Exception {
        helpTest(exampleAuthSvc2(), "select * from xmltest.doc1", FakeMetadataFactory.example1Cached(), new String[] {}); //$NON-NLS-1$
    }
    
    public void testXMLInAccessible() throws Exception {
        helpTest(exampleAuthSvc1(), "select * from xmltest.doc1", FakeMetadataFactory.example1Cached(), new String[] {"xmltest.doc1"}); //$NON-NLS-1$ //$NON-NLS-2$
    }
}
