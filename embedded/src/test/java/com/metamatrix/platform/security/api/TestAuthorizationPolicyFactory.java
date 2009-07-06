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

/*
 * Date: Jan 22, 2004
 * Time: 8:39:33 PM
 */
package com.metamatrix.platform.security.api;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import junit.framework.TestCase;

import org.xml.sax.SAXException;

import com.metamatrix.core.util.FileUtil;
import com.metamatrix.core.util.UnitTestUtil;

public final class TestAuthorizationPolicyFactory extends TestCase {

    static String[] roleNames = new String[] {"admin", "user", "other"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    
    static String[] descriptions = new String[] {"This is a description", null, "I have no permissions"}; //$NON-NLS-1$ //$NON-NLS-2$
    
    static String[][] resourceNames = new String[][] {{"x", "y"}, {"a"}, {}}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    
    static String[][] groupNames = new String[][] {{"group1", "group2"}, {}, {"group3"}}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    
    static int[][] actionValues = new int[][] {{StandardAuthorizationActions.ALL_VALUE, StandardAuthorizationActions.DATA_CREATE_VALUE}, {13}, {}};
    
    
    public TestAuthorizationPolicyFactory(String name) {
        super(name);
    }
    
    public void testExport() throws Exception {
        
        String vdbName = "vdbName"; //$NON-NLS-1$
        String vdbVersion = "1"; //$NON-NLS-1$
        
        AuthorizationRealm realm = new AuthorizationRealm(vdbName, vdbVersion);
        
        BasicAuthorizationPermissionFactory bapf = new BasicAuthorizationPermissionFactory();
        
        List policies = new ArrayList();
        
        for (int i = 0; i < roleNames.length; i++) {
            AuthorizationPolicyID policyID = new AuthorizationPolicyID(roleNames[i], vdbName, vdbVersion);
            
            AuthorizationPolicy policy = new AuthorizationPolicy(policyID);
            
            policy.setDescription(descriptions[i]);
            
            policies.add(policy);
            for (int j = 0; j < resourceNames[i].length; j++) {
                policy.addPermission(bapf.create(resourceNames[i][j], realm, StandardAuthorizationActions.getAuthorizationActions(actionValues[i][j])));
            }
            
            for (int j = 0; j < groupNames[i].length; j++) {
                policy.addPrincipal(new MetaMatrixPrincipalName(groupNames[i][j], MetaMatrixPrincipal.TYPE_GROUP));
            }
            
        }
        
        //add doc to test against
        char[] result = AuthorizationPolicyFactory.exportPolicies(policies);
        String expected = FileUtil.read(new FileReader(UnitTestUtil.getTestDataPath()+File.separator+"permissions.xml")); //$NON-NLS-1$
        String actual = new String(result).replaceAll("\r\n", "\n"); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals(expected, actual); 
    }
    
    public void testImport() throws Exception {
        String vdbName = "vdbNamexx"; //$NON-NLS-1$
        String vdbVersion = "11"; //$NON-NLS-1$
                
        Collection roles = AuthorizationPolicyFactory.buildPolicies(vdbName,vdbVersion,FileUtil.read(new FileReader(UnitTestUtil.getTestDataPath()+File.separator+ "permissions.xml")).toCharArray()); //$NON-NLS-1$

        assertEquals(3, roles.size());

        for (final Iterator i = roles.iterator(); i.hasNext();) {
            final AuthorizationPolicy policy = (AuthorizationPolicy)i.next();
            AuthorizationPolicyID policyID = policy.getAuthorizationPolicyID();

            assertTrue(Arrays.asList(roleNames).indexOf(policyID.getDisplayName()) != -1);
            int index = Arrays.asList(roleNames).indexOf(policyID.getDisplayName());
            if (index == 2) {
                i.remove();
            }

            if (index == 1) {
                policy.removePermissions();
            }
        }

        char[] result = AuthorizationPolicyFactory.exportPolicies(roles);
        String expected = FileUtil.read(new FileReader(UnitTestUtil.getTestDataPath() + "/permissions2.xml")); //$NON-NLS-1$
        String actual = new String(result).replaceAll("\r\n", "\n"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(expected, actual); 
    }
    
    public void testParsingFails() throws Exception {
        
        try {
            AuthorizationPolicyFactory.buildPolicies("foo", "bar", "<notvalid/>".toCharArray()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            fail("expected exception"); //$NON-NLS-1$
        } catch (SAXException e) {
            assertEquals("Error during parsing authorizations: cvc-elt.1: Cannot find the declaration of element 'notvalid'.", e.getMessage()); //$NON-NLS-1$
        }
        
    }
    
    public void testAdminRoles() {
    	Properties p = new Properties();
    	p.setProperty("Admin.SystemAdmin", "GroupA, GroupB"); //$NON-NLS-1$ //$NON-NLS-2$
    	p.setProperty("Admin.ProductAdmin", "GroupB"); //$NON-NLS-1$ //$NON-NLS-2$
    	p.setProperty("Admin.ReadOnlyAdmin", "");//$NON-NLS-1$ //$NON-NLS-2$
    	
    	Collection<AuthorizationPolicy> policies = AuthorizationPolicyFactory.buildAdminPolicies(p);
    	
    	assertEquals(3, policies.size());
    	
    	for (AuthorizationPolicy policy: policies) {
    		if (policy.getAuthorizationPolicyID().getName().equalsIgnoreCase("Admin.SystemAdmin")) { //$NON-NLS-1$
    			Set<MetaMatrixPrincipalName> principals = policy.getPrincipals();
    			assertEquals(2, principals.size());
    		}
    		else if (policy.getAuthorizationPolicyID().getName().equalsIgnoreCase("Admin.ProductAdmin")) { //$NON-NLS-1$
    			Set<MetaMatrixPrincipalName> principals = policy.getPrincipals();
    			assertEquals(1, principals.size());
    			assertEquals(new MetaMatrixPrincipalName("GroupB", MetaMatrixPrincipal.TYPE_GROUP), principals.iterator().next()); //$NON-NLS-1$
    		}
    	}
    }
    
}