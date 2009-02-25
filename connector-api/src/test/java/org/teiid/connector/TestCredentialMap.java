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

package org.teiid.connector;

import java.util.HashMap;
import java.util.Map;

import org.teiid.connector.api.CredentialMap;


import junit.framework.TestCase;

/**
 * Note-- due to time constraints, tests make hard-coded assumptions that 
 * keywords for system, user name, and password, are "system", 
 * "user", and "password".  The actual keywords are found in
 * com.metamatrix.platform.security.CredentialsMap:  SYSTEM_KEYWORD,
 * USER_KEYWORD, and PASSWORD_KEYWORD.
 */
public class TestCredentialMap extends TestCase {

	/**
	 * Constructor for TestCredentialMap.
	 * @param arg0
	 */
	public TestCredentialMap(String arg0) {
		super(arg0);
	}
	
	public void testParseNullCredentialString() {
		String command = null;
	try {
            CredentialMap.parseCredentials(command);
			fail("Did not throw exception on null credential string"); //$NON-NLS-1$
		} catch (Exception ex) {
		}
	}
	
	public void testParseEmptyCredentialString() {
		String command = ""; //$NON-NLS-1$
	try {
            CredentialMap.parseCredentials(command);
			fail("Did not throw exception on empyt credential string"); //$NON-NLS-1$
		} catch (Exception ex) {
		}
	}

	
	public void testParseMissingSystem() {
		String command = "(user=myusername)"; //$NON-NLS-1$
		try {
            CredentialMap.parseCredentials(command);
			fail("Did not throw exception on missing system keyword"); //$NON-NLS-1$
		} catch (Exception ex) {
		}
	}
	

	public void testParseNoOpeningParen() {
		String command = "system=mysystem,username=me,mypassword=you)"; //$NON-NLS-1$
		try {
            CredentialMap.parseCredentials(command);
			fail("Did not throw exception on missing open paren"); //$NON-NLS-1$
		} catch (Exception ex) {
		}
	}
	
	public void testParseNoClosingParen() {
		String command = "(system=mysystem,username=me,mypassword=you"; //$NON-NLS-1$
		try {
            CredentialMap.parseCredentials(command);
			fail("Did not throw exception on missing closing paren"); //$NON-NLS-1$
		} catch (Exception ex) {
		}
	}

	public void testCaseSensitivity() throws Exception {
		String command = "(system=mysystem,username=me,mypassword=you)"; //$NON-NLS-1$
        CredentialMap cm = CredentialMap.parseCredentials(command);
        Map map = cm.getSystemCredentials("MySystem"); //$NON-NLS-1$
        if (map == null) {
        	fail("Error looking up MySystem credentials using different cases."); //$NON-NLS-1$
        }
	}

	public void testCorrectMixedEntries() throws Exception {
		String command = "(system=system1,user=username1, password=password1 /  " + //$NON-NLS-1$
				         " system=system2,user=username2, password=password2 /  " + //$NON-NLS-1$
				         " system=system3,user=username3, password=password3 /  " + //$NON-NLS-1$
				         " system=system4,user=username4, password=password4)"; //$NON-NLS-1$

		CredentialMap cm = CredentialMap.parseCredentials(command);
		assertNotNull("Null CredentialsMap returned on correct entry", cm); //$NON-NLS-1$

		int mapSize = cm.getSystems().length;
		assertEquals("Incorrect number of entries in credential map", 4, //$NON-NLS-1$
				mapSize);
				
		// test 1st set of credentials
		String system = "system1"; //$NON-NLS-1$
		String userVal = "username1"; //$NON-NLS-1$
		String passwordVal = "password1"; //$NON-NLS-1$
		
		Map map = cm.getSystemCredentials(system);
		assertNotNull("Null Map returned on correct entry: system1", map); //$NON-NLS-1$

		String user = (String) map.get("user"); //$NON-NLS-1$
		assertTrue("Incorrect username returned for " + system, userVal.equals(user)); //$NON-NLS-1$

		String password = (String) map.get("password"); //$NON-NLS-1$
		assertTrue("Incorrect password returned for " + system, passwordVal.equals(password)); //$NON-NLS-1$

		// test 2nd set of credentials
		system = "system2"; //$NON-NLS-1$
		userVal = "username2"; //$NON-NLS-1$
		passwordVal = "password2"; //$NON-NLS-1$
		
		map = cm.getSystemCredentials(system);
		assertNotNull("Null Map returned on correct entry: " + system, map); //$NON-NLS-1$

		user = (String) map.get("user"); //$NON-NLS-1$
		assertTrue("Incorrect username returned for " + system, userVal.equals(user)); //$NON-NLS-1$

		password = (String) map.get("password"); //$NON-NLS-1$
		assertTrue("Incorrect password returned for " + system, passwordVal.equals(password)); //$NON-NLS-1$

		// test 3rd set of credentials
		system = "system3"; //$NON-NLS-1$
		userVal = "username3"; //$NON-NLS-1$
		passwordVal = "password3"; //$NON-NLS-1$
		
		map = cm.getSystemCredentials(system);
		assertNotNull("Null Map returned on correct entry: " + system, map); //$NON-NLS-1$

		user = (String) map.get("user"); //$NON-NLS-1$
		assertTrue("Incorrect username returned for " + system, userVal.equals(user)); //$NON-NLS-1$

		password = (String) map.get("password"); //$NON-NLS-1$
		assertTrue("Incorrect password returned for " + system, passwordVal.equals(password)); //$NON-NLS-1$

		// test 4th set of credentials
		system = "system4"; //$NON-NLS-1$
		userVal = "username4"; //$NON-NLS-1$
		passwordVal = "password4"; //$NON-NLS-1$
		
		map = cm.getSystemCredentials(system);
		assertNotNull("Null Map returned on correct entry: " + system, map); //$NON-NLS-1$

		user = (String) map.get("user"); //$NON-NLS-1$
		assertTrue("Incorrect username returned for " + system, userVal.equals(user)); //$NON-NLS-1$

		password = (String) map.get("password"); //$NON-NLS-1$
		assertTrue("Incorrect password returned for " + system, passwordVal.equals(password)); //$NON-NLS-1$

	}

	public void testEscapeCharacters() throws Exception {
		String command = "(system=test\\/system1,user=username1, password=\\=password1 /  " + //$NON-NLS-1$
				         " system=system2,user=username2, password=\\=\\,)\\/;password2 /  " + //$NON-NLS-1$
				         " system=system3,user=username3, password=\"\'\\password3 /  " + //$NON-NLS-1$
				         " system=system4,user=username4, password=\\password4)"; //$NON-NLS-1$

		CredentialMap cm = CredentialMap.parseCredentials(command);
		assertNotNull("Null CredentialsMap returned on correct entry", cm); //$NON-NLS-1$

		int mapSize = cm.getSystems().length;
		assertEquals("Incorrect number of entries in credential map", 4, //$NON-NLS-1$
				mapSize);
				
		// test 1st set of credentials
		String system = "test/system1"; //$NON-NLS-1$
		String userVal = "username1"; //$NON-NLS-1$
		String passwordVal = "=password1"; //$NON-NLS-1$

		Map map = cm.getSystemCredentials(system);
		assertNotNull("Null Map returned on correct entry: system1", map); //$NON-NLS-1$

		String user = (String) map.get("user"); //$NON-NLS-1$
		assertTrue("Incorrect username returned for " + system, userVal.equals(user)); //$NON-NLS-1$
        assertTrue("Incorrect username returned for " + system, userVal.equals(cm.getUser(system))); //$NON-NLS-1$
        

		String password = (String) map.get("password"); //$NON-NLS-1$
		assertTrue("Incorrect password returned for " + system, passwordVal.equals(password)); //$NON-NLS-1$
        assertTrue("Incorrect password returned for " + system, passwordVal.equals(cm.getPassword(system))); //$NON-NLS-1$

		// test 2nd set of credentials
		system = "system2"; //$NON-NLS-1$
		userVal = "username2"; //$NON-NLS-1$
		passwordVal = "=,)/;password2"; //$NON-NLS-1$
		
		map = cm.getSystemCredentials(system);
		assertNotNull("Null Map returned on correct entry: " + system, map); //$NON-NLS-1$

		user = (String) map.get("user"); //$NON-NLS-1$
		assertTrue("Incorrect username returned for " + system, userVal.equals(user)); //$NON-NLS-1$
        assertTrue("Incorrect username returned for " + system, userVal.equals(cm.getUser(system))); //$NON-NLS-1$

		password = (String) map.get("password"); //$NON-NLS-1$
		assertTrue("Incorrect password returned for " + system, passwordVal.equals(password)); //$NON-NLS-1$
        assertTrue("Incorrect password returned for " + system, passwordVal.equals(cm.getPassword(system))); //$NON-NLS-1$

		// test 3rd set of credentials
		system = "system3"; //$NON-NLS-1$
		userVal = "username3"; //$NON-NLS-1$
		passwordVal = "\"\'\\password3"; //$NON-NLS-1$
		
		map = cm.getSystemCredentials(system);
		assertNotNull("Null Map returned on correct entry: " + system, map); //$NON-NLS-1$

		user = (String) map.get("user"); //$NON-NLS-1$
		assertTrue("Incorrect username returned for " + system, userVal.equals(user)); //$NON-NLS-1$
        assertTrue("Incorrect username returned for " + system, userVal.equals(cm.getUser(system))); //$NON-NLS-1$

		password = (String) map.get("password"); //$NON-NLS-1$
		assertTrue("Incorrect password returned for " + system, passwordVal.equals(password)); //$NON-NLS-1$
        assertTrue("Incorrect password returned for " + system, passwordVal.equals(cm.getPassword(system))); //$NON-NLS-1$

		// test 4th set of credentials
		system = "system4"; //$NON-NLS-1$
		userVal = "username4"; //$NON-NLS-1$
		passwordVal = "\\password4"; //$NON-NLS-1$
		
		map = cm.getSystemCredentials(system);
		assertNotNull("Null Map returned on correct entry: " + system, map); //$NON-NLS-1$

		user = (String) map.get("user"); //$NON-NLS-1$
		assertTrue("Incorrect username returned for " + system, userVal.equals(user)); //$NON-NLS-1$

		password = (String) map.get("password"); //$NON-NLS-1$
		assertTrue("Incorrect password returned for " + system, passwordVal.equals(password)); //$NON-NLS-1$

	}
    
    private Map getDefaultCredentials() {
        Map defaults = new HashMap();
        defaults.put("user", "defaultUser"); //$NON-NLS-1$ //$NON-NLS-2$
        defaults.put("password", "defaultPassword"); //$NON-NLS-1$ //$NON-NLS-2$
        return defaults;
    }
    
    private Map getSystemCredentials() {
        Map map = new HashMap();
        map.put("user", "user1"); //$NON-NLS-1$ //$NON-NLS-2$
        map.put("password", "password1"); //$NON-NLS-1$ //$NON-NLS-2$
        map.put("bonus1", "ziggy"); //$NON-NLS-1$ //$NON-NLS-2$
        return map;
    }
    
    private Map getPartialSystemCredentials() {
        Map map = new HashMap();
        map.put("bonus2", "ziggy"); //$NON-NLS-1$ //$NON-NLS-2$
        return map;
    }
    
    public CredentialMap setupDefaultsTest(short mode, boolean setDefaults) {
        CredentialMap cm = new CredentialMap();
        if(setDefaults) {
            cm.setDefaultCredentials(getDefaultCredentials());
        }
        cm.addSystemCredentials("sys1", getSystemCredentials()); //$NON-NLS-1$
        cm.addSystemCredentials("sys2", getPartialSystemCredentials()); //$NON-NLS-1$
        cm.setDefaultCredentialMode(mode);
        return cm;
    }
    
    public void assertCredentialsMatch(Map expected, CredentialMap creds, String system) {
        Map sysCreds = creds.getSystemCredentials(system);
        assertEquals(expected, sysCreds);
    }
    
    public void testIgnoreDefaults() {
        CredentialMap cm = setupDefaultsTest(CredentialMap.MODE_IGNORE_DEFAULTS, true);
                
        assertCredentialsMatch(null, cm, "x"); //$NON-NLS-1$
        assertCredentialsMatch(getSystemCredentials(), cm, "sys1"); //$NON-NLS-1$
        assertCredentialsMatch(getPartialSystemCredentials(), cm, "sys2"); //$NON-NLS-1$
    }

    public void testGlobalDefaultsUnknownSystem() {
        CredentialMap cm = setupDefaultsTest(CredentialMap.MODE_USE_DEFAULTS_GLOBALLY, true);
        
        assertCredentialsMatch(getDefaultCredentials(), cm, "x"); //$NON-NLS-1$
        assertCredentialsMatch(getSystemCredentials(), cm, "sys1"); //$NON-NLS-1$
        
        // Get mixture of system and defaults
        Map mixed = new HashMap();
        mixed.putAll(getDefaultCredentials());
        mixed.putAll(getPartialSystemCredentials());
        assertCredentialsMatch(mixed, cm, "sys2"); //$NON-NLS-1$        
    }

    public void testExistingDefaultsUnknownSystem() {
        CredentialMap cm = setupDefaultsTest(CredentialMap.MODE_USE_DEFAULTS_ON_EXISTING, true);
        
        assertCredentialsMatch(null, cm, "x"); //$NON-NLS-1$
        assertCredentialsMatch(getSystemCredentials(), cm, "sys1"); //$NON-NLS-1$

        // Get mixture of system and defaults
        Map mixed = new HashMap();
        mixed.putAll(getDefaultCredentials());
        mixed.putAll(getPartialSystemCredentials());
        assertCredentialsMatch(mixed, cm, "sys2"); //$NON-NLS-1$        
    }

    public void testIgnoreWithNoDefaults() {
        CredentialMap cm = setupDefaultsTest(CredentialMap.MODE_IGNORE_DEFAULTS, false);
                
        assertCredentialsMatch(null, cm, "x"); //$NON-NLS-1$
        assertCredentialsMatch(getSystemCredentials(), cm, "sys1"); //$NON-NLS-1$
        assertCredentialsMatch(getPartialSystemCredentials(), cm, "sys2"); //$NON-NLS-1$
    }

    public void testGlobalDefaultsUnknownSystemNoDefaults() {
        CredentialMap cm = setupDefaultsTest(CredentialMap.MODE_USE_DEFAULTS_GLOBALLY, false);
        
        assertCredentialsMatch(null, cm, "x"); //$NON-NLS-1$
        assertCredentialsMatch(getSystemCredentials(), cm, "sys1"); //$NON-NLS-1$
        assertCredentialsMatch(getPartialSystemCredentials(), cm, "sys2"); //$NON-NLS-1$
    }

    public void testExistingDefaultsUnknownSystemNoDefaults() {
        CredentialMap cm = setupDefaultsTest(CredentialMap.MODE_USE_DEFAULTS_ON_EXISTING, false);
        
        assertCredentialsMatch(null, cm, "x"); //$NON-NLS-1$
        assertCredentialsMatch(getSystemCredentials(), cm, "sys1"); //$NON-NLS-1$
        assertCredentialsMatch(getPartialSystemCredentials(), cm, "sys2"); //$NON-NLS-1$
    }

}
