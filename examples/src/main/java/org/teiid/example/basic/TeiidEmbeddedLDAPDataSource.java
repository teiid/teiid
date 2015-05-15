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
package org.teiid.example.basic;

import static org.teiid.example.util.JDBCUtils.executeQuery;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.teiid.example.ExampleBase;
import org.teiid.example.util.FileUtils;
import org.teiid.resource.adapter.ldap.LDAPManagedConnectionFactory;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.ldap.LDAPExecutionFactory;

public class TeiidEmbeddedLDAPDataSource extends ExampleBase {
	
	private static String ldapUrl = "ldap://127.0.0.1:389";
	private static String ldapAdminUserDN = "cn=Manager,dc=example,dc=com";
	private static String ldapAdminPassword = "redhat";
	
	@Override
	public void execute(String vdb) throws Exception {

		initLDAPProperties();
		
		server = new EmbeddedServer();
		
		LDAPExecutionFactory factory = new LDAPExecutionFactory();
		factory.start();
		server.addTranslator("translator-ldap", factory);
		
		LDAPManagedConnectionFactory managedconnectionFactory = new LDAPManagedConnectionFactory();
		managedconnectionFactory.setLdapUrl(ldapUrl);
		managedconnectionFactory.setLdapAdminUserDN(ldapAdminUserDN);
		managedconnectionFactory.setLdapAdminUserPassword(ldapAdminPassword);
		server.addConnectionFactory("java:/ldapDS", managedconnectionFactory.createConnectionFactory());
		
		start(false);
		
		server.deployVDB(new ByteArrayInputStream(vdb.getBytes()));
		
		conn = server.getDriver().connect("jdbc:teiid:ldapVDB", null);
		
		executeQuery(conn, "SELECT * FROM HR_Group");
		
		tearDown();
	}

	public static void main(String[] args) throws Exception {
		new TeiidEmbeddedLDAPDataSource().execute(FileUtils.readFileContent("ldap-as-a-datasource", "ldap-vdb.xml"));
	}

	private static void initLDAPProperties() throws IOException {
		
		Properties prop = new Properties();
		InputStream input = null;
		
		try { 
			input = new FileInputStream(FileUtils.readFile("ldap-as-a-datasource", "ldap.properties"));
			prop.load(input);
			
			if(prop.getProperty("ldap.url") != null) {
				ldapUrl = prop.getProperty("ldap.url");
			}
			
			if(prop.getProperty("ldap.adminUserDN") != null) {
				ldapAdminUserDN = prop.getProperty("ldap.adminUserDN");
			}
			
			if(prop.getProperty("ldap.adminUserPassword") != null) {
				ldapAdminPassword = prop.getProperty("ldap.adminUserPassword");
			}
	 
		} catch (IOException e) {
			throw e;
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	

}
