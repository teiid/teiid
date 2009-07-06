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

package com.metamatrix.platform.security.api;

import java.io.ByteArrayOutputStream;
import java.io.CharArrayReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.DOMBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.platform.security.util.RolePermissionFactory;

/**
 * The class build the Policies from the xml file or converts the policies to xml file for importing and exporting of the policy
 * files from one server to another. look in the authorizations.xsd in this package for the format of the XML being imported and
 * exported
 */
public class AuthorizationPolicyFactory {

    private static final String GROUP = "group"; //$NON-NLS-1$
    private static final String PRINCIPALS = "principals"; //$NON-NLS-1$
    private static final String ALLOW = "allow-"; //$NON-NLS-1$
    private static final String RESOURCE_NAME = "resource-name"; //$NON-NLS-1$
    private static final String PERMISSION = "permission"; //$NON-NLS-1$
    private static final String PERMISSIONS = "permissions"; //$NON-NLS-1$
    private static final String DESCRIPTION = "description"; //$NON-NLS-1$
    private static final String VDB_VERSION = "vdb-version"; //$NON-NLS-1$
    private static final String VDB_NAME = "vdb-name"; //$NON-NLS-1$
    private static final String NAME = "name"; //$NON-NLS-1$
    private static final String DATA_ROLE = "data-role"; //$NON-NLS-1$
    private static final String ROLES = "roles"; //$NON-NLS-1$
    private static final String REALM = "realm"; //$NON-NLS-1$
    
    static final String JAXP_SCHEMA_SOURCE = "http://java.sun.com/xml/jaxp/properties/schemaSource"; //$NON-NLS-1$
    static final String JAXP_SCHEMA_LANGUAGE = "http://java.sun.com/xml/jaxp/properties/schemaLanguage"; //$NON-NLS-1$
    static final String W3C_XML_SCHEMA = "http://www.w3.org/2001/XMLSchema"; //$NON-NLS-1$
    
    private static String[] ALLOW_TYPES = new String[] {
        StandardAuthorizationActions.DATA_CREATE_LABEL,
        StandardAuthorizationActions.DATA_DELETE_LABEL,
        StandardAuthorizationActions.DATA_UPDATE_LABEL,
        StandardAuthorizationActions.DATA_READ_LABEL,
    };

    public static Collection<AuthorizationPolicy> buildPolicies(String vdbName, String vdbVersion, char[] xmlContents) 
        throws SAXException, IOException, ParserConfigurationException {

        DOMBuilder builder = new DOMBuilder();
        
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setAttribute(JAXP_SCHEMA_LANGUAGE, W3C_XML_SCHEMA);
        documentBuilderFactory.setAttribute(JAXP_SCHEMA_SOURCE, AuthorizationPolicyFactory.class.getResourceAsStream("authorizations.xsd")); //$NON-NLS-1$
        documentBuilderFactory.setValidating(true);
        DocumentBuilder docBuilder = documentBuilderFactory.newDocumentBuilder();
        docBuilder.setErrorHandler(new ErrorHandler() {

            public void warning(SAXParseException arg0) throws SAXException {
                LogManager.logWarning(LogConstants.CTX_AUTHORIZATION,arg0,SecurityPlugin.Util.getString("AuthorizationPolicyFactory.parsing_warning", arg0.getMessage())); //$NON-NLS-1$
            }

            public void error(SAXParseException arg0) throws SAXException {
                throw new SAXException(SecurityPlugin.Util.getString("AuthorizationPolicyFactory.parsing_error", arg0.getMessage()), arg0); //$NON-NLS-1$
            }

            public void fatalError(SAXParseException arg0) throws SAXException {
                throw new SAXException(SecurityPlugin.Util.getString("AuthorizationPolicyFactory.parsing_error", arg0.getMessage()), arg0); //$NON-NLS-1$
            }
        });
        
        Document doc = builder.build(docBuilder.parse(new InputSource(new CharArrayReader(xmlContents))));

        Element root = doc.getRootElement();
        Element roles = root.getChild(ROLES);
        List dataRoles = roles.getChildren(DATA_ROLE);

        AuthorizationRealm realm = new AuthorizationRealm(vdbName, vdbVersion);
        BasicAuthorizationPermissionFactory bapf = new BasicAuthorizationPermissionFactory();

        List<AuthorizationPolicy> result = new ArrayList<AuthorizationPolicy>();

        for (final Iterator iter = dataRoles.iterator(); iter.hasNext();) {
            final Element role = (Element)iter.next();

            Element name = role.getChild(NAME);

            AuthorizationPolicyID policyID = new AuthorizationPolicyID(name.getText(), vdbName, vdbVersion);
            AuthorizationPolicy policy = new AuthorizationPolicy(policyID);

            result.add(policy);

            Element description = role.getChild(DESCRIPTION);
            if (description != null) {
                policy.setDescription(description.getText());
            }

            Element permsElem = role.getChild(PERMISSIONS);

            if (permsElem != null) {

                List perms = permsElem.getChildren(PERMISSION);

                for (final Iterator permIter = perms.iterator(); permIter.hasNext();) {
                    final Element perm = (Element)permIter.next();

                    Element resourceElem = perm.getChild(RESOURCE_NAME);

                    String resourceName = resourceElem.getText();

                    int actionsValue = StandardAuthorizationActions.NONE_VALUE;
                    for (int i = 0; i < ALLOW_TYPES.length; i++) {
                        if (perm.getChild(ALLOW+ALLOW_TYPES[i].toLowerCase()) == null) {
                            continue;
                        }
                        AuthorizationActions action = StandardAuthorizationActions.getAuthorizationActions(ALLOW_TYPES[i]);
                        actionsValue |= action.getValue();
                    }
                    AuthorizationPermission permission = bapf.create(resourceName, realm, StandardAuthorizationActions.getAuthorizationActions(actionsValue));
                    policy.addPermission(permission);
                }
            }

            Element principalsElem = role.getChild(PRINCIPALS);

            if (principalsElem != null) {

                List groups = principalsElem.getChildren(GROUP);

                for (final Iterator groupsIter = groups.iterator(); groupsIter.hasNext();) {
                    final Element group = (Element)groupsIter.next();

                    policy.addPrincipal(new MetaMatrixPrincipalName(group.getText(), MetaMatrixPrincipal.TYPE_GROUP));
                }
            }
        }

        return result;
    }

    public static char[] exportPolicies(Collection<AuthorizationPolicy> roles) throws IOException {
        Document doc = new Document(new Element(REALM));

        Element rolesElement = new Element(ROLES);

        doc.getRootElement().addContent(rolesElement);

        for (AuthorizationPolicy policy : roles) {
            AuthorizationPolicyID policyId = policy.getAuthorizationPolicyID();

            Element roleElement = new Element(DATA_ROLE);
            rolesElement.addContent(roleElement);

            roleElement.addContent(new Element(NAME).setText(policyId.getDisplayName()));

            AuthorizationRealm realm = policyId.getRealm();
            roleElement.addContent(new Element(VDB_NAME).setText(realm.getSuperRealmName()));
            roleElement.addContent(new Element(VDB_VERSION).setText(realm.getSubRealmName()));

            roleElement.addContent(new Element(DESCRIPTION).setText(policy.getDescription()));

            // Now add each individual role
            Set permissions = policy.getPermissions();

            if (!permissions.isEmpty()) {
                Element permissionsElement = new Element(PERMISSIONS);
                roleElement.addContent(permissionsElement);

                for (final Iterator permissionIter = permissions.iterator(); permissionIter.hasNext();) {
                    BasicAuthorizationPermission permission = (BasicAuthorizationPermission)permissionIter.next();
                    Element permissionElement = new Element(PERMISSION);
                    permissionsElement.addContent(permissionElement);

                    permissionElement.addContent(new Element(RESOURCE_NAME).setText(permission.getResourceName()));

                    String[] labels = permission.getActions().getLabels();
                    for (int i = 0; i < labels.length; i++) {
                        permissionElement.addContent(new Element(ALLOW + labels[i].toLowerCase()));
                    }
                }
            }

            Set principals = policy.getPrincipals();

            if (!principals.isEmpty()) {
                Element principalsElement = new Element(PRINCIPALS);
                roleElement.addContent(principalsElement);

                for (final Iterator principalsIter = principals.iterator(); principalsIter.hasNext();) {
                    MetaMatrixPrincipalName principal = (MetaMatrixPrincipalName)principalsIter.next();
                    principalsElement.addContent(new Element(GROUP).setText(principal.getName()));
                }
            }
        } // for

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        XMLOutputter outputter = new XMLOutputter();
        outputter.setFormat(Format.getPrettyFormat());
        outputter.output(doc, baos);

        return baos.toString().toCharArray();
    }

    /**
     * The properties will have format of 
     *  role1 = group1, group2
     *  role2 = group3
     *  
     * @param roles
     * @return
     */
	public static Collection<AuthorizationPolicy> buildAdminPolicies(Properties roleMap) {
		List<AuthorizationPolicy> result = new ArrayList<AuthorizationPolicy>();
        Set keys = roleMap.keySet();

        for(Object key:keys) {
        	String role = (String)key;
            AuthorizationPolicyID policyID = new AuthorizationPolicyID(role, role);
            AuthorizationPolicy policy = new AuthorizationPolicy(policyID);

            // allowed groups
            StringTokenizer st = new StringTokenizer(roleMap.getProperty(role), ","); //$NON-NLS-1$
            while (st.hasMoreTokens()) {
            	String group = st.nextToken();
            	MetaMatrixPrincipalName member = new MetaMatrixPrincipalName(group, MetaMatrixPrincipal.TYPE_GROUP);
            	policy.addPrincipal(member);
            }            
            result.add(policy);
        }
		return result;
	}
}
