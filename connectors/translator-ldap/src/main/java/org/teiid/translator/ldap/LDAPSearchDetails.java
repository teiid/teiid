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

package org.teiid.translator.ldap;

import java.util.ArrayList;
import java.util.Iterator;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchControls;
import javax.naming.ldap.SortKey;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;



/**
 * Utility class used to maintain the details of a particular LDAP search,
 * such as the context, the attributes of interest, the filter, and the 
 * search scope.
 */
public class LDAPSearchDetails {
	private String contextName;
	private int searchScope;
	private String contextFilter;
	private ArrayList attributeList;
	private SortKey[] keys;
//	private LdapSortKey[] netscapeKeys;
	// If limit is set to -1, this means no limit (return all rows)
	private long limit;
	private ArrayList<Column> elementList;
	
	/**
	 * Constructor
	 * @param name the context name
	 * @param searchScope the search scope
	 * @param filter the context filter
	 * @param attributeList the list of attributes
	 * @param keys
	 * @param limit
	 * @param elementList
	 */
	public LDAPSearchDetails(String name, int searchScope, String filter, ArrayList attributeList, SortKey[] keys, long limit, ArrayList elementList) {

		this.contextName = name;
		this.searchScope = searchScope;
		this.contextFilter = filter;
		this.attributeList = attributeList;
		this.keys = keys;
		this.limit = limit;
		this.elementList = elementList;
	}
	
	/**
	 * get the context name
	 * @return the context name
	 */
	public String getContextName() {
		return contextName;
	}
	
	/**
	 * get the context name
	 * @return the context name
	 */
	public int getSearchScope() {
		return searchScope;
	}
	
	/**
	 * get the context filter
	 * @return the context filter
	 */
	public String getContextFilter() {
		return contextFilter;
	}
	
	/**
	 * get the attribute list
	 * @return the attribute list
	 */
	public ArrayList getAttributeList() {
		return attributeList;
	}
	
	/**
	 * get the element list
	 * @return the element list
	 */
	public ArrayList<Column> getElementList() {
		return elementList;
	}
	
	/**
	 * get the sort keys
	 * @return the sort keys
	 */
	public SortKey[] getSortKeys() {
		return keys;
	}
	
	/**
	 * get the count limit
	 * @return the count limit
	 */
	public long getCountLimit() {
		return limit;
	}
	/*
	public LdapSortKey[] getNetscapeSortKeys() {
		return netscapeKeys;
	}
	private void createNetscapeKeys() {	
		if(keys != null) {
			netscapeKeys = new LdapSortKey[keys.length];
			for(int i=0; i<keys.length; i++) {
				LdapSortKey nKey = new LdapSortKey(keys[i].getAttributeID(), 
						keys[i].isAscending());
				netscapeKeys[i] = nKey;
			}
		} else {
			// set it null
			netscapeKeys = null;
		}
	}	
	*/
	
	/**
	 * Print Method for Logging - (Detail level logging)
	 */
	public void printDetailsToLog() throws NamingException {
		// Log Search Scope
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Search context: " + contextName); //$NON-NLS-1$
		if(searchScope == SearchControls.SUBTREE_SCOPE) {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR,"Search scope = SUBTREE_SCOPE"); //$NON-NLS-1$
		} else if(searchScope == SearchControls.OBJECT_SCOPE) {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR,"Search scope = OBJECT_SCOPE"); //$NON-NLS-1$
		} else if(searchScope == SearchControls.ONELEVEL_SCOPE) {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR,"Search scope = ONELEVEL_SCOPE"); //$NON-NLS-1$
		}
		
		// Log Search Attributes
		LogManager.logDetail(LogConstants.CTX_CONNECTOR,"Search attributes: "); //$NON-NLS-1$	
		Iterator itr = this.attributeList.iterator();
		int i = 0;
		while(itr.hasNext()) {
			Attribute attr = (Attribute)itr.next();
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Attribute [" + i + "]: " + attr.getID() + " (" +attr.get().toString() + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
			i++;
		}
		
		// Log Context Filter
		if(contextFilter != null && (!contextFilter.equals(""))) { //$NON-NLS-1$
			LogManager.logDetail(LogConstants.CTX_CONNECTOR,"Where clause was translated into Ldap search filter: " + contextFilter); //$NON-NLS-1$
		}
		
		// Log Sort Keys
		if(keys != null) { 
			LogManager.logDetail(LogConstants.CTX_CONNECTOR,"Sort keys: "); //$NON-NLS-1$
			for(int j=0; j<keys.length; j++) {
				LogManager.logDetail(LogConstants.CTX_CONNECTOR,"\tName: " + keys[j].getAttributeID()); //$NON-NLS-1$
				LogManager.logDetail(LogConstants.CTX_CONNECTOR,"\tOrder: "); //$NON-NLS-1$
				if(keys[j].isAscending()) {
					LogManager.logDetail(LogConstants.CTX_CONNECTOR,"ASC"); //$NON-NLS-1$
				} else {
					LogManager.logDetail(LogConstants.CTX_CONNECTOR,"DESC"); //$NON-NLS-1$
				}
			}
		}
	}
}
