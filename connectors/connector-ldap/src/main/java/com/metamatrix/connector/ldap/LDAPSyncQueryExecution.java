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

/**
 * 
 * Please see the user's guide for a full description of capabilties, etc.
 * 
 * Description/Assumptions:
 * 1. Table's name in source defines the base DN (or context) for the search.
 * Example: Table.NameInSource=ou=people,dc=gene,dc=com
 * [Optional] The table's name in source can also define a search scope. Append
 * a "?" character as a delimiter to the base DN, and add the search scope string. 
 * The following scopes are available:
 * SUBTREE_SCOPE
 * ONELEVEL_SCOPE
 * OBJECT_SCOPE
 * [Default] LDAPConnectorConstants.ldapDefaultSearchScope 
 * is the default scope used, if no scope is defined (currently, ONELEVEL_SCOPE).
 * 
 * 2. Column's name in source defines the LDAP attribute name.
 * [Default] If no name in source is defined, then we attempt to use the column name
 * as the LDAP attribute name.
 * 
 * 
 * TODO: Implement paged searches -- the LDAP server must support VirtualListViews.
 * TODO: Implement cancel.
 * TODO: Add Sun/Netscape implementation, AD/OpenLDAP implementation.
 * 
 * 
 * Note: 
 * Greater than is treated as >=
 * Less-than is treater as <=
 * If an LDAP entry has more than one entry for an attribute of interest (e.g. a select item), we only return the
 * first occurrance. The first occurance is not predictably the same each time, either, according to the LDAP spec.
 * If an attribute is not present, we return the empty string. Arguably, we could throw an exception.
 * 
 * Sun LDAP won't support Sort Orders for very large datasets. So, we've set the sorting to NONCRITICAL, which
 * allows Sun to ignore the sort order. This will result in the results to come back as unsorted, without any error.
 * 
 * Removed support for ORDER BY for two reasons:
 * 1: LDAP appears to have a limit to the number of records that 
 * can be server-side sorted. When the limit is reached, two things can happen:
 * a. If sortControl is set to CRITICAL, then the search fails.
 * b. If sortControl is NONCRITICAL, then the search returns, unsorted.
 * We'd like to support ORDER BY, no matter how large the size, so we turn it off,
 * and allow MetaMatrix to do it for us.
 * 2: Supporting ORDER BY appears to negatively effect the query plan
 * when cost analysis is used. We stop using dependent queries, and start
 * using inner joins.
 *
 */

package com.metamatrix.connector.ldap;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.SizeLimitExceededException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.SortControl;
import javax.naming.ldap.SortKey;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.basic.BasicExecution;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;


/** 
 * LDAPSyncQueryExecution is responsible for executing an LDAP search 
 * corresponding to a read-only "select" query from MetaMatrix.
 */
public class LDAPSyncQueryExecution extends BasicExecution implements ResultSetExecution {

	private ConnectorLogger logger;
	private LDAPSearchDetails searchDetails;
	private RuntimeMetadata rm;
	private InitialLdapContext initialLdapContext;
	private LdapContext ldapCtx;
	private NamingEnumeration searchEnumeration;
	private IQueryToLdapSearchParser parser;
	private Properties props;
	private IQuery query;

	/** 
	 * Constructor
	 * @param executionMode the execution mode.
	 * @param ctx the execution context.
	 * @param rm the runtimeMetadata
	 * @param logger the ConnectorLogger
	 * @param ldapCtx the LDAP Context
	 */
	public LDAPSyncQueryExecution(IQuery query, ExecutionContext ctx,
			RuntimeMetadata rm, ConnectorLogger logger,
			InitialLdapContext ldapCtx, Properties props) throws ConnectorException {
		this.rm = rm;
		this.logger = logger;
		this.initialLdapContext = ldapCtx;
		this.props = props;
		this.query = query;
	}

	/** 
	 * method to execute the supplied query
	 * @param query the query object.
	 * @param maxBatchSize the max batch size.
	 */
	@Override
	public void execute() throws ConnectorException {
		// Parse the IQuery, and translate it into an appropriate LDAP search.
		this.parser = new IQueryToLdapSearchParser(logger, this.rm, this.props);
		searchDetails = parser.translateSQLQueryToLDAPSearch(query);

		// Create and configure the new search context.
		createSearchContext();
		SearchControls ctrls = setSearchControls();
		setStandardRequestControls();
		// Execute the search.
		executeSearch(ctrls);
	}

	/** 
	 * Set the standard request controls
	 */
	private void setStandardRequestControls() throws ConnectorException {
		Control[] sortCtrl = new Control[1];
		SortKey[] keys = searchDetails.getSortKeys();
		if (keys != null) {
			try {
				sortCtrl[0] = new SortControl(keys, Control.NONCRITICAL);
				this.ldapCtx.setRequestControls(sortCtrl);
				logger.logTrace("Sort ordering was requested, and sort control was created successfully."); //$NON-NLS-1$
			} catch (NamingException ne) {
	            final String msg = LDAPPlugin.Util.getString("LDAPSyncQueryExecution.setControlsError") +  //$NON-NLS-1$
	            " : "+ne.getExplanation(); //$NON-NLS-1$
				throw new ConnectorException(msg);
			} catch(IOException e) {
	            final String msg = LDAPPlugin.Util.getString("LDAPSyncQueryExecution.setControlsError"); //$NON-NLS-1$
				throw new ConnectorException(e,msg);
			}
		}
	}

	/** 
	 * Perform a lookup against the initial LDAP context, which 
	 * sets the context to something appropriate for the search that is about to occur.
	 * 
	 */
	private void createSearchContext() throws ConnectorException {
		try {
			ldapCtx = (LdapContext) this.initialLdapContext.lookup(searchDetails.getContextName());
		} catch (NamingException ne) {			
			if (searchDetails.getContextName() != null) {
				logger.logError("Attempted to search context: " //$NON-NLS-1$
						+ searchDetails.getContextName());
			}
            final String msg = LDAPPlugin.Util.getString("LDAPSyncQueryExecution.createContextError"); //$NON-NLS-1$
			throw new ConnectorException(msg); 
		}
	}


	/** 
	 * Set the search controls
	 */
	private SearchControls setSearchControls() throws ConnectorException {
		SearchControls ctrls = new SearchControls();
		//ArrayList modelAttrList = searchDetails.getAttributeList();
		ArrayList modelAttrList = searchDetails.getElementList();
		String[] attrs = new String[modelAttrList.size()];
		Iterator itr = modelAttrList.iterator();
		int i = 0;
		while(itr.hasNext()) {
			attrs[i] = (parser.getNameFromElement((Element)itr.next()));
			//attrs[i] = (((Attribute)itr.next()).getID();
			//logger.logTrace("Adding attribute named " + attrs[i] + " to the search list.");
			i++;
		}

		if(attrs == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPSyncQueryExecution.configAttrsError"); //$NON-NLS-1$
			throw new ConnectorException(msg); 
		}
		
		ctrls.setSearchScope(searchDetails.getSearchScope());
		ctrls.setReturningAttributes(attrs);
		
		long limit = searchDetails.getCountLimit();
		if(limit != -1) {
			ctrls.setCountLimit(limit);
		}
		return ctrls;
	}

	/**
	 * Perform the LDAP search against the subcontext, using the filter and 
	 * search controls appropriate to the query and model metadata.
	 */
	private void executeSearch(SearchControls ctrls) throws ConnectorException {
		String ctxName = searchDetails.getContextName();
		String filter = searchDetails.getContextFilter();
		if (ctxName == null || filter == null || ctrls == null) {
			logger.logError("Search context, filter, or controls were null. Cannot execute search."); //$NON-NLS-1$
		}
		try {
			searchEnumeration = this.ldapCtx.search("", filter, ctrls); //$NON-NLS-1$
		} catch (NamingException ne) {
			logger.logError("LDAP search failed. Attempted to search context " //$NON-NLS-1$
					+ ctxName + " using filter " + filter); //$NON-NLS-1$
            final String msg = LDAPPlugin.Util.getString("LDAPSyncQueryExecution.execSearchError"); //$NON-NLS-1$
			throw new ConnectorException(msg + " : " + ne.getExplanation());  //$NON-NLS-1$ 
		} catch(Exception e) {
			logger.logError("LDAP search failed. Attempted to search context " //$NON-NLS-1$
					+ ctxName + " using filter " + filter); //$NON-NLS-1$
            final String msg = LDAPPlugin.Util.getString("LDAPSyncQueryExecution.execSearchError"); //$NON-NLS-1$
			throw new ConnectorException(e, msg); 
		}
	}

	// GHH 20080326 - attempt to implement cancel here.  First try to
	// close the searchEnumeration, then the search context.
	// We are very conservative when closing the enumeration
	// but less so when closing context, since it is safe to call close
	// on contexts multiple times
	@Override
	public void cancel() throws ConnectorException {
		close();
	}

	// GHH 20080326 - replaced existing implementation with the same
	// code as used by cancel method.  First try to
	// close the searchEnumeration, then the search context
	// We are very conservative when closing the enumeration
	// but less so when closing context, since it is safe to call close
	// on contexts multiple times
	@Override
	public void close() throws ConnectorException {
		if (searchEnumeration != null) {
			try {
				searchEnumeration.close();
			} catch (Exception e) { } // catch everything, because NamingEnumeration has undefined behavior if it previously hit an exception
		}
		if (ldapCtx != null) {
			try {
				ldapCtx.close();
			} catch (NamingException ne) {
	            final String msg = LDAPPlugin.Util.getString("LDAPSyncQueryExecution.closeContextError",ne.getExplanation()); //$NON-NLS-1$
				logger.logError(msg);
			}
		}
	}
	
	/**
	 * Fetch the next batch of data from the LDAP searchEnumerationr result.
	 * @return the next Batch of results.
	 */
	// GHH 20080326 - set all batches as last batch after an exception
	// is thrown calling a method on the enumeration.  Per Javadoc for
	// javax.naming.NamingEnumeration, enumeration is invalid after an
	// exception is thrown - by setting last batch indicator we prevent
	// it from being used again.
	// GHH 20080326 - also added return of explanation for generic
	// NamingException
	public List next() throws ConnectorException {
		try {
			// The search has been executed, so process up to one batch of
			// results.
			List result = null;
			while (result == null && searchEnumeration != null && searchEnumeration.hasMore())
			{
				SearchResult searchResult = (SearchResult) searchEnumeration.next();
				result = getRow(searchResult);
			}

			return result;
		} catch (SizeLimitExceededException e) {
			logger.logWarning("Search results exceeded size limit. Results may be incomplete."); //$NON-NLS-1$
			searchEnumeration = null; // GHH 20080326 - NamingEnumartion's are no longer good after an exception so toss it
			return null; // GHH 20080326 - if size limit exceeded don't try to read more results
		} catch (NamingException ne) {
			final String msg = "Ldap error while processing next batch of results: " + ne.getExplanation(); //$NON-NLS-1$
			logger.logError(msg);  // GHH 20080326 - changed to output explanation from LDAP server
			searchEnumeration = null; // GHH 20080326 - NamingEnumertion's are no longer good after an exception so toss it
			throw new ConnectorException(msg);
		}
	}

	/**
	 * Create a row using the searchResult and add it to the supplied batch.
	 * @param batch the supplied batch
	 * @param result the search result
	 */
	// GHH 20080326 - added fetching of DN of result, for directories that
	// do not include it as an attribute
	private List getRow(SearchResult result) throws ConnectorException, NamingException {
		Attributes attrs = result.getAttributes();
		String resultDN = result.getNameInNamespace(); // added GHH 20080326 
		ArrayList attributeList = searchDetails.getElementList();
		List row = new ArrayList();
		
		if (attrs != null && attrs.size()>0) {
			Iterator itr = attributeList.iterator();
			while(itr.hasNext()) {
				addResultToRow((Element)itr.next(), resultDN, attrs, row);  // GHH 20080326 - added resultDN parameter to call
			}
			return row;
		}
		return null;
	}

	/**
	 * Add Result to Row
	 * @param modelElement the model element
	 * @param attrs the attributes
	 * @param row the row
	 */
	// GHH 20080326 - added resultDistinguishedName to method signature.  If
	// there is an element in the model named "DN" and there is no attribute
	// with this name in the search result, we return this new parameter
	// value for that column in the result
	// GHH 20080326 - added handling of ClassCastException when non-string
	// attribute is returned
	private void addResultToRow(Element modelElement, String resultDistinguishedName, Attributes attrs, List row) throws ConnectorException, NamingException {

		String strResult;
		String modelAttrName = parser.getNameFromElement(modelElement);
		Class modelAttrClass = (Class)modelElement.getJavaType();
		if(modelAttrName == null) {
            final String msg = LDAPPlugin.Util.getString("LDAPSyncQueryExecution.nullAttrError"); //$NON-NLS-1$
			throw new ConnectorException(msg); 
		}

		Attribute resultAttr = attrs.get(modelAttrName);
		
		// If the attribute is not present, we return NULL.
		if(resultAttr == null) {
			// MPW - 2-20-07 - Changed from returning empty string to returning null.
			//row.add("");
			//logger.logTrace("Did not find a match for attribute named: " + modelAttrName);
			// GHH 20080326 - return DN from input parameter
			// if DN attribute is not present in search result
			if (modelAttrName.toUpperCase().equals("DN")) {  //$NON-NLS-1$
				row.add(resultDistinguishedName);
			}
			else {
				row.add(null);
			}
			return;
		} 
		// TODO: Currently, if an LDAP entry contains more than one matching
		// attribute, we only return the first. 
		// Since attribute order is not guaranteed, this means that we may not
		// always return the exact same information.
		// Putting multi-valued attributes into a single row (or multiple rows) requires
		// some design decisions.
		// GHH 20080326 - first get attribute as generic object
		// so we can check to make sure it is a string separately - previously it was just put straight into a string.
		Object objResult = null;
		try {
			objResult = resultAttr.get();
		} catch (NamingException ne) {
            final String msg = LDAPPlugin.Util.getString("LDAPSyncQueryExecution.attrValueFetchError",modelAttrName); //$NON-NLS-1$
			logger.logWarning(msg+" : "+ne.getExplanation()); //$NON-NLS-1$
			throw new ConnectorException(msg+" : "+ne.getExplanation()); //$NON-NLS-1$
		}

		// GHH 20080326 - if attribute is not a string, just
		// return an empty string.
		// TODO - allow return of non-strings (always byte[]) as
		// MM object.  Perhaps also add directory-specific logic
		// to deserialize byte[] attributes into Java objects
		// when appropriate
		try {
			strResult = (String)objResult;
		} catch (ClassCastException cce) {
			strResult = ""; //$NON-NLS-1$
		}

		// MPW - 3.9.07 - Also return NULL when attribute is unset or empty string.
		// There is no way to differentiate between being unset and being the empty string.
		if(strResult.equals("")) {  //$NON-NLS-1$
			strResult = null;
		}

		// MPW: 3-11-07: Added support for java.lang.Integer conversion.
		try {
			if(modelAttrClass.equals(Class.forName(Integer.class.getName()))) {
				try {
					//	Throw an exception if class cast fails.
					if(strResult != null) {
						Integer intResult = new Integer(strResult);
						row.add(intResult);
					} else {
						row.add(null);
					}
				} catch(NumberFormatException nfe) {
					throw new ConnectorException(nfe, "Element " + modelAttrName + " is typed as Integer, " + //$NON-NLS-1$ //$NON-NLS-2$
							"but it's value (" + strResult + ") cannot be converted from string " + //$NON-NLS-1$ //$NON-NLS-2$
							"to Integer. Please change type to String, or modify the data."); //$NON-NLS-1$
				}
			// java.lang.String
			} else if(modelAttrClass.equals(Class.forName(String.class.getName()))) {
				row.add(strResult);
			// java.sql.Timestamp
			} else if(modelAttrClass.equals(Class.forName(java.sql.Timestamp.class.getName()))) {
				Properties p = modelElement.getProperties();

				String timestampFormat = p.getProperty("Format"); //$NON-NLS-1$
				SimpleDateFormat dateFormat;
				if(timestampFormat == null) {
					timestampFormat = LDAPConnectorConstants.ldapTimestampFormat;
					
				}
				dateFormat = new SimpleDateFormat(timestampFormat);
				try {
					if(strResult != null) {
						Date dateResult = dateFormat.parse(strResult);
						Timestamp tsResult = new Timestamp(dateResult.getTime());
						row.add(tsResult);
					} else {
						row.add(null);
					}
				} catch(ParseException pe) {
					throw new ConnectorException(pe, "Timestamp could not be parsed. Please check to ensure the "  //$NON-NLS-1$
							+ " Format field for attribute "  //$NON-NLS-1$
							+ modelAttrName + " is configured using SimpleDateFormat conventions."); //$NON-NLS-1$
				}		
				
			//	TODO: Extend support for more types in the future.
			// Specifically, add support for byte arrays, since that's actually supported
			// in the underlying data source.
			} else {
				throw new ConnectorException("Base type " + modelAttrClass.toString()  //$NON-NLS-1$
						+ " is not supported in the LDAP connector. "  //$NON-NLS-1$
						+ " Please modify the base model to use a supported type."); //$NON-NLS-1$
			}
		} catch(ClassNotFoundException cne) {
            final String msg = LDAPPlugin.Util.getString("LDAPSyncQueryExecution.supportedClassNotFoundError"); //$NON-NLS-1$
			throw new ConnectorException(cne, msg); 
		}
	}
	

	/**
	 * Active Directory and OpenLDAP supports PagedResultsControls, so I left
	 * this method in here in case we decide to extend support for this control
	 * in the future.
	 */
//	private void setADRequestControls(int maxBatchSize) {
//		try {
//			ldapCtx.setRequestControls(new Control[] { new PagedResultsControl(
//					maxBatchSize, Control.CRITICAL) });
//		} catch (NamingException ne) {
//			logger.logError("Failed to set page size for LDAP results. Please ensure that paged results controls are supported by the LDAP server implementation."); //$NON-NLS-1$
//			ne.printStackTrace();
//		} catch (IOException ioe) {
//			logger.logError("IO Exception while setting paged results control."); //$NON-NLS-1$
//			ioe.printStackTrace();
//		}
//	}
}
