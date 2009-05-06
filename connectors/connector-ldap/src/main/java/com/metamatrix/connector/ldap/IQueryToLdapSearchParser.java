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
 * Utility class to handle the parsing of an IQuery object into all the relevant LDAP search
 * information. Uses LdapSearchDetails class to store this information and return the search details.
 * This class was intended for use by the execution classes that need to translate SQL. As new capabilities
 * are implemented, this class will be expanded to accommodate the appropriate SQL.
 * 
 * This class should remove all the MMX-specific stuff, and turn it into something any
 * LDAP implementation can understand.
 *
 */

package com.metamatrix.connector.ldap;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;

import javax.naming.NamingException;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.SearchControls;
import javax.naming.ldap.SortKey;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.language.IAggregate;
import org.teiid.connector.language.ICompareCriteria;
import org.teiid.connector.language.ICompoundCriteria;
import org.teiid.connector.language.ICriteria;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IExistsCriteria;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFromItem;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.language.IInCriteria;
import org.teiid.connector.language.ILikeCriteria;
import org.teiid.connector.language.ILimit;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.language.INotCriteria;
import org.teiid.connector.language.IOrderBy;
import org.teiid.connector.language.IOrderByItem;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.IScalarSubquery;
import org.teiid.connector.language.ISearchedCaseExpression;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.language.ICompareCriteria.Operator;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.Group;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;


/**
 * Utility class which translates a SQL query into an LDAP search.
 */
public class IQueryToLdapSearchParser {
	private ConnectorLogger logger; 
	private RuntimeMetadata rm;
	private Properties props;
	
	/**
	 * Constructor.
	 * @param logger the connector logger
	 * @param rm the RuntimeMetadata
	 */
	public IQueryToLdapSearchParser(ConnectorLogger logger, RuntimeMetadata rm, Properties props) {
		this.logger = logger;
		this.rm = rm;
		this.props = props;
	}
	
	/** 
	 * Public entry point to the parser.
	 *  Parses the IQuery object, and constructs an equivalent LDAP search filter,
	 *  keeping track of the attributes of interest.
	 *  Here are some example SQL queries, and the equivalent LDAP search info:
	 *  SQL: select cn, managerName from people_table where managerName LIKE "John%" and cn!="Mar()"
	 *  Context name: [people_table's NameInSource, e.g. (ou=people,dc=company,dc=com)]
	 *  LDAP attributes: (cn, String), (managerName, String)
	 *  LDAP search filter: (&(managerName="John*")(!(cn="Mar\(\)")))
	 *  
	 *  @param query the query
	 *  @return the LDAPSearchDetails object
	 */
	// GHH 20080326 - added ability to restrict queries to only values where
	// objectClass = table name.  This is done by adding a third parameter,
	// RESTRICT, to the NameInSource property in the model:
	// ou=people,dc=company,dc=com?SUBTREE_SCOPE?RESTRICT
	// TODO - change method for calling RESTRICT to also specify
	// object class name (RESTRICT=inetOrgPerson)
	public LDAPSearchDetails translateSQLQueryToLDAPSearch(IQuery query) throws ConnectorException {
			// Parse SELECT symbols.
			// The columns will be translated into LDAP attributes of interest.
			ArrayList attributeList = getAttributesFromSelectSymbols(query);
			ArrayList elementList = getElementsFromSelectSymbols(query);
			
			// Parse FROM table.
			// Only one table is expected here.
			List fromList = query.getFrom().getItems();
			Iterator itr = fromList.listIterator();
			if(!itr.hasNext()) {
	            final String msg = LDAPPlugin.Util.getString("IQueryToLdapSearchParser.noTablesInFromError"); //$NON-NLS-1$
				logger.logError(msg);  
				throw new ConnectorException(msg); 
			}
			IFromItem fItm = (IFromItem)itr.next();
			if(itr.hasNext()) {
	            final String msg = LDAPPlugin.Util.getString("IQueryToLdapSearchParser.multiItemsInFromError"); //$NON-NLS-1$
				logger.logError(msg);
				throw new ConnectorException(msg); 
			}
			String contextName = getContextNameFromFromItem(fItm);
			int searchScope = getSearchScopeFromFromItem(fItm);
			// GHH 20080326 - added check for RESTRICT parameter in
			// NameInSource of from item
			String classRestriction = getRestrictToNamedClass(fItm);
					
			// Parse the WHERE clause.
			// Create an equivalent LDAP search filter.
			List searchStringList = new LinkedList();
			searchStringList = getSearchFilterFromWhereClause(query.getWhere(), searchStringList);
			String filter = new String();
			ListIterator filterItr = searchStringList.listIterator();
			while(filterItr.hasNext()) {
				filter += filterItr.next();
			}
			// GHH 20080326 - if there is a class restriction,
			// add it to the search filter
			if (classRestriction != null && classRestriction.trim().length()>0) {
				filter = "(&"+filter+"(objectClass="+classRestriction+"))";  //$NON-NLS-1$  //$NON-NLS-2$  //$NON-NLS-3$
			}
			
			// Parse the ORDER BY clause.
			// Create an ordered sort list.
			IOrderBy orderBy = (IOrderBy)query.getOrderBy();
			// Referenced the JNDI standard...arguably, this should not be done inside this
			// class, and we should make our own key class. In practice, this makes things simpler.
			SortKey[] sortKeys = getSortKeysFromOrderByClause(orderBy);
			
			// Parse LIMIT clause. 
			// Note that offsets are not supported.
			ILimit limit = (ILimit)query.getLimit();
			long countLimit = -1;
			if(limit != null) {
				countLimit = limit.getRowLimit();
			}
			
			// Create Search Details
			LDAPSearchDetails sd = new LDAPSearchDetails(contextName, searchScope, filter, attributeList, sortKeys, countLimit, elementList);
			// Search Details logging
			try {
				sd.printDetailsToLog(logger);
			} catch (NamingException nme) {
				final String msg = LDAPPlugin.Util.getString("IQueryToLdapSearchParser.searchDetailsLoggingError");  //$NON-NLS-1$
				throw new ConnectorException(msg);
			}
			
			return sd;
			
	}
	
	/**
	 * get SortKeys from the supplied ORDERBY clause.
	 * @param orderBy the OrderBy clause
	 * @param the array of SortKeys
	 */
	private SortKey[] getSortKeysFromOrderByClause(IOrderBy orderBy) throws ConnectorException {
		SortKey[] sortKeys = null;
		if(orderBy != null) {
			List orderItems = orderBy.getItems();
			if(orderItems == null) {
				return null;
			}
			SortKey sortKey = null;
			sortKeys = new SortKey[orderItems.size()];
			Iterator orderItr = orderItems.iterator();
			int i = 0;
			while(orderItr.hasNext()) {
				IOrderByItem item = (IOrderByItem)orderItr.next();
				if(item != null) {
					String itemName = getExpressionString((IExpression)item.getElement());
					logger.logTrace("Adding sort key for item: " + itemName); //$NON-NLS-1$
					if(item.getDirection() == IOrderByItem.ASC) {
						logger.logTrace("with ASC ordering."); //$NON-NLS-1$
						sortKey = new SortKey(itemName, true, null);
					} else if(item.getDirection() == IOrderByItem.DESC){
						logger.logTrace("with DESC ordering."); //$NON-NLS-1$
						sortKey = new SortKey(itemName, false, null);
					}
				}
				sortKeys[i] = sortKey;
				i++;
			}
			
		} else {
			// Insert a default? No, allow the Execution to do this. Just return a null list.
		}
		return sortKeys;
	}
	
	/** 
	 * Utility method to pull the name in source (or, base DN/context name) from the table.
	 * @param fromItem
	 * @return name in source
	 */
	// GHH 20080409 - changed to fall back on new connector property
	// for default base DN if available
	private String getContextNameFromFromItem(IFromItem fromItem) throws ConnectorException {
		String nameInSource;
		String contextName;

		// TODO: Re-use the getExpressionString method if possible, rather than 
		// rewriting the same code twice.
		if(fromItem instanceof IGroup) {
			Group group = ((IGroup)fromItem).getMetadataObject();
			nameInSource = group.getNameInSource();
			// if NameInSource is null set it to an empty
			// string instead so we can safely call split on it
			if(nameInSource == null) {
				nameInSource = "";  //$NON-NLS-1$
			}
			// now split it on ? to find the part of it that specifies context name
			String nameInSourceArray[] = nameInSource.split("\\?");  //$NON-NLS-1$
			contextName = nameInSourceArray[0];
			// if there is no context name specified
			// try the default in the connector properties
			if(contextName.equals("")) {  //$NON-NLS-1$
				contextName = props.getProperty(LDAPConnectorPropertyNames.LDAP_DEFAULT_BASEDN);	
			}
		} else {
            final String msg = LDAPPlugin.Util.getString("IQueryToLdapSearchParser.groupCountExceededError"); //$NON-NLS-1$
			throw new ConnectorException(msg);
		}
		// if the context name is not specified either in Name in Source
		// or in the default connector properties it'll be either
		// null or an empty string
		if(contextName == null || contextName.equals("")) { //$NON-NLS-1$
            final String msg = LDAPPlugin.Util.getString("IQueryToLdapSearchParser.baseContextNameError"); //$NON-NLS-1$
			throw new ConnectorException(msg);
		}
		return contextName;

	}

	// GHH 20080326 - added below method to check for RESTRICT parameter in
	// from item's NameInSource, and if true return name (not NameInSource)
	// of from item as the objectClass name on to which the query should
	// be restricted
	private String getRestrictToNamedClass(IFromItem fromItem) throws ConnectorException {
		String nameInSource;
		String namedClass = null;
		if(fromItem instanceof IGroup) {
			// Here we use slightly different logic than in
			// getContextNameFromFromItem so it is easier to get
			// the group name later if needed
			Group mdIDGroup = ((IGroup)fromItem).getMetadataObject();
			nameInSource = mdIDGroup.getNameInSource();
			// groupName = mdIDGroup.getName();
			// if NameInSource is null set it to an empty
			// string instead so we can safely call split on it
			if(nameInSource == null) {
				nameInSource = "";  //$NON-NLS-1$
			}
			// now split it on ? to find the part of it that specifies the objectClass we should restrict on
			String nameInSourceArray[] = nameInSource.split("\\?");  //$NON-NLS-1$
			if(nameInSourceArray.length >= 3) {
				namedClass = nameInSourceArray[2];
			}
			// if there is no specification in the Name In Source,
			// see if the connector property is set to true.  If
			// it is, use the Name of the class for the restriction.
			if(namedClass == null || namedClass.equals("")) {  //$NON-NLS-1$
				String restrictToObjectClass =  props.getProperty(LDAPConnectorPropertyNames.LDAP_RESTRICT_TO_OBJECTCLASS);	
				if (restrictToObjectClass == null || restrictToObjectClass.toUpperCase().equals("FALSE")) {  //$NON-NLS-1$
					namedClass = "";  //$NON-NLS-1$
				} else {
					namedClass = mdIDGroup.getName();
				}
			}
		} else {
            final String msg = LDAPPlugin.Util.getString("IQueryToLdapSearchParser.groupCountExceededError"); //$NON-NLS-1$
			throw new ConnectorException(msg);
		}
		return namedClass;
	}

	private int getSearchScopeFromFromItem(IFromItem fromItem) throws ConnectorException {
		String searchScopeString = "";  //$NON-NLS-1$
		int searchScope = LDAPConnectorConstants.ldapDefaultSearchScope;
		// TODO: Re-use the getExpressionString method if possible, rather than 
		// rewriting the same code twice.
		if(fromItem instanceof IGroup) {
			Group group = ((IGroup)fromItem).getMetadataObject();
			String nameInSource = group.getNameInSource();
			// if NameInSource is null set it to an empty
			// string instead so we can safely call split on it
			if(nameInSource == null) {
				nameInSource = "";  //$NON-NLS-1$
			}
			// now split it on ? to find the part of it that specifies search scope
			String nameInSourceArray[] = nameInSource.split("\\?");  //$NON-NLS-1$
			if(nameInSourceArray.length >= 2) {
				searchScopeString = nameInSourceArray[1];
			}
			// if there is no search scope specified
			// try the default in the connector properties
			if(searchScopeString.equals("")) {  //$NON-NLS-1$
				searchScopeString = props.getProperty(LDAPConnectorPropertyNames.LDAP_DEFAULT_SCOPE);	
				// protect against getting null back from the property
				if(searchScopeString == null) {
					searchScopeString = "";  //$NON-NLS-1$
				}
			}
			if(searchScopeString.equals("SUBTREE_SCOPE")) {  //$NON-NLS-1$
				searchScope = SearchControls.SUBTREE_SCOPE;
			} else if(searchScopeString.equals("ONELEVEL_SCOPE")) {  //$NON-NLS-1$
				searchScope = SearchControls.ONELEVEL_SCOPE;
			} else if(searchScopeString.equals("OBJECT_SCOPE")) {  //$NON-NLS-1$
				searchScope = SearchControls.OBJECT_SCOPE;
			}
		} else {
            final String msg = LDAPPlugin.Util.getString("IQueryToLdapSearchParser.groupCountExceededError"); //$NON-NLS-1$
			throw new ConnectorException(msg);
		}
		return searchScope;
	}
	
	/** 
	 * Utility method to convert operator to the appropriate string value for LDAP.
	 * @param op operator to evaluate
	 * @return LDAP-specific string equivalent of the operator
	 */
	private String parseCompoundCriteriaOp(ICompoundCriteria.Operator op) throws ConnectorException {
		switch(op) {
			case AND:
				return "&";	 //$NON-NLS-1$
			case OR:
				return "|"; //$NON-NLS-1$
			default:
	            final String msg = LDAPPlugin.Util.getString("IQueryToLdapSearchParser.criteriaNotParsableError"); //$NON-NLS-1$
				throw new ConnectorException(msg); 
		}
	}

	/** 
	 * Utility method to convert expression to the appropriate string value for LDAP.
	 * @param e expression to evaluate
	 * @return LDAP-specific string equivalent of the expression
	 */
	// GHH 20080326 - found that code to fall back on Name if NameInSource
	// was null wasn't working properly, so replaced with tried and true
	// code from another custom connector.
	private String getExpressionString(IExpression e) throws ConnectorException {
		String expressionName = null;
		// GHH 20080326 - changed around the IElement handling here
		// - the rest of this method is unchanged
		if(e instanceof IElement) {
			Element mdIDElement = ((IElement)e).getMetadataObject();
			expressionName = mdIDElement.getNameInSource();
			if(expressionName == null || expressionName.equals("")) {  //$NON-NLS-1$
				expressionName = mdIDElement.getName();
			}
		} else if(e instanceof ILiteral) {
			try {
				if(((ILiteral)e).getType().equals(Class.forName(Timestamp.class.getName()))) {
					logger.logTrace("Found an expression that uses timestamp; converting to LDAP string format."); //$NON-NLS-1$
					Timestamp ts = (Timestamp)((ILiteral)e).getValue();
					Date dt = new Date(ts.getTime());
					//TODO: Fetch format if provided.
					SimpleDateFormat sdf = new SimpleDateFormat(LDAPConnectorConstants.ldapTimestampFormat);
					expressionName = sdf.format(dt);
					logger.logTrace("Timestamp to stsring is: " + expressionName); //$NON-NLS-1$
				}
				else {
					expressionName = ((ILiteral)e).getValue().toString();
				}
			} catch (ClassNotFoundException cce) {
	            final String msg = LDAPPlugin.Util.getString("IQueryToLdapSearchParser.timestampClassNotFoundError"); //$NON-NLS-1$
				throw new ConnectorException(cce, msg); 
			}
				
		} else {
			if(e instanceof IAggregate) {
				logger.logError("Received IAggregate, but it is not supported. Check capabilities."); //$NON-NLS-1$
			} else if(e instanceof IFunction) {
				logger.logError("Received IFunction, but it is not supported. Check capabilties."); //$NON-NLS-1$
			} else if(e instanceof IScalarSubquery) {
				logger.logError("Received IScalarSubquery, but it is not supported. Check capabilties."); //$NON-NLS-1$
			} else if (e instanceof ISearchedCaseExpression) {
				logger.logError("Received ISearchedCaseExpression, but it is not supported. Check capabilties."); //$NON-NLS-1$
			}
            final String msg = LDAPPlugin.Util.getString("IQueryToLdapSearchParser.unsupportedElementError"); //$NON-NLS-1$
			throw new ConnectorException(msg + e.toString()); 
		}
		expressionName = escapeReservedChars(expressionName);
		return expressionName;
	}
	
	private String escapeReservedChars(String expr) {
		StringBuffer sb = new StringBuffer();
        for (int i = 0; i < expr.length(); i++) {
            char curChar = expr.charAt(i);
            switch (curChar) {
                case '\\':
                    sb.append("\\5c"); //$NON-NLS-1$
                    break;
                case '*':
                    sb.append("\\2a"); //$NON-NLS-1$
                    break;
                case '(':
                    sb.append("\\28"); //$NON-NLS-1$
                    break;
                case ')':
                    sb.append("\\29"); //$NON-NLS-1$
                    break;
                case '\u0000': 
                    sb.append("\\00"); //$NON-NLS-1$
                    break;
                default:
                    sb.append(curChar);
            }
        }
        return sb.toString();
	}

	/** 
	 * Recursive method to translate the where clause into an LDAP search filter.
	 * The goal is to convert infix notation to prefix (Polish) notation.
	 * TODO: There's probably a clever way to do this with a Visitor.
	 * @param criteria Criteria to evaluate.
	 * @param List list to hold each pre-fix character of the search filter. 
	 * @return list list that can be traversed in order to construct the search filter.
	 */
	private List getSearchFilterFromWhereClause(ICriteria criteria, List filterList) throws ConnectorException {
		if(criteria == null) {
			filterList.add("(objectClass=*)"); //$NON-NLS-1$
		}
		boolean isNegated = false;
		// Recursive case: compound criteria
		if(criteria instanceof ICompoundCriteria) {
			logger.logTrace("Parsing compound criteria."); //$NON-NLS-1$
			ICompoundCriteria.Operator op = ((ICompoundCriteria) criteria).getOperator();
			List criteriaList = ((ICompoundCriteria) criteria).getCriteria();
			String stringOp = parseCompoundCriteriaOp(op);
			
			filterList.add("("); //$NON-NLS-1$
			filterList.add(stringOp);
			Iterator itr = criteriaList.iterator();
			while(itr.hasNext()) {
				ICriteria c = (ICriteria)itr.next();
				// recurse on each criterion
				filterList = getSearchFilterFromWhereClause(c, filterList);
			}
			filterList.add(")"); //$NON-NLS-1$
			
		// Base case
		} else if(criteria instanceof ICompareCriteria) {
			logger.logTrace("Parsing compare criteria."); //$NON-NLS-1$
			ICompareCriteria.Operator op = ((ICompareCriteria) criteria).getOperator();
			
			isNegated = op == Operator.NE || op == Operator.GT || op == Operator.LT;
			
			IExpression lhs = ((ICompareCriteria) criteria).getLeftExpression();
			IExpression rhs = ((ICompareCriteria) criteria).getRightExpression();
		
			String lhsString = getExpressionString(lhs);
			String rhsString = getExpressionString(rhs);
			if(lhsString == null || rhsString == null) {
	            final String msg = LDAPPlugin.Util.getString("IQueryToLdapSearchParser.missingNISError"); //$NON-NLS-1$
				throw new ConnectorException(msg); 
			}
			
			addCompareCriteriaToList(filterList, op, lhsString, rhsString);
		// Base case
		} else if(criteria instanceof IExistsCriteria) {
			logger.logTrace("Parsing EXISTS criteria: NOT IMPLEMENTED YET"); //$NON-NLS-1$
			// TODO Exists should be supported in a future release.
		// Base case
		} else if(criteria instanceof ILikeCriteria) {
			logger.logTrace("Parsing LIKE criteria."); //$NON-NLS-1$
			isNegated = ((ILikeCriteria) criteria).isNegated();
			// Convert LIKE to Equals, where any "%" symbol is replaced with "*".
			ICompareCriteria.Operator op = Operator.EQ;
			IExpression lhs = ((ILikeCriteria) criteria).getLeftExpression();
			IExpression rhs = ((ILikeCriteria) criteria).getRightExpression();
		
			String lhsString = getExpressionString(lhs);
			String rhsString = getExpressionString(rhs);
			rhsString = rhsString.replace("%", "*"); //$NON-NLS-1$ //$NON-NLS-2$
			addCompareCriteriaToList(filterList, op, lhsString, rhsString);
			
		// Base case
		} else if(criteria instanceof IInCriteria) {
			logger.logTrace("Parsing IN criteria."); //$NON-NLS-1$
			isNegated = ((IInCriteria) criteria).isNegated();
			IExpression lhs = ((IInCriteria)criteria).getLeftExpression();
			List rhsList = ((IInCriteria)criteria).getRightExpressions();
			// Recursively add each IN expression to the filter list.
			processInCriteriaList(filterList, rhsList, lhs);
		} else if (criteria instanceof INotCriteria) {
			logger.logTrace("Parsing NOT criteria."); //$NON-NLS-1$
			isNegated = true;
			filterList = getSearchFilterFromWhereClause(((INotCriteria)criteria).getCriteria(), filterList);
		}
		
		if (isNegated) {
			filterList.add(0, "("); //$NON-NLS-1$
			filterList.add(1, "!"); //$NON-NLS-1$
			filterList.add(")"); //$NON-NLS-1$
		}
		
		return filterList;
	}
	
	/** 
	 * Process a list of right-hand side IN expresssions and add the corresponding LDAP filter
	 * clause string to the given filterList.
	 */
	private void processInCriteriaList(List filterList, List rhsList, IExpression lhs) throws ConnectorException {
		if(rhsList.size() == 0) {
			return;
		}
		filterList.add("("); //$NON-NLS-1$
		filterList.add(parseCompoundCriteriaOp(org.teiid.connector.language.ICompoundCriteria.Operator.OR));
		Iterator rhsItr = rhsList.iterator();
		while(rhsItr.hasNext()) {
			addCompareCriteriaToList(filterList, Operator.EQ, getExpressionString(lhs), 
					getExpressionString((IExpression)rhsItr.next()));
		}
		filterList.add(")"); //$NON-NLS-1$
	}
	
	/** 
	 * Add Compare Criteria to List
	 * @param filterList the filter list
	 * @param op
	 * @param lhs left hand side expression
	 * @param rhs right hand side expression
	 */
	private void addCompareCriteriaToList(List filterList, ICompareCriteria.Operator op, String lhs, String rhs) throws ConnectorException {
		// Push the comparison statement into the list, e.g.:
		// (sn=Mike)
		// !(empNum>=100)
		filterList.add("("); //$NON-NLS-1$
		filterList.add(lhs);

		switch(op) {
		    case NE:
			case EQ:
				filterList.add("="); //$NON-NLS-1$
				break;
			case LT:
			case GE:
				filterList.add(">="); //$NON-NLS-1$
				break;
			case GT:
			case LE:
				filterList.add("<="); //$NON-NLS-1$
				break;
			default:
	            final String msg = LDAPPlugin.Util.getString("IQueryToLdapSearchParser.criteriaNotSupportedError"); //$NON-NLS-1$
				throw new ConnectorException(msg); 
				
		}
		filterList.add(rhs);
		filterList.add(")"); //$NON-NLS-1$
	}
	
	/** 
	 * Method to get name from the supplied Element
	 * @param e the supplied Element
	 * @return the name
	 */
    // GHH 20080326 - found that code to fall back on Name if NameInSource
	// was null wasn't working properly, so replaced with tried and true
	// code from another custom connector.
	public String getNameFromElement(Element e) throws ConnectorException {
		String ldapAttributeName = null;
		ldapAttributeName = e.getNameInSource();
		if (ldapAttributeName == null || ldapAttributeName.equals("")) { //$NON-NLS-1$
			ldapAttributeName = e.getName();
			// If name in source is not set, then fall back to the column name.
		}
		return ldapAttributeName;
	}
	
	/** 
	 * Method to get SELECT Element list from the supplied query
	 * @param query the supplied Query
	 * @return the list of SELECT elements
	 */
	private ArrayList getElementsFromSelectSymbols(IQuery query) throws ConnectorException {
		ArrayList selectElementList = new ArrayList();
		Iterator selectSymbolItr = query.getSelect().getSelectSymbols().iterator();

		while(selectSymbolItr.hasNext()) {
			Element e = getElementFromSymbol((ISelectSymbol)selectSymbolItr.next());
			selectElementList.add(e);
		}
		return selectElementList;
	}
	
	/** 
	 * Method to get attribute list from the supplied query
	 * @param query the supplied Query
	 * @return the list of attributes
	 */
	private ArrayList getAttributesFromSelectSymbols(IQuery query) throws ConnectorException {
		ArrayList ldapAttributeList = new ArrayList();
			
		Iterator selectSymbolItr = query.getSelect().getSelectSymbols().iterator();
		int i=0;
		while(selectSymbolItr.hasNext()) {
			Element e = getElementFromSymbol((ISelectSymbol)selectSymbolItr.next());
			String ldapAttributeName = this.getNameFromElement(e);
			Object ldapAttributeClass = e.getJavaType();

			// Store the element's name and class type, so that we know what to look for in the search results.
			BasicAttribute newAttr = new BasicAttribute(ldapAttributeName, ldapAttributeClass);
			ldapAttributeList.add(newAttr);
			i++;
		}
		return ldapAttributeList;
	}
	
    /**
     * Helper method for getting runtime {@link org.teiid.connector.metadata.runtime.Element} from a
     * {@link org.teiid.connector.language.ISelectSymbol}.
     * @param symbol Input ISelectSymbol
     * @return Element returned metadata runtime Element
     */
    private Element getElementFromSymbol(ISelectSymbol symbol) throws ConnectorException {
        IElement expr = (IElement) symbol.getExpression();
        return expr.getMetadataObject();
    }
	
	
}
