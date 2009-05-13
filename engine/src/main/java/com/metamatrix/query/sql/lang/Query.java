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

package com.metamatrix.query.sql.lang;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SelectSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
/**
 * A representation of a data query.  A query consists of various parts,
 * referred to as clauses.  The following list the types of clauses
 * that a query can hold, and their purpose: <p>
 * <pre>
 *      CLAUSE          PURPOSE
 *      =========       ==============================================
 *      Select          Defines the variables data to be retrieved for
 *		From			Defines the groups to retrieve data from
 *      Criteria        Defines constraints on data retrieval ("where")
 * 		GroupBy			Defines how rows being returned should be grouped
 * 		Having			Defines how groups should be filtered, also a criteria
 *      OrderBy         Defines how the results should be sorted
 *	 	Option			Defines any extra options on the query
 * </pre>
 */
public class Query extends QueryCommand {

    /** The select clause. */
    private Select select;

	/** The from clause. */
	private From from;
	
    /** The criteria specifying constraints on what data will be retrieved. */
    private Criteria criteria;

	/** The group by specifying how to group rows. */
	private GroupBy groupBy;
	
	/** The having specifying which group rows will be returned. */
	private Criteria having;

	/** XML flag */
	private boolean isXML;
    
    /** The into clause. */
    private Into into;
    
    /** xml projected symbols */
    private List selectList;
	
    // =========================================================================
    //                         C O N S T R U C T O R S
    // =========================================================================
    
    /**
     * Constructs a default instance of this class.
     */
    public Query() {
        super();
    }

    /**
     * Constructs an instance of this class given the specified clauses
     * @param select SELECT clause
     * @param from FROM clause
     * @param criteria WHERE clause
     * @param orderBy ORDER BY clause
     * @param option OPTION clause
     */
    public Query( Select select, From from, Criteria criteria, OrderBy orderBy, Option option ) {
        super();
        setSelect( select );
        setFrom( from );
        setCriteria( criteria );
        setOrderBy( orderBy );
        setOption( option );
    }

    /**
     * Constructs an instance of this class given all the clauses
     * @param select SELECT clause
     * @param from FROM clause
     * @param criteria WHERE clause
     * @param groupBy GROUP BY clause
     * @param having HAVING clause
     * @param orderBy ORDER BY clause
     * @param option OPTION clause
     */
    public Query( Select select, From from, Criteria criteria, GroupBy groupBy, Criteria having, OrderBy orderBy, Option option ) {
        super();
        setSelect( select );
        setFrom( from );
        setCriteria( criteria );
		setGroupBy( groupBy );
		setHaving( having );
        setOrderBy( orderBy );
        setOption( option );
    }

   	/**
	 * Return type of command.
	 * @return TYPE_QUERY
	 */
	public int getType() {
		return Command.TYPE_QUERY;
	}
    
    // =========================================================================
    //                     S E L E C T      M E T H O D S
    // =========================================================================
    
    /**
     * Get the select clause for the query.
     * @return SELECT clause
     */
    public Select getSelect() {
        return select;
    }
    
    /**
     * Set the select clause for the query.
     * @param select SELECT clause
     */
    public void setSelect( Select select ) {
        this.select = select;
    }
     
 
	/**
	 * Get the xml flag for the query
	 * @return boolean
	 */
	public boolean getIsXML() {
		return isXML;
	}
    
	/**
	 * Get the xml flag for the query
	 * @return boolean
	 */
	public void setIsXML(boolean isXML) {
		this.isXML = isXML;
	}
      
    // =========================================================================
    //                     F R O M      M E T H O D S
    // =========================================================================
    
    /**
     * Get the from clause for the query.
     * @return FROM clause
     */
    public From getFrom() {
        return from;
    }
    
    /**
     * Set the from clause for the query.
     * @param from FROM clause
     */
    public void setFrom( From from  ) {
        this.from = from;
    }
        
    // =========================================================================
    //                   C R I T E R I A      M E T H O D S
    // =========================================================================
    
    /**
     * Get the criteria clause for the query.
     * @return WHERE clause
     */
    public Criteria getCriteria() {
        return criteria;
    }
    
    /**
     * Set the criteria clause for the query.
     * @param criteria WHERE clause
     */
    public void setCriteria( Criteria criteria ) {
        this.criteria = criteria;
    }

    /**
     * Set the criteria clause to null
     */
    public void clearCriteria() {
        this.criteria = null;
    }

    // =========================================================================
    //                    G R O U P   B Y     M E T H O D S
    // =========================================================================
    
    /**
     * Get the group by clause for the query.
     * @return GROUP BY clause
     */
    public GroupBy getGroupBy() {
        return groupBy;
    }
    
    /**
     * Set the group by clause for the query.
     * @param groupBy GROUP BY clause
     */
    public void setGroupBy( GroupBy groupBy ) {
        this.groupBy = groupBy;
    }


    // =========================================================================
    //                   H A V I N G      M E T H O D S
    // =========================================================================
    
    /**
     * Get the having clause for the query.
     * @return HAVING clause
     */
    public Criteria getHaving() {
        return having;
    }
    
    /**
     * Set the criteria clause for the query.
     * @param having HAVING clause
     */
    public void setHaving( Criteria having ) {
        this.having = having;
    }

    // =========================================================================
    //                   I N T O      M E T H O D S
    // =========================================================================

    /**
     * @return
     */
    public Into getInto() {
        return into;
    }

    /**
     * @param into
     */
    public void setInto(Into into) {
        this.into = into;
    }
    
    // =========================================================================
    //                  P R O C E S S I N G     M E T H O D S
    // =========================================================================
        
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

   	/**
	 * Get the ordered list of all elements returned by this query.  These elements
	 * may be ElementSymbols or ExpressionSymbols but in all cases each represents a 
	 * single column.
	 * @return Ordered list of SingleElementSymbol
	 */
	public List getProjectedSymbols() {
		if (!getIsXML()) {
			if(getSelect() != null) { 
                if(getInto() != null){
                    //SELECT INTO clause
                    return Command.getUpdateCommandSymbol();
                }
				return getSelect().getProjectedSymbols();
			}
			return Collections.EMPTY_LIST;
		}
		if(selectList == null){
			selectList = new ArrayList(1);
			ElementSymbol xmlElement = new ElementSymbol("xml"); //$NON-NLS-1$
	        xmlElement.setType(DataTypeManager.DefaultDataClasses.XML);
			selectList.add(xmlElement);
		}
		return selectList;
	}
	
    // =========================================================================
    //          O V E R R I D D E N     O B J E C T     M E T H O D S
    // =========================================================================

	/**
	 * Deep clone Query to produce a new identical query.
	 * @return Deep clone
	 */
	public Object clone() {		
		Query copy = new Query();

		if(select != null) {
			copy.setSelect( (Select) select.clone());
		}

		if(from != null) {
			copy.setFrom( (From) from.clone());
		}

		if(criteria != null) {
			copy.setCriteria( (Criteria) criteria.clone());
		}

		if(groupBy != null) { 
			copy.setGroupBy( (GroupBy) groupBy.clone());
		} 
		
		if(having != null) { 
			copy.setHaving( (Criteria) having.clone());
		}	

        if(getOrderBy() != null) {
            copy.setOrderBy( (OrderBy) getOrderBy().clone());
        }

        if(getLimit() != null) {
            copy.setLimit( (Limit) getLimit().clone());
        }

		if(getOption() != null) {
			copy.setOption( (Option) getOption().clone());
		}
        
        // Defect 13751: should clone isXML state.
        copy.setIsXML(getIsXML());
        
        if(selectList != null){
        	copy.selectList = new ArrayList();
            Iterator iter = selectList.iterator();
            while(iter.hasNext()) {
            	copy.selectList.add(((SelectSymbol)iter.next()).clone());
            }
        }
        
        if (into != null) {
        	copy.into = (Into)into.clone();
        }
        
        copyMetadataState(copy);
        
		return copy;
	}
	
    /**
     * Compare two queries for equality.  Queries will only evaluate to equal if
     * they are IDENTICAL: select variables are in the same order, criteria are in
     * the same exact structure.  
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
    	// Quick same object test
    	if(this == obj) {
    		return true;
		}

		// Quick fail tests		
    	if(!(obj instanceof Query)) {
    		return false;
		}

		Query other = (Query) obj;
		
        return EquivalenceUtil.areEqual(getSelect(), other.getSelect()) &&
               EquivalenceUtil.areEqual(getFrom(), other.getFrom()) &&
               EquivalenceUtil.areEqual(getCriteria(), other.getCriteria()) &&
               EquivalenceUtil.areEqual(getGroupBy(), other.getGroupBy()) &&
               EquivalenceUtil.areEqual(getHaving(), other.getHaving()) &&
               EquivalenceUtil.areEqual(getOrderBy(), other.getOrderBy()) &&
               EquivalenceUtil.areEqual(getLimit(), other.getLimit()) &&
               EquivalenceUtil.areEqual(getOption(), other.getOption()) &&
               getIsXML() == other.getIsXML();
    }

    /**
     * Get hashcode for query.  WARNING: This hash code relies on the hash codes of the
     * Select and Criteria clauses.  If the query changes, it's hash code will change and
     * it can be lost from collections.  Hash code is only valid after query has been
     * completely constructed.
     * @return Hash code
     */
    public int hashCode() {
    	// For speed, this hash code relies only on the hash codes of its select
    	// and criteria clauses, not on the from, order by, or option clauses
    	int myHash = 0;
    	myHash = HashCodeUtil.hashCode(myHash, this.select);
		myHash = HashCodeUtil.hashCode(myHash, this.criteria);
		return myHash;
	}
      
	/**
	 * @see com.metamatrix.query.sql.lang.Command#areResultsCachable()
	 */
	public boolean areResultsCachable() {
		if(this.getInto() != null){
			return false;
		}
		List projectedSymbols = getProjectedSymbols();
		for(int i=0; i<projectedSymbols.size(); i++){
			SingleElementSymbol projectedSymbol = (SingleElementSymbol)projectedSymbols.get(i);
			if(DataTypeManager.isLOB(projectedSymbol.getType())) {
				return false;
			}
		}
		return true;
	}
    
    public int updatingModelCount(QueryMetadataInterface metadata) throws MetaMatrixComponentException{
        if(isXML) {
            return 0;
        }
        
        if(into != null && !into.getGroup().isTempGroupSymbol()) {
            return 2;
        }
        
        //any subcommand performing an update requires a transaction
        //since we may not be able to roll it back other wise
        int count = getSubCommandsUpdatingModelCount(metadata);
        if (count > 0) {
        	return 2;
        }
        return 0;
    }

    /** 
     * @see com.metamatrix.query.sql.lang.QueryCommand#getProjectedQuery()
     */
    @Override
    public Query getProjectedQuery() {
        return this;
    }
}  // END CLASS
