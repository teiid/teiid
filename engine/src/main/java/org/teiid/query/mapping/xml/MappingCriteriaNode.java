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

package org.teiid.query.mapping.xml;

import org.teiid.query.sql.lang.Criteria;



/** 
 * Represents a Criteria Node under a Choice Node, which defines the criteria
 * on the selection of child elements.
 */
public class MappingCriteriaNode extends MappingBaseNode{
    boolean defalt;
    Criteria criteriaNode;
    
    public MappingCriteriaNode(String criteria, boolean defalt) {
        setCriteria(criteria);
        setAsDefault(defalt);
        setProperty(MappingNodeConstants.Properties.NODE_TYPE, MappingNodeConstants.CRITERIA);
    }
         
    public MappingCriteriaNode() {
        setAsDefault(true);
        setProperty(MappingNodeConstants.Properties.NODE_TYPE, MappingNodeConstants.CRITERIA);
    }
    
    public void acceptVisitor(MappingVisitor visitor) {
        visitor.visit(this);
    }
    
    public MappingCriteriaNode setCriteria(String criteria) {        
        if (criteria != null && criteria.length() > 0) {
            criteria = criteria.trim();
            setProperty(MappingNodeConstants.Properties.CRITERIA, criteria);
        }
        return this;
    }
    
    public MappingCriteriaNode setAsDefault(boolean defalt) {
        this.defalt = defalt;
        setProperty(MappingNodeConstants.Properties.IS_DEFAULT_CHOICE, Boolean.valueOf(defalt));
        return this;
    }
    
    public boolean isDefault() {
        return this.defalt;
    }
    
    public String getCriteria(){
        return (String) getProperty(MappingNodeConstants.Properties.CRITERIA);
    }   
    
    /**
     * This is parsed and resolved criteria node based on the criteria string. This is set by
     * ValidateMappedCriteriaVisitor class during pre planning.
     * @param node
     */
    public void setCriteriaNode(Criteria node) {
        this.criteriaNode = node;
    }
    
    public Criteria getCriteriaNode() {
        return this.criteriaNode;
    }
    
    /** 
     * @see org.teiid.query.mapping.xml.MappingNode#isExcluded()
     */
    public boolean isExcluded() {
        return false;
    }
}
