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
 * A element which specifies a recursive element inside and xml schema.
 * A recursive element is that embeds the self node type of elements 
 * upto given depth limit. 
 */
public class MappingRecursiveElement extends MappingElement {
    Criteria criteriaNode;    
    
    public MappingRecursiveElement(String name, String mappingClass) {
        super(name);
        setProperty(MappingNodeConstants.Properties.IS_RECURSIVE, Boolean.TRUE);
        setProperty(MappingNodeConstants.Properties.RECURSION_ROOT_MAPPING_CLASS, mappingClass);         
    }
    
    public MappingRecursiveElement(String name, Namespace namespace, String mappingClass) {
        super(name, namespace);
        setProperty(MappingNodeConstants.Properties.IS_RECURSIVE, Boolean.TRUE);
        setProperty(MappingNodeConstants.Properties.RECURSION_ROOT_MAPPING_CLASS, mappingClass);         
    }    
    
    public void acceptVisitor(MappingVisitor visitor) {
        visitor.visit(this);
    }
    
    /**
     * Is recursice
     * @see org.teiid.query.mapping.xml.MappingElement#isRecursive()
     */
    public boolean isRecursive() {
        return true;
    }
    
    /**
     * Sets the criteria on which the recurrsion must occur
     */
    public MappingRecursiveElement setCriteria(String criteria) {
        if (criteria != null && criteria.length() > 0) {
            criteria = criteria.trim();
            setProperty(MappingNodeConstants.Properties.RECURSION_CRITERIA, criteria);
        }
        return this;
    }
    
    /**
     * Sets limit on how deep the recurrsion is allowed to occur in result
     * document. If the execeptionOnBreach is set to true, if the limit rules are
     * violated then exception will be thrown, otherwise re-currsion will stop at
     * the depth specified.
     */
    public MappingRecursiveElement setRecursionLimit(int depth, boolean execeptionOnBreach) {
        setProperty(MappingNodeConstants.Properties.RECURSION_LIMIT, new Integer(depth));
        setProperty(MappingNodeConstants.Properties.EXCEPTION_ON_RECURSION_LIMIT, Boolean.valueOf(execeptionOnBreach));
        return this;
    }
    
    public String getMappingClass() {
        return (String) getProperty(MappingNodeConstants.Properties.RECURSION_ROOT_MAPPING_CLASS);        
    }
    
    /**
     * Get the re-currsion criteria; not to be consufused with criteria on the MappingCriteria 
     * element
     * @return
     */
    public String getCriteria(){
        return (String) getProperty(MappingNodeConstants.Properties.RECURSION_CRITERIA);
    }    
    
    public int getRecursionLimit() {
        Integer limit = (Integer)getProperty(MappingNodeConstants.Properties.RECURSION_LIMIT);
        if (limit != null) {
            return limit.intValue();
        }
        return MappingNodeConstants.Defaults.DEFAULT_RECURSION_LIMIT.intValue();
    }
    
    public boolean throwExceptionOnRecurrsionLimit() {
        Boolean breached = (Boolean)getProperty(MappingNodeConstants.Properties.EXCEPTION_ON_RECURSION_LIMIT);
        if (breached != null) {
            return breached.booleanValue();
        }
        return MappingNodeConstants.Defaults.DEFAULT_EXCEPTION_ON_RECURSION_LIMIT.booleanValue();
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
}
