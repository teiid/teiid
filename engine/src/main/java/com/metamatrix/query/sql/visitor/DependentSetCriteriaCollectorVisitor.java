/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.query.sql.visitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.navigator.DeepPreOrderNavigator;


/** 
 * Visitor to collect DependentSetCriteria.  
 */
public class DependentSetCriteriaCollectorVisitor extends LanguageVisitor {

    // List<DependentSetCriteria>
    private List crits = new ArrayList();
    
    /** 
     * 
     */
    public DependentSetCriteriaCollectorVisitor() {
        super();
    }

    /**
     * Get the criteria collected by the visitor.  This should be called
     * after the visitor has been run on the language object tree.
     * @return List of {@link com.metamatrix.query.sql.lang.DependentSetCriteria}
     */
    public List getCriteria() {
        return this.crits;
    }

    /**
     * Visit a language object and collect symbols.  This method should <b>NOT</b> be
     * called directly.
     * @param obj Language object
     */
    public void visit(DependentSetCriteria obj) {
        this.crits.add(obj);
    }
    
    /**
     * Get all DependentSetCriteria found in a deep search from the input LanguageObject. 
     * @param obj Starting language object (criteria, command, etc)
     * @return List of DependentSetCriteria
     */
    public static List getDependentSetCriteria(LanguageObject obj) {
        if(obj == null) {
            return Collections.EMPTY_LIST;
        }
        
        DependentSetCriteriaCollectorVisitor visitor = new DependentSetCriteriaCollectorVisitor();
        DeepPreOrderNavigator.doVisit(obj, visitor);        
        return visitor.getCriteria();
        
    }

}
