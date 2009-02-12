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

package com.metamatrix.query.sql.navigator;

import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.symbol.ScalarSubquery;


/** 
 * @since 4.2
 */
public class DeepPostOrderNavigator extends PostOrderNavigator {

    public DeepPostOrderNavigator(LanguageVisitor visitor) {
        super(visitor);
    }

    public void visit(ExistsCriteria obj) {
        visitNode(obj.getCommand());
        visitVisitor(obj);
    }
    public void visit(ScalarSubquery obj) {
        visitNode(obj.getCommand());
        visitVisitor(obj);
    }
    public void visit(SubqueryCompareCriteria obj) {
        visitNode(obj.getLeftExpression());
        visitNode(obj.getCommand());
        visitVisitor(obj);
    }
    public void visit(SubqueryFromClause obj) {
        visitNode(obj.getCommand());
        visitNode(obj.getGroupSymbol());
        visitVisitor(obj);
    }
    public void visit(SubquerySetCriteria obj) {
        visitNode(obj.getCommand());
        visitNode(obj.getExpression());
        visitVisitor(obj);
    }
    
    public static void doVisit(LanguageObject object, LanguageVisitor visitor) {
        DeepPostOrderNavigator nav = new DeepPostOrderNavigator(visitor);
        object.acceptVisitor(nav);
    }

}
