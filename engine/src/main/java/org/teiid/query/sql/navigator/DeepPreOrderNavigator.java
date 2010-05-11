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

package org.teiid.query.sql.navigator;

import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.symbol.ScalarSubquery;


/** 
 * @since 4.2
 */
public class DeepPreOrderNavigator extends PreOrderNavigator {

    /** 
     * @param visitor
     * @since 4.2
     */
    public DeepPreOrderNavigator(LanguageVisitor visitor) {
        super(visitor);
    }


    public void visit(ExistsCriteria obj) {
        visitVisitor(obj);
        visitNode(obj.getCommand());
    }
    public void visit(ScalarSubquery obj) {
        visitVisitor(obj);
        visitNode(obj.getCommand());
    }
    public void visit(SubqueryCompareCriteria obj) {
        visitVisitor(obj);
        visitNode(obj.getLeftExpression());
        visitNode(obj.getCommand());
    }
    public void visit(SubqueryFromClause obj) {
        visitVisitor(obj);
        visitNode(obj.getCommand());
        visitNode(obj.getGroupSymbol());
    }
    public void visit(SubquerySetCriteria obj) {
        visitVisitor(obj);
        visitNode(obj.getCommand());
        visitNode(obj.getExpression());
    }
    
    public static void doVisit(LanguageObject object, LanguageVisitor visitor) {
        DeepPreOrderNavigator nav = new DeepPreOrderNavigator(visitor);
        object.acceptVisitor(nav);
    }

}
