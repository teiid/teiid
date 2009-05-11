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

package org.teiid.connector.jdbc.translator;

import org.teiid.connector.language.ICompareCriteria;
import org.teiid.connector.language.IExistsCriteria;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.IInCriteria;
import org.teiid.connector.language.IInlineView;
import org.teiid.connector.language.IInsert;
import org.teiid.connector.language.IInsertExpressionValueSource;
import org.teiid.connector.language.ILanguageObject;
import org.teiid.connector.language.ILikeCriteria;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.language.IScalarSubquery;
import org.teiid.connector.language.ISearchedCaseExpression;
import org.teiid.connector.language.ISetClause;
import org.teiid.connector.language.ISubqueryCompareCriteria;
import org.teiid.connector.visitor.framework.HierarchyVisitor;

/**
 * This visitor will mark literals in well known locations as bindValues.
 * These values will be put in the generated SQL as ? 
 * and have the corresponding value set on the PreparedStatement 
 */
final class BindValueVisitor extends HierarchyVisitor {

    private boolean replaceWithBinding = false;

    public void visit(IInlineView obj) {
        replaceWithBinding = false;
        visitNode(obj.getQuery());
    }

    public void visit(IScalarSubquery obj) {
        replaceWithBinding = false;
        super.visit(obj);
    }

    public void visit(IExistsCriteria obj) {
        replaceWithBinding = false;
        super.visit(obj);
    }

    public void visit(ISubqueryCompareCriteria obj) {
        replaceWithBinding = false;
        super.visit(obj);
    }

    /**
     * In general it is not appropriate to use bind values within a function
     * unless the particulars of the function parameters are know.  
     * As needed, other visitors or modifiers can set the literals used within
     * a particular function as bind variables.  
     */
    public void visit(IFunction obj) {
        replaceWithBinding = false;
        super.visit(obj);
    }

    public void visit(IInCriteria obj) {
        replaceWithBinding = true;
        visitNodes(obj.getRightExpressions());
    }

    public void visit(ILikeCriteria obj) {
        replaceWithBinding = true;
        visitNode(obj.getRightExpression());
    }

    /**
     * Note that this will only visit the right expression.  In general most compares
     * involving literals will be something like element = literal (this is enforced as
     * much as possible by the QueryRewriter).  In rare circumstances, it is possible to
     * have literal = literal (most notably null <> null).  Using bind variables on
     * both sides of the operator is not supported by most databases.
     */
    public void visit(ICompareCriteria obj) {
        replaceWithBinding = true;
        visitNode(obj.getRightExpression());
    }

    /**
     * Will look for bind values in the when criteria.
     * The actual restriction for case statements seems to be that at least one branch must
     * not contain a bind variable.
     */
    public void visit(ISearchedCaseExpression obj) {
        for (int i = 0; i < obj.getWhenCount(); i++) {
            visitNode(obj.getWhenCriteria(i));
        }
    }

    @Override
    public void visit(IInsertExpressionValueSource obj) {
        replaceWithBinding = true;
        visitNodes(obj.getValues());
    }
    
    @Override
    public void visit(ISetClause obj) {
        replaceWithBinding = true;
        visitNode(obj.getValue());
    }

    public void visit(ILiteral obj) {
        if (replaceWithBinding || TranslatedCommand.isBindEligible(obj)) {
            obj.setBindValue(true);
        }
    }
    
    public void visitNode(ILanguageObject obj) {
        boolean replacementMode = replaceWithBinding;
        super.visitNode(obj);
        this.replaceWithBinding = replacementMode;
    }
}