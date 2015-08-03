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
package org.teiid.odata;

import org.odata4j.expression.*;
import org.odata4j.expression.Expression.DefaultHierarchyVisitor;

public class ODataHierarchyVisitor extends DefaultHierarchyVisitor {
    
	@Override
	public void visitNode(BinaryCommonExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(BinaryBoolCommonExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(BoolParenExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(NegateExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(NotExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(ParenExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(OrderByExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(EndsWithMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(StartsWithMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(SubstringOfMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(CeilingMethodCallExpression obj) {
        obj.visitThis(this);
    }
    
    @Override    
    public void visitNode(ConcatMethodCallExpression obj) {
        obj.visitThis(this);
    }  
    
    @Override    
    public void visitNode(DayMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(FloorMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(HourMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(IndexOfMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(LengthMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(MinuteMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(MonthMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(ReplaceMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(RoundMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(SecondMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(SubstringMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(ToLowerMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(ToUpperMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(TrimMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(YearMethodCallExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(AggregateBoolFunction obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(IsofExpression obj) {
        obj.visitThis(this);
    }

    @Override
    public void visitNode(CastExpression obj) {
    	obj.visitThis(this);
    }
}
