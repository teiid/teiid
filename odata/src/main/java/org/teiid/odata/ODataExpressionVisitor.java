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

import static org.teiid.language.SQLConstants.Reserved.CONVERT;

import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.odata4j.expression.AddExpression;
import org.odata4j.expression.AndExpression;
import org.odata4j.expression.BinaryLiteral;
import org.odata4j.expression.BoolMethodExpression;
import org.odata4j.expression.BoolParenExpression;
import org.odata4j.expression.BooleanLiteral;
import org.odata4j.expression.ByteLiteral;
import org.odata4j.expression.CastExpression;
import org.odata4j.expression.CeilingMethodCallExpression;
import org.odata4j.expression.ConcatMethodCallExpression;
import org.odata4j.expression.DateTimeLiteral;
import org.odata4j.expression.DateTimeOffsetLiteral;
import org.odata4j.expression.DayMethodCallExpression;
import org.odata4j.expression.DecimalLiteral;
import org.odata4j.expression.DivExpression;
import org.odata4j.expression.DoubleLiteral;
import org.odata4j.expression.EndsWithMethodCallExpression;
import org.odata4j.expression.EntitySimpleProperty;
import org.odata4j.expression.EqExpression;
import org.odata4j.expression.FloorMethodCallExpression;
import org.odata4j.expression.GeExpression;
import org.odata4j.expression.GtExpression;
import org.odata4j.expression.GuidLiteral;
import org.odata4j.expression.HourMethodCallExpression;
import org.odata4j.expression.IndexOfMethodCallExpression;
import org.odata4j.expression.Int64Literal;
import org.odata4j.expression.IntegralLiteral;
import org.odata4j.expression.IsofExpression;
import org.odata4j.expression.LeExpression;
import org.odata4j.expression.LengthMethodCallExpression;
import org.odata4j.expression.LtExpression;
import org.odata4j.expression.MinuteMethodCallExpression;
import org.odata4j.expression.ModExpression;
import org.odata4j.expression.MonthMethodCallExpression;
import org.odata4j.expression.MulExpression;
import org.odata4j.expression.NeExpression;
import org.odata4j.expression.NegateExpression;
import org.odata4j.expression.NotExpression;
import org.odata4j.expression.NullLiteral;
import org.odata4j.expression.OrExpression;
import org.odata4j.expression.ParenExpression;
import org.odata4j.expression.ReplaceMethodCallExpression;
import org.odata4j.expression.RoundMethodCallExpression;
import org.odata4j.expression.SByteLiteral;
import org.odata4j.expression.SecondMethodCallExpression;
import org.odata4j.expression.SingleLiteral;
import org.odata4j.expression.StartsWithMethodCallExpression;
import org.odata4j.expression.StringLiteral;
import org.odata4j.expression.SubExpression;
import org.odata4j.expression.SubstringMethodCallExpression;
import org.odata4j.expression.SubstringOfMethodCallExpression;
import org.odata4j.expression.TimeLiteral;
import org.odata4j.expression.ToLowerMethodCallExpression;
import org.odata4j.expression.ToUpperMethodCallExpression;
import org.odata4j.expression.TrimMethodCallExpression;
import org.odata4j.expression.YearMethodCallExpression;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.ExpressionCriteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.odata.ODataTypeManager;

public class ODataExpressionVisitor extends ODataHierarchyVisitor {
    private ArrayList<SQLParam> params = new ArrayList<SQLParam>();
    private boolean prepared = true;
    private Stack<Expression> stack = new Stack<Expression>();
    
    public ODataExpressionVisitor(boolean prepared) {
        this.prepared = prepared;
    }
    
    boolean isPrepared() {
        return this.prepared;
    }
    
    boolean isEmpty() {
        return this.stack.isEmpty();
    }    
    
    Expression getExpression() {
        return stack.pop();
    }
    
    void setExpression(Expression expr) {
        stack.push(expr);
    }
    
    public List<SQLParam> getParameters(){
        return this.params;
    }    
    
    @Override
    public void visit(AddExpression expr) {
        visitNode(expr.getLHS());
        visitNode(expr.getRHS());
        Expression rhs = stack.pop();
        Expression lhs = stack.pop();
        stack.push(new Function("+", new Expression[] {lhs, rhs})); //$NON-NLS-1$
    }

    @Override
    public void visit(AndExpression expr) {
        visitNode(expr.getLHS());
        visitNode(expr.getRHS());
        Expression rhs = stack.pop();
        Expression lhs = stack.pop();
        Criteria criteria = new CompoundCriteria(CompoundCriteria.AND, (Criteria)lhs, (Criteria)rhs);
        stack.push(criteria);
    }

    @Override
    public void visit(BooleanLiteral expr) {
        if (this.prepared) {
            stack.add(new Reference(this.params.size())); 
            this.params.add(new SQLParam(expr.getValue(), Types.BOOLEAN));
        }
        else {
            stack.add(new Constant(expr.getValue())); 
        }
    }

    @Override
    public void visit(CastExpression expr) {
        visitNode(expr.getExpression());
        Expression rhs = new Constant(ODataTypeManager.teiidType(expr.getType()));
        Expression lhs = stack.pop();
        stack.push(new Function(CONVERT, new Expression[] {lhs, rhs})); 
    }

    @Override
    public void visit(ConcatMethodCallExpression expr) {
        visitNode(expr.getLHS());
        visitNode(expr.getRHS());
        Expression rhs = stack.pop();
        Expression lhs = stack.pop();       
        stack.push(new Function("CONCAT2", new Expression[] {lhs, rhs})); //$NON-NLS-1$ 
    }

    @Override
    public void visit(DateTimeLiteral expr) {
        Timestamp timestamp = new Timestamp(expr.getValue().toDateTime().getMillis());
        if (this.prepared) {
            stack.add(new Reference(this.params.size())); 
            this.params.add(new SQLParam(timestamp, Types.TIMESTAMP));
        }
        else {
            stack.add(new Constant(timestamp)); 
        }
    }

    @Override
    public void visit(DateTimeOffsetLiteral expr) {
        throw new UnsupportedOperationException();

    }

    @Override
    public void visit(DecimalLiteral expr) {
        if (this.prepared) {
            stack.add(new Reference(this.params.size())); 
            this.params.add(new SQLParam(expr.getValue(), Types.DECIMAL));
        }
        else {
            stack.add(new Constant(expr.getValue())); 
        }
    }

    @Override
    public void visit(DivExpression expr) {
        visitNode(expr.getLHS());
        visitNode(expr.getRHS());
        Expression rhs = stack.pop();
        Expression lhs = stack.pop();       
        stack.push(new Function("/", new Expression[] {lhs, rhs})); //$NON-NLS-1$           
    }

    @Override
    public void visit(EndsWithMethodCallExpression expr) {
        visitNode(expr.getTarget());
        Expression target = stack.pop();
        visitNode(expr.getValue());
        Expression value = stack.pop();     
        Criteria criteria = new CompareCriteria(new Function("ENDSWITH",
                new Expression[] { target, value }), CompareCriteria.EQ,
                new Constant(Boolean.TRUE));
        stack.push(criteria);
    }
    @Override
    public void visit(EqExpression expr) {
        visitNode(expr.getLHS());
        visitNode(expr.getRHS());
        Expression rhs = stack.pop();
        Expression lhs = stack.pop();
        Criteria criteria = null;
        if (rhs instanceof Constant && ((Constant)rhs).getType() == DataTypeManager.DefaultDataClasses.NULL) {
            criteria = new IsNullCriteria(lhs);
        }
        else {
            criteria = new CompareCriteria(lhs, CompareCriteria.EQ, rhs);
        }
        stack.push(criteria);
    }

    @Override
    public void visit(GeExpression expr) {
        visitNode(expr.getLHS());
        visitNode(expr.getRHS());
        Expression rhs = stack.pop();
        Expression lhs = stack.pop();       
        Criteria criteria = new CompareCriteria(lhs, CompareCriteria.GE, rhs);
        stack.push(criteria);
    }

    @Override
    public void visit(GtExpression expr) {
        visitNode(expr.getLHS());
        visitNode(expr.getRHS());
        Expression rhs = stack.pop();
        Expression lhs = stack.pop();       
        Criteria criteria = new CompareCriteria(lhs, CompareCriteria.GT, rhs);
        stack.push(criteria);
    }

    @Override
    public void visit(GuidLiteral expr) {
        if (this.prepared) {
            stack.add(new Reference(this.params.size())); 
            this.params.add(new SQLParam(expr.getValue().toString(), Types.VARCHAR));
        }
        else {
            stack.add(new Constant(expr.getValue().toString())); 
        }
    }

    @Override
    public void visit(BinaryLiteral expr) {
        if (this.prepared) {
            stack.add(new Reference(this.params.size())); 
            this.params.add(new SQLParam(expr.getValue(), Types.BINARY));
        }
        else {
            stack.add(new Constant(expr.getValue())); 
        }
    }

    @Override
    public void visit(ByteLiteral expr) {
        if (this.prepared) {
            stack.add(new Reference(this.params.size())); 
            this.params.add(new SQLParam(expr.getValue(), Types.TINYINT));
        }
        else {
            stack.add(new Constant(expr.getValue())); 
        }
    }

    @Override
    public void visit(SByteLiteral expr) {
        if (this.prepared) {
            stack.add(new Reference(this.params.size())); 
            this.params.add(new SQLParam(expr.getValue(), Types.TINYINT));
        }
        else {
            stack.add(new Constant(expr.getValue())); 
        }
    }

    @Override
    public void visit(IndexOfMethodCallExpression expr) {
        visitNode(expr.getValue());
        visitNode(expr.getTarget());
        Expression target = stack.pop();
        Expression value = stack.pop();     
        stack.push(new Function("LOCATE", new Expression[] {value, target})); //$NON-NLS-1$
    }

    @Override
    public void visit(SingleLiteral expr) {
        if (this.prepared) {
            stack.add(new Reference(this.params.size())); 
            this.params.add(new SQLParam(expr.getValue(), Types.FLOAT));
        }
        else {
            stack.add(new Constant(expr.getValue())); 
        }
    }

    @Override
    public void visit(DoubleLiteral expr) {
        if (this.prepared) {
            stack.add(new Reference(this.params.size())); 
            this.params.add(new SQLParam(expr.getValue(), Types.DOUBLE));
        }
        else {
            stack.add(new Constant(expr.getValue())); 
        }
    }

    @Override
    public void visit(IntegralLiteral expr) {
        if (this.prepared) {
            stack.add(new Reference(this.params.size())); 
            this.params.add(new SQLParam(expr.getValue(), Types.INTEGER));
        }
        else {
            stack.add(new Constant(expr.getValue())); 
        }
    }

    @Override
    public void visit(Int64Literal expr) {
        if (this.prepared) {
            stack.add(new Reference(this.params.size())); 
            this.params.add(new SQLParam(expr.getValue(), Types.BIGINT));
        }
        else {
            stack.add(new Constant(expr.getValue())); 
        }
    }

    @Override
    public void visit(IsofExpression expr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(LeExpression expr) {
        visitNode(expr.getLHS());
        visitNode(expr.getRHS());
        Expression rhs = stack.pop();
        Expression lhs = stack.pop();       
        Criteria criteria = new CompareCriteria(lhs, CompareCriteria.LE, rhs);
        stack.push(criteria);
    }

    @Override
    public void visit(LengthMethodCallExpression expr) {
        visitNode(expr.getTarget());    
        stack.push(new Function("LENGTH", new Expression[] {stack.pop()})); //$NON-NLS-1$
    }

    @Override
    public void visit(LtExpression expr) {
        visitNode(expr.getLHS());
        visitNode(expr.getRHS());
        Expression rhs = stack.pop();
        Expression lhs = stack.pop();       
        Criteria criteria = new CompareCriteria(lhs, CompareCriteria.LT, rhs);
        stack.push(criteria);
    }

    @Override
    public void visit(ModExpression expr) {
        visitNode(expr.getLHS());   
        visitNode(expr.getRHS());
        Expression rhs = stack.pop();
        Expression lhs = stack.pop();       
        stack.push(new Function("MOD", new Expression[] {lhs, rhs})); //$NON-NLS-1$
    }

    @Override
    public void visit(MulExpression expr) {
        visitNode(expr.getLHS());
        visitNode(expr.getRHS());
        Expression rhs = stack.pop();
        Expression lhs = stack.pop();       
        stack.push(new Function("*", new Expression[] {lhs, rhs})); //$NON-NLS-1$
    }

    @Override
    public void visit(NeExpression expr) {
        visitNode(expr.getLHS());
        visitNode(expr.getRHS());
        Expression rhs = stack.pop();
        Expression lhs = stack.pop();
        Criteria criteria = null;
        if (rhs instanceof Constant && ((Constant)rhs).getType() == DataTypeManager.DefaultDataClasses.NULL) {
            IsNullCriteria crit = new IsNullCriteria(lhs);
            crit.setNegated(true);
            criteria = crit;
        }
        else {
            criteria = new CompareCriteria(lhs, CompareCriteria.NE, rhs);
        }
        stack.push(criteria);
    }

    @Override
    public void visit(NegateExpression expr) {
        visitNode(expr.getExpression());
        Expression ex = stack.pop();
        stack.push(new Function(SourceSystemFunctions.MULTIPLY_OP, new Expression[] {new Constant(-1), ex})); 
    }

    @Override
    public void visit(NotExpression expr) {
        visitNode(expr.getExpression());
        Criteria criteria = new NotCriteria(new ExpressionCriteria(stack.pop()));
        stack.push(criteria);
    }

    @Override
    public void visit(NullLiteral expr) {
        stack.push(new Constant(null));
    }

    @Override
    public void visit(OrExpression expr) {
        visitNode(expr.getLHS());
        visitNode(expr.getRHS());
        Expression rhs = stack.pop();
        Expression lhs = stack.pop();
        Criteria criteria = new CompoundCriteria(CompoundCriteria.OR, (Criteria)lhs, (Criteria)rhs);
        stack.push(criteria);
    }

    @Override
    public void visit(ParenExpression expr) {
        visitNode(expr.getExpression());
    }

    @Override
    public void visit(BoolParenExpression expr) {
        visitNode(expr.getExpression());
    }

    @Override
    public void visit(ReplaceMethodCallExpression expr) {
        List<Expression> expressions = new ArrayList<Expression>();
        visitNode(expr.getTarget());
        expressions.add(stack.pop());
        visitNode(expr.getFind());
        expressions.add(stack.pop());
        visitNode(expr.getReplace());
        expressions.add(stack.pop());
        stack.push(new Function("REPLACE", expressions.toArray(new Expression[expressions.size()]))); //$NON-NLS-1$
    }

    @Override
    public void visit(StartsWithMethodCallExpression expr) {
        locate(expr, CompareCriteria.EQ);
    }

    private void locate(BoolMethodExpression expr, int compare) {
        visitNode(expr.getTarget());
        Expression target = stack.pop();
        visitNode(expr.getValue());
        Expression value = stack.pop();     
        Criteria criteria = new CompareCriteria(new Function("LOCATE",
                new Expression[] { value, target, new Constant(1) }), compare,
                new Constant(1));
        stack.push(criteria);
    }

    @Override
    public void visit(StringLiteral expr) {
        if (this.prepared) {
            stack.push(new Reference(this.params.size()));
            this.params.add(new SQLParam(expr.getValue(), Types.VARCHAR));
        }
        else {
            stack.push(new Constant(expr.getValue()));
        }
    }

    @Override
    public void visit(SubExpression expr) {
        visitNode(expr.getLHS());
        visitNode(expr.getRHS());
        Expression rhs = stack.pop();
        Expression lhs = stack.pop();
        stack.push(new Function("-", new Expression[] {lhs, rhs})); //$NON-NLS-1$       
    }

    @Override
    public void visit(SubstringMethodCallExpression expr) {
        visitNode(expr.getTarget());
        List<Expression> expressions = new ArrayList<Expression>();
        expressions.add(stack.pop());
        if (expr.getStart() != null) {
            visitNode(expr.getStart());
            expressions.add(stack.pop());
        }
        if (expr.getLength() != null) {
            visitNode(expr.getLength());
            expressions.add(stack.pop());
        }
        stack.push(new Function("SUBSTRING", expressions.toArray(new Expression[expressions.size()]))); //$NON-NLS-1$       
    }

    @Override
    public void visit(SubstringOfMethodCallExpression expr) {
        locate(expr, CompareCriteria.GE);
    }

    @Override
    public void visit(TimeLiteral expr) {
        Time time = new Time(expr.getValue().toDateTimeToday().getMillis());
        if (this.prepared) {
            stack.push(new Reference(this.params.size()));
            this.params.add(new SQLParam(time, Types.TIME));
        }
        else {
            stack.push(new Constant(time));
        }       
    }

    @Override
    public void visit(ToLowerMethodCallExpression expr) {
        visitNode(expr.getTarget());
        stack.push(new Function("LCASE", new Expression[] {stack.pop()})); //$NON-NLS-1$
    }

    @Override
    public void visit(ToUpperMethodCallExpression expr) {
        visitNode(expr.getTarget());
        stack.push(new Function("UCASE", new Expression[] {stack.pop()})); //$NON-NLS-1$
    }

    @Override
    public void visit(TrimMethodCallExpression expr) {
        visitNode(expr.getTarget());
        stack.push(new Function("TRIM", 
                new Expression[] {new Constant("BOTH"), new Constant(' '), stack.pop()})); //$NON-NLS-1$
    }

    @Override
    public void visit(YearMethodCallExpression expr) {
        visitNode(expr.getTarget());
        stack.push(new Function("YEAR", new Expression[] {stack.pop()})); //$NON-NLS-1$     
    }

    @Override
    public void visit(MonthMethodCallExpression expr) {
        visitNode(expr.getTarget());
        stack.push(new Function("MONTH", new Expression[] {stack.pop()})); //$NON-NLS-1$
    }

    @Override
    public void visit(DayMethodCallExpression expr) {
        visitNode(expr.getTarget());
        stack.push(new Function("DAYOFMONTH", new Expression[] {stack.pop()})); //$NON-NLS-1$
    }

    @Override
    public void visit(HourMethodCallExpression expr) {
        visitNode(expr.getTarget());
        stack.push(new Function("HOUR", new Expression[] {stack.pop()})); //$NON-NLS-1$
    }

    @Override
    public void visit(MinuteMethodCallExpression expr) {
        visitNode(expr.getTarget());
        stack.push(new Function("MINUTE", new Expression[] {stack.pop()})); //$NON-NLS-1$
    }

    @Override
    public void visit(SecondMethodCallExpression expr) {
        visitNode(expr.getTarget());
        stack.push(new Function("SECOND", new Expression[] {stack.pop()})); //$NON-NLS-1$
    }

    @Override
    public void visit(RoundMethodCallExpression expr) {
        visitNode(expr.getTarget());
        stack.push(new Function("ROUND", new Expression[] {stack.pop(), new Constant(0)})); //$NON-NLS-1$
    }

    @Override
    public void visit(FloorMethodCallExpression expr) {
        visitNode(expr.getTarget());
        stack.push(new Function("FLOOR", new Expression[] {stack.pop()})); //$NON-NLS-1$
    }

    @Override
    public void visit(CeilingMethodCallExpression expr) {
        visitNode(expr.getTarget());
        stack.push(new Function("CEILING", new Expression[] {stack.pop()})); //$NON-NLS-1$
    }
    
    @Override
    public void visit(EntitySimpleProperty expr) {
        String property = expr.getPropertyName();
        if (property.indexOf('/') == -1) {
            stack.push(new ElementSymbol(property, getDocumentGroup()));
            return;
        }
        
        String[] segments = property.split("/");
        setExpression(new ElementSymbol(segments[1], getDocumentGroup()));
    }    
    
    public GroupSymbol getDocumentGroup() {
        return null;
    }
}
