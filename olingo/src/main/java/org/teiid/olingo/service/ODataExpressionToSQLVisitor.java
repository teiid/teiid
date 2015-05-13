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
package org.teiid.olingo.service;

import static org.teiid.language.SQLConstants.Reserved.CAST;
import static org.teiid.language.SQLConstants.Reserved.CONVERT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriResourceCount;
import org.apache.olingo.server.api.uri.UriResourceIt;
import org.apache.olingo.server.api.uri.UriResourceLambdaAll;
import org.apache.olingo.server.api.uri.UriResourceLambdaAny;
import org.apache.olingo.server.api.uri.UriResourceLambdaVariable;
import org.apache.olingo.server.api.uri.UriResourceNavigation;
import org.apache.olingo.server.api.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.api.uri.UriResourceRoot;
import org.apache.olingo.server.api.uri.queryoption.expression.Alias;
import org.apache.olingo.server.api.uri.queryoption.expression.Binary;
import org.apache.olingo.server.api.uri.queryoption.expression.Enumeration;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.LambdaRef;
import org.apache.olingo.server.api.uri.queryoption.expression.Literal;
import org.apache.olingo.server.api.uri.queryoption.expression.Member;
import org.apache.olingo.server.api.uri.queryoption.expression.Method;
import org.apache.olingo.server.api.uri.queryoption.expression.TypeLiteral;
import org.apache.olingo.server.api.uri.queryoption.expression.Unary;
import org.apache.olingo.server.core.RequestURLHierarchyVisitor;
import org.teiid.core.TeiidException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Table;
import org.teiid.olingo.LiteralParser;
import org.teiid.olingo.ODataPlugin;
import org.teiid.olingo.api.ODataExpressionVisitor;
import org.teiid.olingo.api.ODataTypeManager;
import org.teiid.olingo.api.ProjectedColumn;
import org.teiid.olingo.api.SQLParameter;
import org.teiid.olingo.service.ODataSQLBuilder.ItResource;
import org.teiid.olingo.service.ODataSQLBuilder.UniqueNameGenerator;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.ExpressionCriteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.translator.SourceSystemFunctions;

public class ODataExpressionToSQLVisitor extends RequestURLHierarchyVisitor implements  ODataExpressionVisitor{
    private final Stack<org.teiid.query.sql.symbol.Expression> stack = new Stack<org.teiid.query.sql.symbol.Expression>();
    private List<SQLParameter> params;
    private boolean prepared = false;
    private final List<TeiidException> exceptions = new ArrayList<TeiidException>();
    private final UriInfo uriInfo;
    private MetadataStore metadata;
    private GroupSymbol tableGroup;
    private Table table;
    private UniqueNameGenerator nameGenerator;
    private Lambda lambda;
    private boolean count;
    private ItResource itResource;
    
    public ODataExpressionToSQLVisitor(Table table, GroupSymbol tableGroup,
            boolean prepared, UriInfo info, MetadataStore metadata,
            UniqueNameGenerator nameGenerator, ItResource itResource,
            List<SQLParameter> params) {
        this.table = table;
        this.tableGroup = tableGroup;
        this.prepared = prepared;
        this.uriInfo = info;
        this.metadata = metadata;
        this.nameGenerator = nameGenerator;
        this.itResource = itResource;
        this.params = params;
    }

    public org.teiid.query.sql.symbol.Expression getExpression(Expression expr) throws TeiidException {
        accept(expr);
        if (!this.exceptions.isEmpty()) {
            throw this.exceptions.get(0);
        }
        return this.stack.pop();
    }
    
    public org.teiid.query.sql.symbol.Expression getExpression(UriInfoResource info) throws TeiidException {
        visit(info);
        if (!this.exceptions.isEmpty()) {
            throw this.exceptions.get(0);
        }
        return this.stack.pop();
    }
    
    public Lambda getLambda() {
        return lambda;
    }    

    @Override
    public void visit(Alias expr) {
        Object value = LiteralParser.parseLiteral(this.uriInfo.getValueForAlias(expr.getParameterName()));
        if (this.prepared) {
            stack.add(new Reference(this.params.size()));
            this.params.add(new SQLParameter(value, JDBCSQLTypeInfo.getSQLTypeFromClass(value.getClass().getName())));
        } else {
            this.stack.add(new Constant(value));
        }
    }

    @Override
    public void visit(Binary expr) {
        accept(expr.getLeftOperand());
        org.teiid.query.sql.symbol.Expression lhs = this.stack.pop();

        accept(expr.getRightOperand());
        org.teiid.query.sql.symbol.Expression rhs = this.stack.pop();

        org.teiid.query.sql.symbol.Expression binaryExpr = null;
        switch (expr.getOperator()) {
        case HAS:
            // TODO: not supported. What would be SQL equivalent?
            this.exceptions.add(new TeiidException(ODataPlugin.Event.TEIID16036, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16036)));
            break;
        case MUL:
            binaryExpr = new Function("*", new org.teiid.query.sql.symbol.Expression[] { lhs, rhs }); //$NON-NLS-1$
            break;
        case DIV:
            binaryExpr  = new Function("/", new org.teiid.query.sql.symbol.Expression[] { lhs, rhs }); //$NON-NLS-1$
            break;
        case MOD:
            binaryExpr  = new Function("MOD", new org.teiid.query.sql.symbol.Expression[] { lhs, rhs }); //$NON-NLS-1$
            break;
        case ADD:
            binaryExpr  = new Function("+", new org.teiid.query.sql.symbol.Expression[] { lhs, rhs }); //$NON-NLS-1$
            break;
        case SUB:
            binaryExpr  = new Function("-", new org.teiid.query.sql.symbol.Expression[] { lhs, rhs }); //$NON-NLS-1$
            break;
        case GT:
            if (this.lambda != null && this.lambda.getKind() == Lambda.Kind.ALL) {
                // lamba operator is reversed
                binaryExpr = new SubqueryCompareCriteria(rhs, buildSubquery(lhs), CompareCriteria.LT, SubqueryCompareCriteria.ALL);
            }
            else {
                binaryExpr  = new CompareCriteria(lhs, CompareCriteria.GT, rhs);
            }
            break;
        case GE:
            if (this.lambda != null && this.lambda.getKind() == Lambda.Kind.ALL) {
                binaryExpr = new SubqueryCompareCriteria(rhs, buildSubquery(lhs), CompareCriteria.LE, SubqueryCompareCriteria.ALL);
            }
            else {            
                binaryExpr  = new CompareCriteria(lhs, CompareCriteria.GE, rhs);
            }
            break;
        case LT:
            if (this.lambda != null && this.lambda.getKind() == Lambda.Kind.ALL) {
                binaryExpr = new SubqueryCompareCriteria(rhs, buildSubquery(lhs), CompareCriteria.GT, SubqueryCompareCriteria.ALL);
            }
            else {            
                binaryExpr  = new CompareCriteria(lhs, CompareCriteria.LT, rhs);
            }
            break;
        case LE:
            if (this.lambda != null && this.lambda.getKind() == Lambda.Kind.ALL) {
                binaryExpr = new SubqueryCompareCriteria(rhs, buildSubquery(lhs), CompareCriteria.GE, SubqueryCompareCriteria.ALL);
            }
            else {            
                binaryExpr  = new CompareCriteria(lhs, CompareCriteria.LE, rhs);
            }
            break;
        case EQ:
            if (this.lambda != null && this.lambda.getKind() == Lambda.Kind.ALL) {
                binaryExpr = new SubqueryCompareCriteria(rhs, buildSubquery(lhs), CompareCriteria.EQ, SubqueryCompareCriteria.ALL);
            }            
            else if (rhs instanceof Constant && ((Constant) rhs).getType() == DataTypeManager.DefaultDataClasses.NULL) {
                binaryExpr  = new IsNullCriteria(lhs);
            } else {
                binaryExpr  = new CompareCriteria(lhs, CompareCriteria.EQ, rhs);
            }
            break;
        case NE:
            if (this.lambda != null && this.lambda.getKind() == Lambda.Kind.ALL) {
                binaryExpr = new SubqueryCompareCriteria(rhs, buildSubquery(lhs), CompareCriteria.NE, SubqueryCompareCriteria.ALL);
            }
            else if (rhs instanceof Constant && ((Constant) rhs).getType() == DataTypeManager.DefaultDataClasses.NULL) {
                IsNullCriteria crit = new IsNullCriteria(lhs);
                crit.setNegated(true);
                binaryExpr  = crit;
            } else {
                binaryExpr  = new CompareCriteria(lhs, CompareCriteria.NE, rhs);
            }
            break;
        case AND:
            binaryExpr  = new CompoundCriteria(CompoundCriteria.AND, (Criteria) lhs, (Criteria) rhs);
            break;
        case OR:
            binaryExpr  = new CompoundCriteria(CompoundCriteria.OR, (Criteria) lhs, (Criteria) rhs);
            break;
        }
        
        this.stack.push(binaryExpr);
    }

    @Override
    public void visit(Enumeration expr) {
        this.exceptions.add(new TeiidException("unsupported option"));//$NON-NLS-1$
    }

    @Override
    public void visit(LambdaRef expr) {
        // TODO: any and All implementations
    }

    @Override
    public void visit(Literal expr) {
        Object value = LiteralParser.parseLiteral(expr.getText());
        if (this.prepared) {
            stack.add(new Reference(this.params.size()));
            this.params.add(new SQLParameter(value, JDBCSQLTypeInfo.getSQLTypeFromClass(value.getClass().getName())));
        } else {
            this.stack.add(new Constant(value));
        }
    }

    @Override
    public void visit(Member expr) {
        // this is seg way into RequestURLHierarchyVisitor based parsing
        visit(expr.getResourcePath());
    }

    @Override
    public void visit(Method expr) {
        List<org.teiid.query.sql.symbol.Expression> teiidExprs = new ArrayList<org.teiid.query.sql.symbol.Expression>();
        for (Expression exp : expr.getParameters()) {
            accept(exp);
            teiidExprs.add(this.stack.pop());
        }

        switch (expr.getMethod()) {
        case CONTAINS:
            CompareCriteria criteria = new CompareCriteria(new Function("LOCATE", new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(1), teiidExprs.get(0), new Constant(1) }), CompareCriteria.GE, new Constant(1)); //$NON-NLS-1$
            this.stack.push(criteria);
            break;
        case STARTSWITH:
            criteria = new CompareCriteria(new Function("LOCATE", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(1), teiidExprs.get(0), new Constant(1) }), CompareCriteria.EQ, new Constant(1)); //$NON-NLS-1$
            this.stack.push(criteria);
            break;
        case ENDSWITH:
            criteria = new CompareCriteria(new Function("ENDSWITH", new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(1), teiidExprs.get(0) }), CompareCriteria.EQ, new Constant(Boolean.TRUE));//$NON-NLS-1$
            this.stack.push(criteria);
            break;
        case LENGTH:
            this.stack.push(new Function("LENGTH", new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case INDEXOF:
            stack.push(new Function("LOCATE", new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(1), teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case SUBSTRING:
            this.stack.push(new Function("SUBSTRING", teiidExprs.toArray(new org.teiid.query.sql.symbol.Expression[teiidExprs.size()]))); //$NON-NLS-1$
            break;
        case TOLOWER:
            this.stack.push(new Function("LCASE", new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case TOUPPER:
            this.stack.push(new Function("UCASE", new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case TRIM:
            this.stack.push(new Function("TRIM", new org.teiid.query.sql.symbol.Expression[] { new Constant("BOTH"), new Constant(' '), teiidExprs.get(0) })); //$NON-NLS-1$ //$NON-NLS-2$
            break;
        case CONCAT:
            this.stack.push(new Function("CONCAT2", new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0), teiidExprs.get(1) })); //$NON-NLS-1$
            break;
        case YEAR:
            this.stack.push(new Function("YEAR", new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case MONTH:
            this.stack.push(new Function("MONTH", new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case DAY:
            this.stack.push(new Function("DAYOFMONTH", new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case HOUR:
            this.stack.push(new Function("HOUR", new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case MINUTE:
            this.stack.push(new Function("MINUTE", new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case SECOND:
            this.stack.push(new Function("SECOND", new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case NOW:
            this.stack.push(new Function("NOW", new org.teiid.query.sql.symbol.Expression[] {})); //$NON-NLS-1$
            break;
        case ROUND:
            stack.push(new Function("ROUND", new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0), new Constant(0) })); //$NON-NLS-1$
            break;
        case FLOOR:
            this.stack.push(new Function("FLOOR", new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case CEILING:
            this.stack.push(new Function("CEILING", new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case CAST:
            this.stack.push(new Function(CONVERT,new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(0), teiidExprs.get(1) }));
            break;
        case FRACTIONALSECONDS:
        case TOTALSECONDS:
        case DATE:
        case TIME:
        case TOTALOFFSETMINUTES:
        case MINDATETIME:
        case MAXDATETIME:
        case GEODISTANCE:
        case GEOLENGTH:
        case GEOINTERSECTS:
        case ISOF:
        default:
            this.exceptions.add(new TeiidException(ODataPlugin.Event.TEIID16027, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16027, expr.getMethod())));
        }
    }

    @Override
    public void visit(TypeLiteral expr) {
        this.stack.push(new Constant(ODataTypeManager.teiidType((SingletonPrimitiveType) expr.getType(), false)));
    }

    @Override
    public void visit(Unary expr) {
        accept(expr.getOperand());
        org.teiid.query.sql.symbol.Expression teiidExpr = this.stack.pop();
        switch (expr.getOperator()) {
        case MINUS:
            this.stack.push(new Function(SourceSystemFunctions.MULTIPLY_OP,new org.teiid.query.sql.symbol.Expression[] {new Constant(-1), teiidExpr }));
            break;
        case NOT:
            this.stack.push(new NotCriteria(new ExpressionCriteria(teiidExpr)));
            break;
        }
    }
    
    private void accept(Expression expr) {
        if (expr instanceof Alias) {
            visit((Alias) expr);
        } else if (expr instanceof Binary) {
            visit((Binary) expr);
        } else if (expr instanceof Enumeration) {
            visit((Enumeration) expr);
        } else if (expr instanceof LambdaRef) {
            visit((LambdaRef) expr);
        } else if (expr instanceof Literal) {
            visit((Literal) expr);
        } else if (expr instanceof Member) {
            visit((Member) expr);
        } else if (expr instanceof Method) {
            visit((Method) expr);
        } else if (expr instanceof TypeLiteral) {
            visit((TypeLiteral) expr);
        } else if (expr instanceof Unary) {
            visit((Unary) expr);
        }
    }
    
    /////////////////////////////////////////////////////////////////////////
    //RequestURLHierarchyVisitor specific methods
    /////////////////////////////////////////////////////////////////////////
    @Override
    public void visit(UriResourcePrimitiveProperty info) {
        if (this.lambda != null) {
            this.stack.add(new ElementSymbol(info.getProperty().getName(), this.lambda.getGroupSymbol()));
        } else {
            this.stack.add(new ElementSymbol(info.getProperty().getName(), this.tableGroup));
        }
    }
    
    @Override
    public void visit(UriResourceCount option) {
        count = true;
    }
    
    @Override
    public void visit(UriResourceNavigation info) {
        EdmEntityType type = (EdmEntityType)info.getType();
        
//        if (!count) {
//            this.exceptions.add(new TeiidException(ODataPlugin.Event.TEIID16028, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16028)));
//        }
        
        Table navigationTable = ODataSQLBuilder.findTable(type, this.metadata);
        GroupSymbol navigationGroup = new GroupSymbol(this.nameGenerator.getNextGroup(), navigationTable.getFullName());//$NON-NLS-1$
        
        Query query = new Query();
        query.setSelect(new Select(Arrays.asList(new AggregateSymbol(AggregateSymbol.Type.COUNT.name(), false, null))));
        query.setFrom(new From(Arrays.asList(new UnaryFromClause(navigationGroup))));

        Criteria criteria = null;
        ForeignKey fk = null;
        if (info.isCollection()) {
            fk = ODataSQLBuilder.joinFK(navigationTable, this.table);    
        }
        else {
            fk = ODataSQLBuilder.joinFK(this.table, navigationTable);
        }
        
        if (fk != null) {
            List<String> lhsColumns = ODataSQLBuilder.getColumnNames(fk.getColumns());
            List<String> rhsColumns = fk.getReferenceColumns();
            for (int i = 0; i < lhsColumns.size(); i++) {
                if (criteria == null) {
                    criteria = new CompareCriteria(new ElementSymbol(lhsColumns.get(i), this.tableGroup),
                            CompareCriteria.EQ, new ElementSymbol(rhsColumns.get(i), navigationGroup));
                } else {
                    Criteria subcriteria = new CompareCriteria(new ElementSymbol(lhsColumns.get(i), this.tableGroup),
                            CompareCriteria.EQ, new ElementSymbol(rhsColumns.get(i), navigationGroup));
                    criteria = new CompoundCriteria(CompoundCriteria.AND, criteria, subcriteria);
                }
            }
        }
        else {
            this.exceptions.add(new TeiidException(ODataPlugin.Event.TEIID16037, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16037)));
        }
        
        query.setCriteria(criteria);
        this.stack.add(new ScalarSubquery(query));
    }
    
    @Override
    public void visit(UriResourceLambdaAll resource) {
        this.lambda = new Lambda();
        this.lambda.setKind(Lambda.Kind.ALL);
        UriResourceLambdaAll all = (UriResourceLambdaAll)resource;
        accept(all.getExpression());
    }

    @Override
    public void visit(UriResourceLambdaAny resource) {
        this.lambda = new Lambda();
        this.lambda.setKind(Lambda.Kind.ANY);
        UriResourceLambdaAny any = (UriResourceLambdaAny) resource;
        accept(any.getExpression());
    }

    @Override
    public void visit(UriResourceLambdaVariable resource) {
        this.lambda.setName(resource.getVariableName());
        this.lambda.setType((EdmEntityType)resource.getType());
        this.lambda.setCollection(resource.isCollection());
        Table table = ODataSQLBuilder.findTable(this.lambda.getType(), this.metadata);
        this.lambda.setTable(table);
        this.lambda.setGroupSymbol(new GroupSymbol(this.lambda.getName(), table.getFullName()));
    }    
    
    private QueryCommand buildSubquery(org.teiid.query.sql.symbol.Expression projected) {
        Criteria criteria = null;
        
        for (ForeignKey fk:this.lambda.getTable().getForeignKeys()) {
            if (fk.getReferenceKey().getParent().equals(this.table)) {
                List<String> refColumns = fk.getReferenceColumns();
                if (refColumns == null) {
                    refColumns = ODataSQLBuilder.getColumnNames(this.lambda.getTable().getPrimaryKey().getColumns());
                }                   
                
                List<String> pkColumns = ODataSQLBuilder.getColumnNames(this.table.getPrimaryKey().getColumns());
                List<Criteria> critList = new ArrayList<Criteria>();

                for (int i = 0; i < refColumns.size(); i++) {
                    critList.add(new CompareCriteria(new ElementSymbol(pkColumns.get(i), this.tableGroup), CompareCriteria.EQ, new ElementSymbol(refColumns.get(i), this.lambda.getGroupSymbol())));
                }                   
                
                criteria = critList.get(0);
                for (int i = 1; i < critList.size(); i++) {
                    criteria = new CompoundCriteria(CompoundCriteria.AND, criteria, critList.get(i));
                }                           
            }
        }       
        Select s1 = new Select();
        s1.addSymbol(projected); 
        From f1 = new From();
        f1.addGroup(this.lambda.getGroupSymbol());
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);    
        q1.setCriteria(criteria);
        
        return q1;
    }
    
    @Override
    public void visit(UriResourceIt info) {
        if (this.itResource == null) {
            this.exceptions.add(new TeiidException(ODataPlugin.Event.TEIID16040, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16040)));
        }
        
        if (info.getType() instanceof SingletonPrimitiveType) {
            String group = this.nameGenerator.getNextGroup();
            ElementSymbol es = new ElementSymbol("col", new GroupSymbol(group));
            String type = ODataTypeManager.teiidType((SingletonPrimitiveType)info.getType(), false);
            Function castFunction = new Function(CAST,new org.teiid.query.sql.symbol.Expression[] {es, new Constant(type)});            
            this.stack.push(castFunction);
            
            StoredProcedure procedure = new StoredProcedure();
            procedure.setProcedureName("arrayiterate");
            
            List<SPParameter> params = new ArrayList<SPParameter>();
            SPParameter param = new SPParameter(1, SPParameter.IN, "val");
            param.setExpression(this.itResource.getReferencedProperty());
            params.add(param);
            
            procedure.setParameter(param);
            
            SubqueryFromClause fromClause = new SubqueryFromClause(group, procedure);
            fromClause.setTable(true);
            
            AliasSymbol expression = new AliasSymbol(this.itResource.getReferencedProperty().getShortName(), castFunction);
            this.itResource.setProjectedFromClause(fromClause);
            this.itResource.setProjectedProperty(new ProjectedColumn(expression, true, info.getType(), true));
        }
        else {
            this.exceptions.add(new TeiidException(ODataPlugin.Event.TEIID16010, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16010)));
        }
    }
    
    @Override
    public void visit(UriResourceRoot info) {
    }
}
