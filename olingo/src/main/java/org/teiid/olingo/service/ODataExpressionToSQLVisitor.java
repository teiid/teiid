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
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriResourceCount;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;
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
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.MetadataStore;
import org.teiid.odata.api.SQLParameter;
import org.teiid.olingo.ODataExpressionVisitor;
import org.teiid.olingo.ODataPlugin;
import org.teiid.olingo.ODataTypeManager;
import org.teiid.olingo.service.ODataSQLBuilder.URLParseService;
import org.teiid.olingo.service.TeiidServiceHandler.UniqueNameGenerator;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.ExpressionCriteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryFromClause;
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
    static enum ExpressionType{LAMBDAALL, LAMBDAANY, ROOT, ANY}
    
    private final Stack<org.teiid.query.sql.symbol.Expression> stack = new Stack<org.teiid.query.sql.symbol.Expression>();
    private List<SQLParameter> params;
    private boolean prepared = false;
    private final List<TeiidException> exceptions = new ArrayList<TeiidException>();
    private final UriInfo uriInfo;
    private MetadataStore metadata;
    private DocumentNode ctxQuery;
    private DocumentNode ctxExpression;
    private UniqueNameGenerator nameGenerator;
    private URLParseService parseService;
    private ExpressionType exprType = ExpressionType.ANY;
    private String lastPropertyType;
    
    public ODataExpressionToSQLVisitor(DocumentNode resource,
            boolean prepared, UriInfo info, MetadataStore metadata,
            UniqueNameGenerator nameGenerator, 
            List<SQLParameter> params, URLParseService parseService) {
        this.ctxQuery = resource;
        this.prepared = prepared;
        this.uriInfo = info;
        this.metadata = metadata;
        this.nameGenerator = nameGenerator;
        this.params = params;
        this.parseService = parseService;
        this.ctxExpression = this.ctxQuery;
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
    
    public DocumentNode getEntityResource() {
        return this.ctxQuery;
    }

    public DocumentNode getExpresionEntityResource() {
        return this.ctxExpression;
    }
    
    @Override
    public void visit(Alias expr) {
        String strValue = this.uriInfo.getValueForAlias(expr.getParameterName());
        try {
            if (strValue.startsWith("$root")) {
                this.stack.add(new ScalarSubquery(this.parseService.parse(strValue)));
            }
            else {
                String type = "Edm.String";
                if (this.lastPropertyType != null) {
                    EdmPrimitiveTypeKind kind = ODataTypeManager.odataType(this.lastPropertyType);
                    type = kind.getFullQualifiedName().getFullQualifiedNameAsString();
                    this.lastPropertyType = null;
                }
                Object value = ODataTypeManager.parseLiteral(type, strValue);
                if (this.prepared) {
                    stack.add(new Reference(this.params.size()));
                    this.params.add(new SQLParameter(value, JDBCSQLTypeInfo.getSQLTypeFromClass(value.getClass().getName())));
                } else {
                    this.stack.add(new Constant(value));
                }
            }
        } catch (TeiidException e) {
            this.exceptions.add(e);
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
            if (this.exprType == ExpressionType.LAMBDAALL) {
                // lamba operator is reversed
                binaryExpr = new SubqueryCompareCriteria(rhs, buildSubquery(
                        this.ctxExpression, lhs), CompareCriteria.LT,
                        SubqueryCompareCriteria.ALL);
            }
            else {
                binaryExpr  = new CompareCriteria(lhs, CompareCriteria.GT, rhs);
            }
            break;
        case GE:
            if (this.exprType == ExpressionType.LAMBDAALL) {
                binaryExpr = new SubqueryCompareCriteria(rhs, buildSubquery(
                        this.ctxExpression, lhs), CompareCriteria.LE,
                        SubqueryCompareCriteria.ALL);
            }
            else {            
                binaryExpr  = new CompareCriteria(lhs, CompareCriteria.GE, rhs);
            }
            break;
        case LT:
            if (this.exprType == ExpressionType.LAMBDAALL) {
                binaryExpr = new SubqueryCompareCriteria(rhs, buildSubquery(
                        this.ctxExpression, lhs), CompareCriteria.GT,
                        SubqueryCompareCriteria.ALL);
            }
            else {            
                binaryExpr  = new CompareCriteria(lhs, CompareCriteria.LT, rhs);
            }
            break;
        case LE:
            if (this.exprType == ExpressionType.LAMBDAALL) {
                binaryExpr = new SubqueryCompareCriteria(rhs, buildSubquery(
                        this.ctxExpression, lhs), CompareCriteria.GE,
                        SubqueryCompareCriteria.ALL);
            }
            else {            
                binaryExpr  = new CompareCriteria(lhs, CompareCriteria.LE, rhs);
            }
            break;
        case EQ:
            if (this.exprType == ExpressionType.LAMBDAALL) {
                binaryExpr = new SubqueryCompareCriteria(rhs, buildSubquery(
                        this.ctxExpression, lhs), CompareCriteria.EQ,
                        SubqueryCompareCriteria.ALL);
            }            
            else if (rhs instanceof Constant && ((Constant) rhs).getType() == DataTypeManager.DefaultDataClasses.NULL) {
                binaryExpr  = new IsNullCriteria(lhs);
            } else {
                binaryExpr  = new CompareCriteria(lhs, CompareCriteria.EQ, rhs);
            }
            break;
        case NE:
            if (this.exprType == ExpressionType.LAMBDAALL) {
                binaryExpr = new SubqueryCompareCriteria(rhs, buildSubquery(
                        this.ctxExpression, lhs), CompareCriteria.NE,
                        SubqueryCompareCriteria.ALL);
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
        try {
            Object value = null;
            if (expr.getText() != null && !expr.getText().equalsIgnoreCase("null")) {
                String type = expr.getType().getFullQualifiedName().getFullQualifiedNameAsString();
                value = ODataTypeManager.parseLiteral(type, expr.getText());
            }
            if (this.prepared) {
                stack.add(new Reference(this.params.size()));
                this.params.add(new SQLParameter(value, JDBCSQLTypeInfo.getSQLTypeFromClass(value.getClass().getName())));
            } else {
                this.stack.add(new Constant(value));
            }
        } catch (TeiidException e) {
            this.exceptions.add(e);
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
        if (this.exprType == ExpressionType.ROOT) {
            this.stack.add(new ScalarSubquery(buildRootSubQuery(info.getProperty().getName(), this.ctxExpression)));
        }
        else {
            this.stack.add(new ElementSymbol(info.getProperty().getName(), this.ctxExpression.getGroupSymbol()));
        }
        
        // hack to resolve the property type.
        Column c = this.ctxExpression.getTable().getColumnByName(info.getProperty().getName());
        this.lastPropertyType = c.getRuntimeType();
    }
    
    @Override
    public void visit(UriResourceCount option) {
    }
    
    @Override
    public void visit(UriResourceNavigation info) {
        try {
            DocumentNode navigationResource = DocumentNode.build(
                    (EdmEntityType) info.getType(), info.getKeyPredicates(), this.metadata,
                    this.nameGenerator, true, getUriInfo(), this.parseService);

            Query query = new Query();
            query.setSelect(new Select(Arrays.asList(new AggregateSymbol(AggregateSymbol.Type.COUNT.name(), false, null))));
            query.setFrom(new From(Arrays.asList(navigationResource.getFromClause())));

            Criteria criteria = null;
            ForeignKey fk = null;
            if (info.isCollection()) {
                fk = DocumentNode.joinFK(navigationResource.getTable(), this.ctxQuery.getTable());    
            }
            else {
                fk = DocumentNode.joinFK(this.ctxQuery.getTable(), navigationResource.getTable());
            }
            
            if (fk != null) {
                List<String> lhsColumns = DocumentNode.getColumnNames(fk.getColumns());
                List<String> rhsColumns = fk.getReferenceColumns();
                for (int i = 0; i < lhsColumns.size(); i++) {
                    if (criteria == null) {
                        criteria = new CompareCriteria(new ElementSymbol(lhsColumns.get(i), this.ctxQuery.getGroupSymbol()),
                                CompareCriteria.EQ, new ElementSymbol(rhsColumns.get(i), navigationResource.getGroupSymbol()));
                    } else {
                        Criteria subcriteria = new CompareCriteria(new ElementSymbol(lhsColumns.get(i), this.ctxQuery.getGroupSymbol()),
                                CompareCriteria.EQ, new ElementSymbol(rhsColumns.get(i), navigationResource.getGroupSymbol()));
                        criteria = new CompoundCriteria(CompoundCriteria.AND, criteria, subcriteria);
                    }
                }
            }
            else {
                throw new TeiidException(ODataPlugin.Event.TEIID16037, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16037));
            }            
            query.setCriteria(criteria);
            this.stack.add(new ScalarSubquery(query));
        } catch (TeiidException e) {
            this.exceptions.add(e);
        }
    }
    
    @Override
    public void visit(UriResourceLambdaAll resource) {
        this.exprType = ExpressionType.LAMBDAALL;
        UriResourceLambdaAll all = (UriResourceLambdaAll)resource;
        accept(all.getExpression());
    }

    @Override
    public void visit(UriResourceLambdaAny resource) {
        this.exprType = ExpressionType.LAMBDAANY;
        UriResourceLambdaAny any = (UriResourceLambdaAny) resource;
        accept(any.getExpression());
    }

    @Override
    public void visit(UriResourceLambdaVariable resource) {
        try {
            DocumentNode lambda = DocumentNode.build(
                    (EdmEntityType) resource.getType(), null, this.metadata,
                    this.nameGenerator, false, this.uriInfo,
                    this.parseService);
            lambda.setGroupSymbol(new GroupSymbol(resource.getVariableName(), lambda.getTable().getFullName()));
            
            if (this.exprType == ExpressionType.LAMBDAALL) {
                // ALL - needs to modeled as subquery
                this.ctxExpression = lambda;
            } 
            else {
                //ANY - Needs to be joined to resource.
                this.ctxQuery.joinTable(
                        lambda, 
                        (DocumentNode.joinFK(this.ctxQuery.getTable(), lambda.getTable()) == null), 
                        JoinType.JOIN_INNER);
                lambda.addCriteria(this.ctxQuery.getCriteria());
                lambda.setDistinct(true);
                this.ctxExpression = lambda;
                this.ctxQuery = lambda;
            }
        } catch (TeiidException e) {
            this.exceptions.add(e);
        }
    }    
    
    private QueryCommand buildSubquery(DocumentNode eResource,
            org.teiid.query.sql.symbol.Expression projected) {
        Select s1 = new Select();
        s1.addSymbol(projected); 
        
        Query q = new Query();
        From from = new From();
        from.addGroup(eResource.getGroupSymbol());
        q.setFrom(from);    
        q.setCriteria(DocumentNode.buildJoinCriteria(eResource, this.ctxQuery));
        
        q.setSelect(s1);
        return q;
    }
    
    @Override
    public void visit(UriResourceIt info) {
        if (info.getType() instanceof SingletonPrimitiveType) {
            String group = this.nameGenerator.getNextGroup();
            GroupSymbol groupSymbol = new GroupSymbol(group);
            ElementSymbol es = new ElementSymbol("col", groupSymbol);
            String type = ODataTypeManager.teiidType((SingletonPrimitiveType)info.getType(), false);
            Function castFunction = new Function(CAST,new org.teiid.query.sql.symbol.Expression[] {es, new Constant(type)});            
            this.stack.push(castFunction);
            
            StoredProcedure procedure = new StoredProcedure();
            procedure.setProcedureName("arrayiterate");
            
            ElementSymbol projectedEs = (ElementSymbol)this.ctxQuery.getProjectedColumns().get(0).getExpression();
            List<SPParameter> params = new ArrayList<SPParameter>();
            SPParameter param = new SPParameter(1, SPParameter.IN, "val");
            param.setExpression(projectedEs);
            params.add(param);
            
            procedure.setParameter(param);
            
            SubqueryFromClause fromClause = new SubqueryFromClause(group, procedure);
            fromClause.setTable(true);
            
            DocumentNode itResource = new DocumentNode();
            AliasSymbol expression = new AliasSymbol(projectedEs.getShortName(), castFunction);
            itResource.setFromClause(fromClause);
            itResource.setGroupSymbol(groupSymbol);            
            itResource.addProjectedColumn(expression, true, info.getType(), true);
            
            this.ctxQuery.getProjectedColumns().remove(0);
            this.ctxQuery.addSibiling(itResource);
        }
        else {
            this.exceptions.add(new TeiidException(ODataPlugin.Event.TEIID16010, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16010)));
        }
    }
    
    @Override
    public void visit(UriResourceRoot info) {
        this.exprType = ExpressionType.ROOT;
    }
    
    @Override
    public void visit(UriResourceEntitySet info) {
        EdmEntityType edmEntityType = info.getEntitySet().getEntityType();
        if (this.exprType == ExpressionType.ROOT) {
            try {
                this.ctxExpression = DocumentNode.build(edmEntityType,
                        info.getKeyPredicates(), this.metadata, this.nameGenerator,
                        true, getUriInfo(), null);
            } catch (TeiidException e) {
                this.exceptions.add(e);
            }
        }
        else {
            if (this.ctxQuery.getEdmEntityType().getFullQualifiedName().equals(edmEntityType.getFullQualifiedName())) {
                this.ctxExpression = this.ctxQuery;
            }
            else {
                for (DocumentNode er: this.ctxQuery.getSibilings()) {
                    if (er.getEdmEntityType().getFullQualifiedName().equals(edmEntityType.getFullQualifiedName())) {
                        this.ctxExpression = er;
                        break;
                    }
                }
            }
        }
    }
    
    public QueryCommand buildRootSubQuery(String element, DocumentNode resource) {
        Select s1 = new Select();
        s1.addSymbol(new ElementSymbol(element, resource.getGroupSymbol())); 
        From f1 = new From();
        f1.addGroup(resource.getGroupSymbol());
        Query q1 = new Query();
        q1.setSelect(s1);
        q1.setFrom(f1);    
        q1.setCriteria(resource.getCriteria());  
        return q1;
    }
}
