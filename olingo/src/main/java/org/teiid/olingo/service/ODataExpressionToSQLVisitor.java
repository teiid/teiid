/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.olingo.service;

import static org.teiid.language.SQLConstants.Reserved.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Stack;

import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.geo.Geospatial;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.uri.*;
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
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.core.util.Assertion;
import org.teiid.metadata.MetadataStore;
import org.teiid.odata.api.SQLParameter;
import org.teiid.olingo.ODataExpressionVisitor;
import org.teiid.olingo.ODataPlugin;
import org.teiid.olingo.ProjectedColumn;
import org.teiid.olingo.common.ODataTypeManager;
import org.teiid.olingo.service.DocumentNode.ContextColumn;
import org.teiid.olingo.service.ODataSQLBuilder.URLParseService;
import org.teiid.olingo.service.TeiidServiceHandler.UniqueNameGenerator;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.symbol.*;
import org.teiid.translator.SourceSystemFunctions;

public class ODataExpressionToSQLVisitor extends RequestURLHierarchyVisitor implements  ODataExpressionVisitor{
    private final Stack<org.teiid.query.sql.symbol.Expression> stack = new Stack<org.teiid.query.sql.symbol.Expression>();
    private List<SQLParameter> params;
    private boolean prepared = false;
    private final UriInfo uriInfo;
    private MetadataStore metadata;
    private DocumentNode ctxQuery;
    private DocumentNode ctxExpression;
    private DocumentNode ctxLambda;
    private UniqueNameGenerator nameGenerator;
    private URLParseService parseService;
    private ContextColumn lastProperty;
    private OData odata;
    private boolean root;

    public ODataExpressionToSQLVisitor(DocumentNode resource,
            boolean prepared, UriInfo info, MetadataStore metadata, OData odata,
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
        this.odata = odata;
    }

    public org.teiid.query.sql.symbol.Expression getExpression(Expression expr) throws TeiidException {
        try {
            accept(expr);
        } catch (TeiidRuntimeException e) {
            if (e.getCause() instanceof TeiidException) {
                throw (TeiidException)e.getCause();
            }
            throw e;
        }
        return this.stack.pop();
    }

    public org.teiid.query.sql.symbol.Expression getExpression(UriInfoResource info) throws TeiidException {
        try {
            visit(info);
        } catch (TeiidRuntimeException e) {
            if (e.getCause() instanceof TeiidException) {
                throw (TeiidException)e.getCause();
            }
            throw e;
        }
        return this.stack.pop();
    }

    public DocumentNode getEntityResource() {
        return this.ctxQuery;
    }

    @Override
    public void visit(Alias expr) {
        String strValue = this.uriInfo.getValueForAlias(expr.getParameterName());
        try {
            if (strValue == null) {
                this.stack.add(new Constant(null));
            }
            else if (strValue.startsWith("$root")) {
                this.stack.add(new ScalarSubquery(this.parseService.parse(strValue, null)));
            }
            else {
                String type = "Edm.String";
                if (this.lastProperty != null) {
                    EdmPrimitiveTypeKind kind = this.lastProperty.getEdmPrimitiveTypeKind();
                    type = kind.getFullQualifiedName().getFullQualifiedNameAsString();
                    this.lastProperty = null;
                }
                Object value = ODataTypeManager.parseLiteral(type, strValue);
                handleValue(value);
            }
        } catch (TeiidException e) {
            throw new TeiidRuntimeException(e);
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
            throw new TeiidRuntimeException(new TeiidNotImplementedException(
                    ODataPlugin.Event.TEIID16036,
                    ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16036)));
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
            binaryExpr  = new CompareCriteria(lhs, CompareCriteria.GT, rhs);
            break;
        case GE:
            binaryExpr  = new CompareCriteria(lhs, CompareCriteria.GE, rhs);
            break;
        case LT:
            binaryExpr  = new CompareCriteria(lhs, CompareCriteria.LT, rhs);
            break;
        case LE:
            binaryExpr  = new CompareCriteria(lhs, CompareCriteria.LE, rhs);
            break;
        case EQ:
            if (rhs instanceof Constant && ((Constant) rhs).getType() == DataTypeManager.DefaultDataClasses.NULL) {
                binaryExpr  = new IsNullCriteria(lhs);
            } else {
                binaryExpr  = new CompareCriteria(lhs, CompareCriteria.EQ, rhs);
            }
            break;
        case NE:
            if (rhs instanceof Constant && ((Constant) rhs).getType() == DataTypeManager.DefaultDataClasses.NULL) {
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
        throw new TeiidRuntimeException(new TeiidException("unsupported option"));//$NON-NLS-1$
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
            handleValue(value);
        } catch (TeiidException e) {
            throw new TeiidRuntimeException(e);
        }
    }

    private void handleValue(Object value) {
        boolean isGeo = false;
        if (value instanceof Geospatial) {
            String geoLiteral = ((Geospatial)value).toString();
            //extract ewkt
            value = geoLiteral.substring(geoLiteral.indexOf("'")+1, geoLiteral.length() - 1); //$NON-NLS-1$
            isGeo = true;
        }
        org.teiid.query.sql.symbol.Expression ex = null;
        if (!this.prepared || value == null) {
            ex = new Constant(value);
        } else {
            ex = new Function(
                    CONVERT,
                    new org.teiid.query.sql.symbol.Expression[] {
                            new Reference(this.params.size()),
                            new Constant(DataTypeManager.getDataTypeName(value.getClass())) });
            this.params.add(new SQLParameter(value,
                    JDBCSQLTypeInfo.getSQLTypeFromClass(value.getClass().getName())));
        }
        if (isGeo) {
            ex = new Function(SourceSystemFunctions.ST_GEOMFROMEWKT, new org.teiid.query.sql.symbol.Expression[] {ex});
        }
        stack.add(ex);
    }

    @Override
    public void visit(Member expr) {
        // this is seg way into RequestURLHierarchyVisitor based parsing
        visit(expr.getResourcePath());
    }

    private org.teiid.query.sql.symbol.Expression addOne(
            org.teiid.query.sql.symbol.Expression expr) {

        org.teiid.query.sql.symbol.Expression when = new CompareCriteria(expr,
                CompareCriteria.LT, new Constant(0));
        SearchedCaseExpression caseExpr = new SearchedCaseExpression(Arrays.asList(when),
                Arrays.asList(expr));
        caseExpr.setElseExpression(new Function("+",
                new org.teiid.query.sql.symbol.Expression[] { expr, new Constant(1) }));
        return caseExpr;
    }

    private org.teiid.query.sql.symbol.Expression minusOne(
            org.teiid.query.sql.symbol.Expression expr) {
        return new Function("-", new org.teiid.query.sql.symbol.Expression[] {
                expr, new Constant(1) });
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
            CompareCriteria criteria = new CompareCriteria(new Function("LOCATE",
                    new org.teiid.query.sql.symbol.Expression[] {
                        teiidExprs.get(1), teiidExprs.get(0), new Constant(1) }),
                        CompareCriteria.GE, new Constant(1)); //$NON-NLS-1$
            this.stack.push(criteria);
            break;
        case STARTSWITH:
            criteria = new CompareCriteria(new Function("LOCATE",
                    new org.teiid.query.sql.symbol.Expression[] {
                    teiidExprs.get(1), teiidExprs.get(0), new Constant(1) }),
                    CompareCriteria.EQ, new Constant(1)); //$NON-NLS-1$
            this.stack.push(criteria);
            break;
        case ENDSWITH:
            criteria = new CompareCriteria(new Function("ENDSWITH",
                    new org.teiid.query.sql.symbol.Expression[] {teiidExprs.get(1), teiidExprs.get(0) }),
                    CompareCriteria.EQ, new Constant(Boolean.TRUE));//$NON-NLS-1$
            this.stack.push(criteria);
            break;
        case LENGTH:
            this.stack.push(new Function("LENGTH",
                    new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case INDEXOF:
            stack.push(minusOne(new Function("LOCATE", new org.teiid.query.sql.symbol.Expression[]
                    { teiidExprs.get(1), teiidExprs.get(0)}))); //$NON-NLS-1$
            break;
        case SUBSTRING:
            org.teiid.query.sql.symbol.Expression[] exprs =
                teiidExprs.toArray(new org.teiid.query.sql.symbol.Expression[teiidExprs.size()]);
                exprs[1] = addOne(exprs[1]);
            this.stack.push(new Function("SUBSTRING", exprs)); //$NON-NLS-1$
            break;
        case TOLOWER:
            this.stack.push(new Function("LCASE",
                    new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case TOUPPER:
            this.stack.push(new Function("UCASE",
                    new org.teiid.query.sql.symbol.Expression[] { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case TRIM:
            this.stack.push(new Function("TRIM",
                    new org.teiid.query.sql.symbol.Expression[] { new Constant("BOTH"),
                    new Constant(' '), teiidExprs.get(0) })); //$NON-NLS-1$ //$NON-NLS-2$
            break;
        case CONCAT:
            this.stack.push(new Function("CONCAT", new org.teiid.query.sql.symbol.Expression[]
                    { teiidExprs.get(0), teiidExprs.get(1) })); //$NON-NLS-1$
            break;
        case YEAR:
            this.stack.push(new Function("YEAR", new org.teiid.query.sql.symbol.Expression[]
                    { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case MONTH:
            this.stack.push(new Function("MONTH", new org.teiid.query.sql.symbol.Expression[]
                    { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case DAY:
            this.stack.push(new Function("DAYOFMONTH", new org.teiid.query.sql.symbol.Expression[]
                    { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case HOUR:
            this.stack.push(new Function("HOUR", new org.teiid.query.sql.symbol.Expression[]
                    { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case MINUTE:
            this.stack.push(new Function("MINUTE", new org.teiid.query.sql.symbol.Expression[]
                    { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case SECOND:
            this.stack.push(new Function("SECOND", new org.teiid.query.sql.symbol.Expression[]
                    { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case NOW:
            this.stack.push(new Function("NOW", new org.teiid.query.sql.symbol.Expression[] {})); //$NON-NLS-1$
            break;
        case ROUND:
            stack.push(new Function("ROUND", new org.teiid.query.sql.symbol.Expression[]
                    { teiidExprs.get(0), new Constant(0) })); //$NON-NLS-1$
            break;
        case FLOOR:
            this.stack.push(new Function("FLOOR", new org.teiid.query.sql.symbol.Expression[]
                    { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case CEILING:
            this.stack.push(new Function("CEILING", new org.teiid.query.sql.symbol.Expression[]
                    { teiidExprs.get(0) })); //$NON-NLS-1$
            break;
        case CAST:
            this.stack.push(new Function(CONVERT,new org.teiid.query.sql.symbol.Expression[]
                    {teiidExprs.get(0), teiidExprs.get(1) }));
            break;

        case DATE:
            this.stack.push(new Function(CONVERT,new org.teiid.query.sql.symbol.Expression[]
                    {teiidExprs.get(0),  new Constant(DataTypeManager.DefaultDataTypes.DATE)}));
            break;
        case TIME:
            this.stack.push(new Function(CONVERT,new org.teiid.query.sql.symbol.Expression[]
                    {teiidExprs.get(0),  new Constant(DataTypeManager.DefaultDataTypes.TIME)}));
            break;
        case GEODISTANCE:
            this.stack.push(new Function(SourceSystemFunctions.ST_DISTANCE, new org.teiid.query.sql.symbol.Expression[] {
                    teiidExprs.get(0), teiidExprs.get(1)
            }));
            break;
        case GEOLENGTH:
            this.stack.push(new Function(SourceSystemFunctions.ST_LENGTH, new org.teiid.query.sql.symbol.Expression[] {
                    teiidExprs.get(0)
            }));
            break;
        case GEOINTERSECTS:
            this.stack.push(new Function(SourceSystemFunctions.ST_INTERSECTS, new org.teiid.query.sql.symbol.Expression[] {
                    teiidExprs.get(0), teiidExprs.get(1)
            }));
            break;
        case FRACTIONALSECONDS:
        case TOTALSECONDS:
        case TOTALOFFSETMINUTES:
        case MINDATETIME:
        case MAXDATETIME:
        case ISOF:
        default:
            throw new TeiidRuntimeException(new TeiidNotImplementedException(
                    ODataPlugin.Event.TEIID16027,
                    ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16027, expr.getMethod())));
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
            this.stack.push(new Function(SourceSystemFunctions.MULTIPLY_OP,
                    new org.teiid.query.sql.symbol.Expression[] {new Constant(-1), teiidExpr }));
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
        if (this.root) {
            this.stack.add(new ScalarSubquery(buildRootSubQuery(info.getProperty().getName(), this.ctxExpression)));
            root = false;
        }
        else {
            this.stack.add(new ElementSymbol(info.getProperty().getName(), this.ctxExpression.getGroupSymbol()));
        }
        // hack to resolve the property type.
        ContextColumn c = this.ctxExpression.getColumnByName(info.getProperty().getName());
        this.lastProperty = c;
        //revert back to the query context
        this.ctxExpression = this.ctxQuery;
    }

    @Override
    public void visit(UriResourceCount option) {
    }

    @Override
    public void visit(UriResourceNavigation info) {
        try {
            DocumentNode navigationResource = DocumentNode.build(
                    (EdmEntityType) info.getType(), info.getKeyPredicates(), this.metadata, this.odata,
                    this.nameGenerator, true, getUriInfo(), this.parseService);

            Query query = new Query();
            query.setSelect(new Select(Arrays.asList(new AggregateSymbol(AggregateSymbol.Type.COUNT.name(), false, null))));
            query.setFrom(new From(Arrays.asList(navigationResource.getFromClause())));

            Criteria criteria = this.ctxQuery.buildJoinCriteria(navigationResource, info.getProperty());
            if (criteria == null) {
                throw new TeiidException(ODataPlugin.Event.TEIID16037, ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16037));
            }
            query.setCriteria(criteria);
            this.stack.add(new ScalarSubquery(query));
        } catch (TeiidException e) {
            throw new TeiidRuntimeException(e);
        }
    }

    @Override
    public void visit(UriResourceLambdaAll all) {
        accept(all.getExpression());
        if (this.ctxLambda != null) {
            org.teiid.query.sql.symbol.Expression predicate = this.stack.pop();
                predicate = new SubqueryCompareCriteria(new Constant(true), buildSubquery(
                        this.ctxLambda, predicate), CompareCriteria.EQ,
                        SubqueryCompareCriteria.ALL);
                this.stack.push(predicate);
        }
        this.ctxLambda = null;
    }

    @Override
    public void visit(UriResourceLambdaAny any) {
        accept(any.getExpression());
        if (this.ctxLambda != null) {
            org.teiid.query.sql.symbol.Expression predicate = this.stack.pop();
            Query q = buildSubquery(this.ctxLambda, new Constant(1));
            Criteria crit = null;
            if (predicate instanceof Criteria) {
                crit = (Criteria)predicate;
            } else {
                crit = new ExpressionCriteria(predicate);
            }
            q.setCriteria(Criteria.combineCriteria(q.getCriteria(), crit));
            predicate = new ExistsCriteria(q);
            this.stack.push(predicate);
        }
        this.ctxLambda = null;
    }

    @Override
    public void visit(UriResourceLambdaVariable resource) {
        try {
            if (this.ctxLambda == null) {
                DocumentNode lambda = DocumentNode.build(
                        (EdmEntityType) resource.getType(), null, this.metadata, this.odata,
                        this.nameGenerator, false, this.uriInfo,
                        this.parseService);
                lambda.setGroupSymbol(new GroupSymbol(resource.getVariableName(), lambda.getFullName()));

                this.ctxLambda = lambda;
            }

            this.ctxExpression = ctxLambda;
        } catch (TeiidException e) {
            throw new TeiidRuntimeException(e);
        }
    }

    private Query buildSubquery(DocumentNode eResource,
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
            org.teiid.query.sql.symbol.Expression ex = null;
            if (this.ctxQuery.getIterator() == null) {
                String group = this.nameGenerator.getNextGroup();
                GroupSymbol groupSymbol = new GroupSymbol(group);

                StoredProcedure procedure = new StoredProcedure();
                procedure.setProcedureName("arrayiterate");

                //the projected should only be the collection property at this point
                //we may need more checks here to ensure that is valid
                Collection<ProjectedColumn> values = this.ctxQuery.getProjectedColumns().values();
                Assertion.assertTrue(values.size() == 1);
                ProjectedColumn projectedColumn = values.iterator().next();
                org.teiid.query.sql.symbol.Expression projectedEs = projectedColumn.getExpression();
                List<SPParameter> params = new ArrayList<SPParameter>();
                SPParameter param = new SPParameter(1, SPParameter.IN, "val");
                param.setExpression(projectedEs);
                params.add(param);

                procedure.setParameter(param);

                SubqueryFromClause fromClause = new SubqueryFromClause(group, procedure);
                fromClause.setLateral(true);

                ElementSymbol es = new ElementSymbol("col", groupSymbol);
                String type = ODataTypeManager.teiidType((SingletonPrimitiveType)info.getType(), false);
                Function castFunction = new Function(CAST,new org.teiid.query.sql.symbol.Expression[] {es, new Constant(type)});

                DocumentNode itResource = new DocumentNode();
                org.teiid.query.sql.symbol.Expression clone = (org.teiid.query.sql.symbol.Expression) castFunction.clone();
                AggregateSymbol symbol = new AggregateSymbol(AggregateSymbol.Type.ARRAY_AGG.name(), false, clone);
                AliasSymbol expression = new AliasSymbol(Symbol.getShortName(projectedEs), symbol);

                itResource.setFromClause(fromClause);
                itResource.setGroupSymbol(groupSymbol);
                itResource.addProjectedColumn(expression, info.getType(), projectedColumn.getProperty(), true);

                this.ctxQuery.getProjectedColumns().remove(projectedColumn.getExpression());
                this.ctxQuery.setIterator(itResource);

                ex = castFunction;
            } else {
                GroupSymbol groupSymbol = this.ctxQuery.getIterator().getGroupSymbol();
                ElementSymbol es = new ElementSymbol("col", groupSymbol);
                String type = ODataTypeManager.teiidType((SingletonPrimitiveType)info.getType(), false);
                ex = new Function(CAST,new org.teiid.query.sql.symbol.Expression[] {es, new Constant(type)});
            }

            this.stack.push(ex);
        }
        else {
            boolean ex = true;
            if (this.ctxQuery instanceof ExpandDocumentNode) {
                ExpandDocumentNode node = (ExpandDocumentNode)this.ctxQuery;
                DocumentNode parent = node.getCollectionContext();
                if (parent != null) {
                    this.ctxExpression = parent;
                    ex = false;
                }
            }
            if (ex) {
                throw new TeiidRuntimeException(new TeiidNotImplementedException(
                        ODataPlugin.Event.TEIID16010,
                        ODataPlugin.Util.gs(ODataPlugin.Event.TEIID16010)));
            }
        }
    }

    @Override
    public void visit(UriResourceRoot info) {
        this.root = true;
    }

    @Override
    public void visit(UriResourceEntitySet info) {
        EdmEntityType edmEntityType = info.getEntitySet().getEntityType();
        if (this.root) {
            try {
                this.ctxExpression = DocumentNode.build(edmEntityType,
                        info.getKeyPredicates(), this.metadata, this.odata, this.nameGenerator,
                        true, getUriInfo(), null);
            } catch (TeiidException e) {
                throw new TeiidRuntimeException(e);
            }
        }
        else {
            if (this.ctxQuery.getEdmStructuredType().getFullQualifiedName().equals(edmEntityType.getFullQualifiedName())) {
                this.ctxExpression = this.ctxQuery;
            }
            else {
                for (DocumentNode er: this.ctxQuery.getSiblings()) {
                    if (er.getEdmStructuredType().getFullQualifiedName().equals(edmEntityType.getFullQualifiedName())) {
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
