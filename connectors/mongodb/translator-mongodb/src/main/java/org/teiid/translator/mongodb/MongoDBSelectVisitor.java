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
package org.teiid.translator.mongodb;

import static org.teiid.language.visitor.SQLStringVisitor.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.bson.types.ObjectId;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.GeometryType;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.basic.ClobToStringTransform;
import org.teiid.language.*;
import org.teiid.language.Join.JoinType;
import org.teiid.language.SortSpecification.Ordering;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.ForeignKey;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.query.function.GeometryHelper;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.mongodb.MergeDetails.Association;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public class MongoDBSelectVisitor extends HierarchyVisitor {
    private AtomicInteger aliasCount = new AtomicInteger();
    private AtomicInteger columnCount = new AtomicInteger();
    protected MongoDBExecutionFactory executionFactory;
    protected RuntimeMetadata metadata;
    private Select command;
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();

    protected Stack<Object> onGoingExpression  = new Stack<Object>();
    protected ConcurrentHashMap<Object, ColumnDetail> expressionMap = new ConcurrentHashMap<Object, ColumnDetail>();
    private HashMap<String, BasicDBObject> groupByProjections = new HashMap<String, BasicDBObject>();
    protected MongoDocument mongoDoc;

    // derived stuff
    protected BasicDBObject project = new BasicDBObject();
    protected Integer limit;
    protected Integer skip;
    protected DBObject sort;
    protected DBObject match;
    protected DBObject having;
    protected BasicDBObject group = new BasicDBObject();
    protected ArrayList<String> selectColumns = new ArrayList<String>();
    protected ArrayList<String> selectColumnReferences = new ArrayList<String>();
    protected boolean projectBeforeMatch = false;
    protected MergePlanner mergePlanner = new MergePlanner();
    protected ArrayList<Condition> pendingConditions = new ArrayList<Condition>();
    protected LinkedList<MongoDocument> joinedDocuments = new LinkedList<MongoDocument>();
    private boolean processingDerivedColumn = false;

    public MongoDBSelectVisitor(MongoDBExecutionFactory executionFactory, RuntimeMetadata metadata) {
        this.executionFactory = executionFactory;
        this.metadata = metadata;
    }

    /**
     * Appends the string form of the LanguageObject to the current buffer.
     * @param obj the language object instance
     */
    public void append(LanguageObject obj) {
        if (obj != null) {
            visitNode(obj);
        }
    }

    /**
     * Simple utility to append a list of language objects to the current buffer
     * by creating a comma-separated list.
     * @param items a list of LanguageObjects
     */
    protected void append(List<? extends LanguageObject> items) {
        if (items != null && items.size() != 0) {
            append(items.get(0));
            for (int i = 1; i < items.size(); i++) {
                append(items.get(i));
            }
        }
    }

    /**
     * Simple utility to append an array of language objects to the current buffer
     * by creating a comma-separated list.
     * @param items an array of LanguageObjects
     */
    protected void append(LanguageObject[] items) {
        if (items != null && items.length != 0) {
            append(items[0]);
            for (int i = 1; i < items.length; i++) {
                append(items[i]);
            }
        }
    }

    public String getColumnName(ColumnReference obj) {
        String elemShortName = null;
        AbstractMetadataRecord elementID = obj.getMetadataObject();
        if(elementID != null) {
            elemShortName = getRecordName(elementID);
        } else {
            elemShortName = obj.getName();
        }
        return elemShortName;
    }

    @Override
    public void visit(DerivedColumn obj) {
        Expression teiidExpression = obj.getExpression();
        String alias = getAlias(obj.getAlias());

        this.processingDerivedColumn = true;
        append(teiidExpression);

        Object mongoExpression = this.onGoingExpression.pop();

        ColumnDetail exprDetails = this.expressionMap.get(mongoExpression);
        if (exprDetails == null) {
            exprDetails = new ColumnDetail();
            exprDetails.addProjectedName(alias);
            this.expressionMap.put(mongoExpression, exprDetails);
        } else if (projectBeforeMatch || teiidExpression instanceof AggregateFunction) {
            alias = exprDetails.getProjectedName();
        }

        // the the expression is already part of group by then the projection should be $_id.{name}
        this.selectColumns.add(alias);
        if (exprDetails.partOfGroupBy) {
            BasicDBObject id = this.groupByProjections.get("_id"); //$NON-NLS-1$
            this.project.append(alias, id.get(exprDetails.getProjectedName()));
            exprDetails.addProjectedName(alias);
            this.selectColumnReferences.add(alias);
        }
        else {
            exprDetails.addProjectedName(alias);
            exprDetails.partOfProject = true;
            if (teiidExpression instanceof ColumnReference) {
                String elementName = getColumnName((ColumnReference)obj.getExpression());
                this.selectColumnReferences.add(elementName);
                // the the expression is already part of group by then the projection should be $_id.{name}
                if (this.command.isDistinct() || this.groupByProjections.get(alias) != null) {
                    // this is DISTINCT case
                    this.project.append(alias, "$_id."+alias); //$NON-NLS-1$
                    // if group by does not exist then build the group root id based on distinct
                    this.group.put(alias, mongoExpression);
                }
                else {
                    this.project.append(alias, mongoExpression);
                }
            }
            else {
                implicitProject(teiidExpression, mongoExpression, exprDetails, alias);
                // what user sees as project
                this.selectColumnReferences.add(alias);
            }
        }
        this.processingDerivedColumn = false;
    }

    private String getAlias(String alias) {
        if (alias == null) {
            return "_m"+this.aliasCount.getAndIncrement(); //$NON-NLS-1$
        }
        return alias;
    }

    private ColumnDetail buildAlias() {
        String alias = getAlias(null);
        ColumnDetail detail =  new ColumnDetail();
        detail.addProjectedName(alias);
        return detail;
    }

    @Override
    public void visit(ColumnReference obj) {
        try {
            if (obj.getMetadataObject() == null) {
                for (Object expr:this.expressionMap.keySet()) {
                    ColumnDetail columnInfo = this.expressionMap.get(expr);
                    if (columnInfo.hasProjectedName(getColumnName(obj))) {
                        this.onGoingExpression.push(expr);
                        break;
                    }
                }
            }
            else {
                // do not allow array type in where clauses etc.
                /*
                if (!this.processingDerivedColumn) {
                    if (DataTypeManager.isArrayType(obj.getMetadataObject().getRuntimeType())){
                        this.exceptions.add(new TranslatorException(MongoDBPlugin.Event.TEIID18027, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18027, getColumnName(obj))));
                    }
                }
                */

                ColumnDetail columnInfo = buildColumnDetail(obj);
                Object mongoExpr = columnInfo.expression;
                this.onGoingExpression.push(mongoExpr);
                this.expressionMap.putIfAbsent(mongoExpr, columnInfo);
            }
        } catch (TranslatorException e) {
            this.exceptions.add(e);
            return;
        }
    }

    ColumnDetail buildColumnDetail(ColumnReference obj) throws TranslatorException {
        MongoDocument columnDocument = getDocument(obj.getTable().getMetadataObject());
        MongoDocument targetDocument = this.mongoDoc.getTargetDocument();

        String columnName = obj.getMetadataObject().getName();
        String documentFieldName = obj.getMetadataObject().getName();

        // column is on the same collection
        if (columnDocument.equals(targetDocument)) {
            documentFieldName = columnDocument.getColumnName(columnName);
        }
        else if (targetDocument.embeds(columnDocument)){
            // if this is embeddable table, then we need to use the embedded collection name
            MergeDetails ref = targetDocument.getEmbeddedDocumentReferenceKey(columnDocument);
            String parentColumnName = ref.getParentColumnName(columnName);
            if (parentColumnName != null) {
                while(ref.isNested()) {
                    columnDocument = ref.getDocument();
                    ref = targetDocument.getEmbeddedDocumentReferenceKey(columnDocument);
                    parentColumnName = ref.getParentColumnName(parentColumnName);
                }
                documentFieldName = targetDocument.getColumnName(parentColumnName);
            }
            else {
                documentFieldName = columnDocument.getDocumentName() + "." + columnDocument.getColumnName(columnName); //$NON-NLS-1$
            }
        }
        else if (targetDocument.merges(columnDocument)){
            documentFieldName = columnDocument.getColumnName(columnName);
        }

        ColumnDetail detail = new ColumnDetail();
        detail.addProjectedName(documentFieldName);
        detail.documentFieldName = documentFieldName;
        detail.expression = "$"+documentFieldName; //$NON-NLS-1$
        return detail;
    }

    private MongoDocument getDocument(Table table) {
        if (this.mongoDoc != null && this.mongoDoc.getTable().getName().equals(table.getName())) {
            return this.mongoDoc;
        }
        for (MongoDocument doc:this.joinedDocuments) {
            if (doc.getTable().getName().equals(table.getName())) {
                return doc;
            }
        }
        return null;
    }

    @Override
    public void visit(AggregateFunction obj) {
        if (!obj.getParameters().isEmpty()) {
            append(obj.getParameters());
        }

        BasicDBObject expr = null;
        if (obj.getName().equals(AggregateFunction.COUNT)) {
            // this is only true for count(*) case, so we need implicit group id clause
            try {
                Object param = this.onGoingExpression.pop();
                BasicDBList eq = new BasicDBList();
                eq.add(0, param);
                eq.add(1, null);
                BasicDBList values = new BasicDBList();
                values.add(0, new BasicDBObject("$eq", eq)); //$NON-NLS-1$
                values.add(1, 0);
                values.add(2, 1);
                expr = new BasicDBObject("$sum",new BasicDBObject("$cond", values)); //$NON-NLS-1$ //$NON-NLS-2$
            } catch (EmptyStackException e) {
                this.group.put("_id", null); //$NON-NLS-1$
                expr = new BasicDBObject("$sum", new Integer(1)); //$NON-NLS-1$
            }
        }
        else if (obj.getName().equals(AggregateFunction.AVG)) {
            expr = new BasicDBObject("$avg", this.onGoingExpression.pop()); //$NON-NLS-1$
        }
        else if (obj.getName().equals(AggregateFunction.SUM)) {
            expr = new BasicDBObject("$sum", this.onGoingExpression.pop()); //$NON-NLS-1$
        }
        else if (obj.getName().equals(AggregateFunction.MIN)) {
            expr = new BasicDBObject("$min", this.onGoingExpression.pop()); //$NON-NLS-1$
        }
        else if (obj.getName().equals(AggregateFunction.MAX)) {
            expr = new BasicDBObject("$max", this.onGoingExpression.pop()); //$NON-NLS-1$
        }
        else {
            this.exceptions.add(new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18005, obj.getName())));
        }

        if (expr != null) {
            this.onGoingExpression.push(expr);
        }
    }

    private ColumnDetail addToProject(Object expr, boolean addExprAsProject, ColumnDetail detail, boolean needsProjection, String projectedName) {
        if (detail == null) {
            // if expression is in having/where clause there is will be no alias; however mongo expects some functions
            // to be elevated to project before $match can be run
            if (needsProjection) {
                this.projectBeforeMatch = true;
            }
            detail = buildAlias();
            this.expressionMap.putIfAbsent(expr, detail);
            projectedName = detail.getProjectedName();
        }
        detail.expression = expr;

        if (needsProjection) {
            if (this.project.get(projectedName) == null && !this.project.values().contains(expr)) {
                this.project.append(projectedName, addExprAsProject?expr:1);
            }
            detail.partOfProject = true;
        }
        else {
            detail.partOfProject = false;
        }
        return detail;
    }

    @Override
    public void visit(Function obj) {
        String functionName = obj.getName();
        if (functionName.indexOf('.') != -1) {
            functionName = functionName.substring(functionName.indexOf('.')+1);
        }
        if (this.executionFactory.getFunctionModifiers().containsKey(functionName)) {
            List<?> parts =  this.executionFactory.getFunctionModifiers().get(functionName).translate(obj);
            if (parts != null) {
                obj = (Function)parts.get(0);
                if (parts.size() > 1) {
                    throw new AssertionError("Not supported"); //$NON-NLS-1$
                }
            }
        }
        BasicDBObject expr = null;
        if (isGeoSpatialFunction(functionName)) {
            try {
                expr = (BasicDBObject)handleGeoSpatialFunction(functionName, obj);
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
        else if (isStringFunction(functionName)) {
            expr = handleStringFunction(functionName, obj);
        }
        else {
            List<Expression> args = obj.getParameters();
            if (args != null) {
                BasicDBList params = new BasicDBList();
                for (int i = 0; i < args.size(); i++) {
                    append(args.get(i));
                    Object param = this.onGoingExpression.pop();
                    params.add(param);
                }
                expr = new BasicDBObject(obj.getName(), params);
            }
        }

        if(expr != null) {
            //functions over dates do not work if the date is null/missing
            if (obj.getParameters().size() == 1
                    && Date.class.isAssignableFrom(obj.getParameters().get(0).getType())
                    && isDateTimeFunction(functionName) ) {
                BasicDBList newParams = new BasicDBList();
                newParams.addAll((BasicDBList)expr.values().iterator().next());
                newParams.add(false);
                BasicDBObject nullCheck = new BasicDBObject("$ifNull", newParams);
                newParams = new BasicDBList();
                newParams.add(nullCheck);
                newParams.add(expr);
                newParams.add(null);
                expr = new BasicDBObject("$cond", newParams);
            }

            this.onGoingExpression.push(expr);
        }
    }

    private boolean isStringFunction(String functionName) {
        if (functionName.equalsIgnoreCase("UCASE")
                || functionName.equalsIgnoreCase("LCASE")
                || functionName.equalsIgnoreCase("SUBSTRING")) {
            return true;
        }
        return false;
    }

    private boolean isDateTimeFunction(String functionName) {
        if (functionName.equalsIgnoreCase(SourceSystemFunctions.DAYOFYEAR)
                || functionName.equalsIgnoreCase(SourceSystemFunctions.DAYOFMONTH)
                || functionName.equalsIgnoreCase(SourceSystemFunctions.DAYOFWEEK)
                || functionName.equalsIgnoreCase(SourceSystemFunctions.YEAR)
                || functionName.equalsIgnoreCase(SourceSystemFunctions.MONTH)
                || functionName.equalsIgnoreCase(SourceSystemFunctions.WEEK)
                || functionName.equalsIgnoreCase(SourceSystemFunctions.HOUR)
                || functionName.equalsIgnoreCase(SourceSystemFunctions.MINUTE)
                || functionName.equalsIgnoreCase(SourceSystemFunctions.SECOND)) {
            return true;
        }
        return false;
    }

    private BasicDBObject handleStringFunction(String functionName, Function function) {
        List<Expression> args = function.getParameters();
        BasicDBObject func = null;

        append(args.get(0));
        Object column = this.onGoingExpression.pop();
        if (args.size() == 1) {
            func = new BasicDBObject(function.getName(), column);
        }
        else {
            BasicDBList params = new BasicDBList();
            params.add(column);
            for (int i = 1; i < args.size(); i++) {
                append(args.get(i));
                Object param = this.onGoingExpression.pop();
                params.add(param);
            }
            func = new BasicDBObject(function.getName(), params);
        }
        BasicDBObject ne = buildNE(column.toString(), null);
        return buildCondition(ne, func, null);
    }

    private BasicDBObject buildCondition(Object expr, Object trueExpr, Object falseExpr) {
        BasicDBList values = new BasicDBList();
        values.add(0, expr);
        values.add(1, trueExpr);
        values.add(2, falseExpr);
        return new BasicDBObject("$cond", values);
    }

    private BasicDBObject buildNE(Object leftExpr, Object rightExpr) {
        BasicDBList values = new BasicDBList();
        values.add(0, leftExpr);
        values.add(1, rightExpr);
        return new BasicDBObject("$ne", values);
    }

    private boolean isGeoSpatialFunction(String name) {
        for (String func:MongoDBExecutionFactory.GEOSPATIAL_FUNCTIONS) {
            if (name.equalsIgnoreCase(func)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void visit(NamedTable obj) {
        try {
            this.mongoDoc = new MongoDocument(obj.getMetadataObject(), this.metadata);
            configureUnwind(this.mongoDoc);
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
    }


    @Override
    public void visit(Join obj) {
        try {
            if (obj.getLeftItem() instanceof Join) {
                append(obj.getLeftItem());
                Table right = ((NamedTable)obj.getRightItem()).getMetadataObject();
                processJoin(this.mongoDoc, new MongoDocument(right, this.metadata), obj.getCondition(), obj.getJoinType());
            }
            else if (obj.getRightItem() instanceof Join) {
                Table left = ((NamedTable)obj.getLeftItem()).getMetadataObject();
                append(obj.getRightItem());
                processJoin(this.mongoDoc, new MongoDocument(left, this.metadata), obj.getCondition(), obj.getJoinType());
            }
            else {
                Table left = ((NamedTable)obj.getLeftItem()).getMetadataObject();
                Table right = ((NamedTable)obj.getRightItem()).getMetadataObject();
                processJoin(new MongoDocument(left, this.metadata), new MongoDocument(right, this.metadata), obj.getCondition(), obj.getJoinType());
            }
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
    }

    private void configureUnwind(MongoDocument document) throws TranslatorException {
        if (document.isMerged()) {

            // if nested document
            MongoDocument mergeDocument = document.getMergeDocument();
            if (mergeDocument.isMerged()) {
                configureUnwind(mergeDocument);
            }

            if (document.getMergeAssociation() == Association.MANY) {
                this.mergePlanner.addNode(new UnwindNode(document));
            }
            else {
                this.mergePlanner.addNode(new ExistsNode(document));
            }
        }
    }

    private void processJoin(MongoDocument left, MongoDocument right, Condition cond, JoinType joinType) throws TranslatorException {
        // now adjust for the left/right outer depending upon who is the outer document
        JoinCriteriaVisitor jcv = new JoinCriteriaVisitor(joinType, left, right, this.mergePlanner);
        jcv.process(cond);

        if (left.contains(right)) {
            this.mongoDoc = left;
            this.joinedDocuments.add(right);
            configureUnwind(right);
        }
        else if (right.contains(left)) {
            this.mongoDoc = right;
            this.joinedDocuments.add(left);
            configureUnwind(left);
        }
        else {
            if (this.mongoDoc != null) {
                // this is for nested grand kids
                for (MongoDocument child:this.joinedDocuments) {
                    if (child.contains(right)) {
                        this.joinedDocuments.add(right);
                        configureUnwind(right);
                        return;
                    }
                }
            }
            throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18012, left.getTable().getName(), right.getTable().getName()));
        }

        if (cond != null) {
            this.pendingConditions.add(cond);
        }
    }

    @Override
    public void visit(Select obj) {

        this.command = obj;

        if (obj.getFrom() != null && !obj.getFrom().isEmpty()) {
            append(obj.getFrom());
        }

        if (!this.exceptions.isEmpty()) {
            return;
        }

        append(obj.getWhere());

        if (!this.onGoingExpression.isEmpty()) {
            if (this.match != null) {
                DBObject expr = (DBObject)this.onGoingExpression.pop();
                ArrayList exprs = (ArrayList)expr.get("$and"); //$NON-NLS-1$
                if (exprs != null) {
                    exprs.add(0, this.match);
                    this.match = expr;
                }
                else {
                    this.match = QueryBuilder.start().and(this.match, expr).get();
                }
            }
            else {
                this.match = (DBObject)this.onGoingExpression.pop();
            }
        }
        else {
            // default match in case no where clause used
            // TEIID-2841 - in ONE-2-ONE case $unwind works as filter
        }

        append(obj.getGroupBy());

        append(obj.getHaving());

        if (!this.onGoingExpression.isEmpty()) {
            this.having = (DBObject)this.onGoingExpression.pop();
        }

        append(obj.getDerivedColumns());

        // in distinct since there may not be group by, but mongo requires a grouping clause.
        if (obj.getGroupBy() == null && obj.isDistinct() && !this.group.containsField("_id")) { //$NON-NLS-1$
            BasicDBObject id = new BasicDBObject(this.group);
            this.group.clear();
            this.group.put("_id", id); //$NON-NLS-1$
        }

        if (!this.group.isEmpty()) {
            if (this.group.get("_id") == null) { //$NON-NLS-1$
                this.group.put("_id", null); //$NON-NLS-1$
            }
        }
        else {
            this.group = null;
        }

        append(obj.getOrderBy());

        append(obj.getLimit());
    }

    private ColumnDetail implicitProject(Expression teiidExpr, Object mongoExpr, ColumnDetail columnDetails, String projectedName) {
        if (teiidExpr instanceof ColumnReference) {
            return this.expressionMap.get(mongoExpr);
        }
        else if (teiidExpr instanceof AggregateFunction) {
            boolean saved = this.projectBeforeMatch;
            ColumnDetail alias = addToProject(mongoExpr, false, columnDetails, true, projectedName);
            //don't toggle for an aggregate function as it is projected by the group operation
            this.projectBeforeMatch = saved;
            if (projectedName == null) {
                projectedName = alias.getProjectedName();
            }
            if (!this.group.values().contains(mongoExpr)) {
                this.group.put(projectedName, mongoExpr);
            }
            return alias;
        }
        else if (teiidExpr instanceof Function) {
            Boolean avoidProjection = Boolean.valueOf(((Function) teiidExpr).getMetadataObject().getProperty(MongoDBExecutionFactory.AVOID_PROJECTION, false));
            return addToProject(mongoExpr, true, columnDetails, processingDerivedColumn||!avoidProjection, projectedName);
        }
        else if (teiidExpr instanceof Condition) {
            // needs to be in the form "_mo: {$cond: [{$eq :["$city", "FREEDOM"]}, true, false]}}}"
            BasicDBList values = new BasicDBList();
            values.add(0, mongoExpr);
            values.add(1, true);
            values.add(2, false);
            return addToProject(new BasicDBObject("$cond", values), true, columnDetails, true, projectedName); //$NON-NLS-1$
        }
        else if (teiidExpr instanceof Literal) {
            if (this.executionFactory.getVersion().compareTo(MongoDBExecutionFactory.TWO_6) >= 0) {
                return addToProject(new BasicDBObject("$literal", mongoExpr), true, columnDetails, true, projectedName); //$NON-NLS-1$
            }
            this.exceptions.add(new TranslatorException(MongoDBPlugin.Event.TEIID18026, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18026)));
        }
        return null;
    }

    @Override
    public void visit(Comparison obj) {

        // this for $cond in the select statement, and formatting of command for $cond vs $match is different
        if (this.processingDerivedColumn) {
            visitDerivedExpression(obj);
            return;
        }

        // this for the normal where clause
        ColumnDetail leftExprDetails = getExpressionAlias(obj.getLeftExpression());
        append(obj.getRightExpression());
        Object rightExpr = this.onGoingExpression.pop();
        if (this.expressionMap.get(rightExpr) != null) {
            rightExpr = this.expressionMap.get(rightExpr).getProjectedName();
        }

        QueryBuilder query = leftExprDetails.getQueryBuilder();
        rightExpr = checkAndConvertToObjectId(obj.getLeftExpression(), obj.getRightExpression(), rightExpr);
        buildComparisionQuery(obj, rightExpr, query);

        if (leftExprDetails.partOfProject || obj.getLeftExpression() instanceof ColumnReference) {
            this.onGoingExpression.push(query.get());
        }
        else {
            this.onGoingExpression.push(buildFunctionQuery(obj, (BasicDBObject)leftExprDetails.expression, rightExpr));
        }

        if (obj.getLeftExpression() instanceof ColumnReference) {
            ColumnReference column = (ColumnReference)obj.getLeftExpression();
            this.mongoDoc.updateReferenceColumnValue(column.getTable().getName(), column.getName(), rightExpr);
        }
    }

    private Object checkAndConvertToObjectId(Expression left, Expression right, Object rightValue) {
        if (left instanceof ColumnReference && right instanceof Literal) {
            String navtiveType = ((ColumnReference)left).getMetadataObject().getNativeType();
            if (navtiveType != null && navtiveType.equals(ObjectId.class.getName())) {
                return new ObjectId((String)rightValue);
            }
        }
        return rightValue;
    }

    protected BasicDBObject buildFunctionQuery(Comparison obj, BasicDBObject leftExpr, Object rightExpr) {
        switch(obj.getOperator()) {
        case EQ:
            if (rightExpr instanceof Boolean && ((Boolean)rightExpr)) {
                return leftExpr;
            }
            //$FALL-THROUGH$
        case NE:
        case LT:
        case LE:
        case GT:
        case GE:
        }
        this.exceptions.add(new TranslatorException(MongoDBPlugin.Event.TEIID18030, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18030)));
        return null;
    }

    protected void buildComparisionQuery(Comparison obj, Object rightExpr, QueryBuilder query) {
        switch(obj.getOperator()) {
        case EQ:
            query.is(rightExpr);
            break;
        case NE:
            query.notEquals(rightExpr);
            break;
        case LT:
            query.lessThan(rightExpr);
            break;
        case LE:
            query.lessThanEquals(rightExpr);
            break;
        case GT:
            query.greaterThan(rightExpr);
            break;
        case GE:
            query.greaterThanEquals(rightExpr);
            break;
        }
    }

    private void visitDerivedExpression(Comparison obj) {
        append(obj.getLeftExpression());
        Object leftExpr = this.onGoingExpression.pop();
        append(obj.getRightExpression());
        Object rightExpr = this.onGoingExpression.pop();

        BasicDBList values = new BasicDBList();
        values.add(0, leftExpr);
        values.add(1, rightExpr);

        switch(obj.getOperator()) {
        case EQ:
            this.onGoingExpression.push(new BasicDBObject("$eq", values)); //$NON-NLS-1$
            break;
        case NE:
            this.onGoingExpression.push(new BasicDBObject("$ne", values)); //$NON-NLS-1$
            break;
        case LT:
            this.onGoingExpression.push(new BasicDBObject("$lt", values)); //$NON-NLS-1$
            break;
        case LE:
            this.onGoingExpression.push(new BasicDBObject("$lte", values)); //$NON-NLS-1$
            break;
        case GT:
            this.onGoingExpression.push(new BasicDBObject("$gt", values)); //$NON-NLS-1$
            break;
        case GE:
            this.onGoingExpression.push(new BasicDBObject("$gte", values)); //$NON-NLS-1$
            break;
        }
    }

    @Override
    public void visit(AndOr obj) {
        append(obj.getLeftCondition());
        append(obj.getRightCondition());
        DBObject right = (DBObject)this.onGoingExpression.pop();
        DBObject left = (DBObject) this.onGoingExpression.pop();

        switch(obj.getOperator()) {
        case AND:
            this.onGoingExpression.push(QueryBuilder.start().and(left, right).get());
            break;
        case OR:
            this.onGoingExpression.push(QueryBuilder.start().or(left, right).get());
            break;
        }
    }

    @Override
    public void visit(Array array) {
        append(array.getExpressions());
        BasicDBList values = new BasicDBList();
        for (int i = 0; i < array.getExpressions().size(); i++) {
            values.add(0, this.onGoingExpression.pop());
        }
        this.onGoingExpression.push(values);
    }

    @Override
    public void visit(Literal obj) {
        try {
            this.onGoingExpression.push(this.executionFactory.convertToMongoType(obj.getValue(), null, null));
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
    }

    @Override
    public void visit(In obj) {
        append(obj.getLeftExpression());
        Object expr = this.onGoingExpression.pop();
        ColumnDetail detail = this.expressionMap.get(expr);
        QueryBuilder query = QueryBuilder.start();
        if (detail == null) {
            this.exceptions.add(new TranslatorException(MongoDBPlugin.Event.TEIID18031, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18031)));
        }
        else {
            query = detail.getQueryBuilder();
            this.onGoingExpression.push(buildInQuery(obj, query).get());
        }
    }

    protected QueryBuilder buildInQuery(In obj, QueryBuilder query) {
        append(obj.getRightExpressions());
        BasicDBList values = new BasicDBList();
        for (int i = 0; i < obj.getRightExpressions().size(); i++) {
            Object rightExpr = this.onGoingExpression.pop();
            rightExpr = checkAndConvertToObjectId(obj.getLeftExpression(), obj.getRightExpressions().get(i), rightExpr);
            values.add(0, rightExpr);
        }
        if (obj.isNegated()) {
            query.notIn(values);
        } else {
            query.in(values);
        }
        return query;
    }

    ColumnDetail getExpressionAlias(Expression obj) {
        // the way DBRef names handled in projection vs selection is different.
        // in projection we want to see as "col" mapped to "col.$_id" as it is treated as sub-document
        // where as in selection it will should be "col._id".
        append(obj);

        Object expr = this.onGoingExpression.pop();
        ColumnDetail detail = this.expressionMap.get(expr);
        detail = implicitProject(obj, expr, detail, detail != null ? detail.getProjectedName():null);

        // when expression shows up in a condition, but it is not a derived column
        // then add implicit project on that alias.
        return detail;
    }

    @Override
    public void visit(IsNull obj) {
        append(obj.getExpression());
        Object expr = this.onGoingExpression.pop();
        ColumnDetail detail = this.expressionMap.get(expr);
        QueryBuilder query = QueryBuilder.start();
        if (detail == null) {
            this.exceptions.add(new TranslatorException(MongoDBPlugin.Event.TEIID18032, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18032)));
        }
        else {
            query = detail.getQueryBuilder();
            this.onGoingExpression.push(buildIsNullQuery(obj, query).get());
        }
    }

    protected QueryBuilder buildIsNullQuery(IsNull obj, QueryBuilder query) {
        if (obj.isNegated()) {
            query.notEquals(null);
        }
        else {
            query.is(null);
        }
        return query;
    }

    @Override
    public void visit(Like obj) {
        append(obj.getLeftExpression());
        Object expr = this.onGoingExpression.pop();
        ColumnDetail detail = this.expressionMap.get(expr);
        QueryBuilder query = QueryBuilder.start();
        if (detail == null) {
            this.exceptions.add(new TranslatorException(MongoDBPlugin.Event.TEIID18033, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18033)));
        }
        else {
            query = detail.getQueryBuilder();
            buildLikeQuery(obj, query);
            this.onGoingExpression.push(query.get());
        }
    }

    protected QueryBuilder buildLikeQuery(Like obj, QueryBuilder query) {
        if (obj.isNegated()) {
            query.not();
        }

        append(obj.getRightExpression());

        StringBuilder value = new StringBuilder((String)this.onGoingExpression.pop());
        int idx = -1;
        while (true) {
            idx = value.indexOf("%", idx+1);//$NON-NLS-1$
            if (idx != -1 && idx == 0) {
                continue;
            }
            if (idx != -1 && idx == value.length()-1) {
                continue;
            }

            if (idx == -1) {
                break;
            }
            value.replace(idx, idx+1, ".*"); //$NON-NLS-1$
        }

        if (value.charAt(0) != '%') {
            value.insert(0, '^');
        }

        idx = value.length();
        if (value.charAt(idx-1) != '%') {
            value.insert(idx, '$');
        }

        String regex = value.toString().replaceAll("%", ""); //$NON-NLS-1$ //$NON-NLS-2$
        query.is(Pattern.compile(regex));
        return query;
    }

    @Override
    public void visit(Limit obj) {
        if (obj.getRowLimit() != Integer.MAX_VALUE) {
            this.limit = new Integer(obj.getRowLimit());
        }
        this.skip = new Integer(obj.getRowOffset());
    }

    @Override
    public void visit(OrderBy obj) {
        append(obj.getSortSpecifications());
    }

    @Override
    public void visit(SortSpecification obj) {
        append(obj.getExpression());
        Object expr = this.onGoingExpression.pop();
        ColumnDetail alias = this.expressionMap.get(expr);
        if (this.sort == null) {
            this.sort =  new BasicDBObject(alias.getProjectedName(), (obj.getOrdering() == Ordering.ASC)?1:-1);
        }
        else {
            this.sort.put(alias.getProjectedName(), (obj.getOrdering() == Ordering.ASC)?1:-1);
        }
    }

    @Override
    public void visit(GroupBy obj) {
        // since grouping requires additional step, this is done at a different pipeline stage.
        // so, that requires additional in-direction.
        if (obj.getElements().size() == 1) {
            append(obj.getElements().get(0));
            Object mongoExpr = this.onGoingExpression.pop();
            ColumnDetail exprDetails = this.expressionMap.get(mongoExpr);
            String projectedName = "_c"+this.columnCount.getAndIncrement(); //$NON-NLS-1$
            exprDetails.addProjectedName(projectedName);
            this.group.put("_id", new BasicDBObject(projectedName, mongoExpr)); //$NON-NLS-1$
            this.groupByProjections.put("_id", new BasicDBObject(projectedName, "$_id."+projectedName)); //$NON-NLS-1$ //$NON-NLS-2$
            exprDetails.partOfGroupBy = true;
        }
        else {
            BasicDBObject fields = new BasicDBObject();
            BasicDBObject exprs = new BasicDBObject();
            for (Expression expr : obj.getElements()) {
                append(expr);
                Object mongoExpr = this.onGoingExpression.pop();
                ColumnDetail exprDetails = this.expressionMap.get(mongoExpr);
                String projectedName = "_c"+this.columnCount.getAndIncrement(); //$NON-NLS-1$
                exprDetails.addProjectedName(projectedName);
                exprs.put(projectedName, mongoExpr);
                fields.put(projectedName, "$_id."+projectedName); //$NON-NLS-1$
                exprDetails.partOfGroupBy = true;
            }
            this.group.put("_id", exprs); //$NON-NLS-1$
            this.groupByProjections.put("_id", fields); //$NON-NLS-1$
        }
    }

    static boolean isPartOfPrimaryKey(Table table, String columnName) {
        KeyRecord pk = table.getPrimaryKey();
        if (pk != null) {
            for (Column column:pk.getColumns()) {
                if (getRecordName(column).equals(columnName)) {
                    return true;
                }
            }
        }
        return false;
    }

    boolean hasCompositePrimaryKey(Table table) {
        KeyRecord pk = table.getPrimaryKey();
        return pk.getColumns().size() > 1;
    }

    static boolean isPartOfForeignKey(Table table, String columnName) {
        for (ForeignKey fk : table.getForeignKeys()) {
            for (Column column : fk.getColumns()) {
                if (column.getName().equals(columnName)) {
                    return true;
                }
            }
        }
        return false;
    }

    static String getForeignKeyRefTable(Table table, String columnName) {
        for (ForeignKey fk : table.getForeignKeys()) {
            for (Column column : fk.getColumns()) {
                if (column.getName().equals(columnName)) {
                    return fk.getReferenceTableName();
                }
            }
        }
        return null;
    }

    static List<String> getColumnNames(List<Column> columns){
        ArrayList<String> names = new ArrayList<String>();
        for (Column c:columns) {
            names.add(c.getName());
        }
        return names;
    }

    static enum SpatialType {Point, LineString, Polygon, MultiPoint, MultiLineString};

    private DBObject handleGeoSpatialFunction(String functionName, Function function) throws TranslatorException{
        if (functionName.equalsIgnoreCase(MongoDBExecutionFactory.FUNC_GEO_NEAR) ||
                functionName.equalsIgnoreCase(MongoDBExecutionFactory.FUNC_GEO_NEAR_SPHERE)) {
            return buildGeoNearFunction(function);
        }
        return buildGeoFunction(function);
    }

    private DBObject buildGeoNearFunction(Function function) throws TranslatorException {
        List<Expression> args = function.getParameters();

        // Column Name
        int paramIndex = 0;
        ColumnDetail column = getExpressionAlias(args.get(paramIndex++));

        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.push(column.documentFieldName);
        builder.push(function.getName());

        append(args.get(paramIndex++));
        Object object = this.onGoingExpression.pop();
        if (object instanceof GeometryType) {
            convertGeometryToJson(builder, (GeometryType)object);
        } else {
            builder.push("$geometry");//$NON-NLS-1$
            builder.add("type", SpatialType.Point.name());//$NON-NLS-1$

            // walk the co-ordinates
            BasicDBList coordinates = new BasicDBList();
            coordinates.add(object);
            builder.add("coordinates", coordinates); //$NON-NLS-1$
        }

        // maxdistance
        append(args.get(paramIndex++));
        BasicDBObjectBuilder b= builder.pop();
        b.add("$maxDistance", this.onGoingExpression.pop()); //$NON-NLS-1$

        if (this.executionFactory.getVersion().compareTo(MongoDBExecutionFactory.TWO_6) >= 0) {
            // mindistance
            append(args.get(paramIndex++));
            b.add("$minDistance", this.onGoingExpression.pop()); //$NON-NLS-1$
        }
        return builder.get();
    }

    private DBObject buildGeoFunction(Function function) throws TranslatorException{
        List<Expression> args = function.getParameters();

        // Column Name
        int paramIndex = 0;
        ColumnDetail column = getExpressionAlias(args.get(paramIndex++));

        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        builder.push(column.documentFieldName);
        builder.push(function.getName());

        append(args.get(paramIndex++));
        Object object = this.onGoingExpression.pop();
        if (object instanceof GeometryType) {
            convertGeometryToJson(builder, (GeometryType)object);
        } else {
            // Type: Point, LineString, Polygon..
            SpatialType type = SpatialType.valueOf((String)object);

            append(args.get(paramIndex++));
            builder.push("$geometry");//$NON-NLS-1$
            builder.add("type", type.name());//$NON-NLS-1$
            // walk the co-ordinates
            BasicDBList coordinates = new BasicDBList();
            coordinates.add(this.onGoingExpression.pop());
            builder.add("coordinates", coordinates); //$NON-NLS-1$
        }
        return builder.get();
    }

    private void convertGeometryToJson(BasicDBObjectBuilder builder, GeometryType object) throws TranslatorException {
        try {
            ClobType clob = GeometryHelper.getInstance().geometryToGeoJson(object);
            ClobToStringTransform clob2str = new ClobToStringTransform();
            String geometry = (String)clob2str.transform(clob, String.class);
            builder.add("$geometry", geometry);
        } catch (FunctionExecutionException | TransformationException e) {
            throw new TranslatorException(e);
        }
    }
}
