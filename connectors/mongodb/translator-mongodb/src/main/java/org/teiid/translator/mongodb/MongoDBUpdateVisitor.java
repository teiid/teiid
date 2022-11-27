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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Stack;

import org.teiid.language.*;
import org.teiid.language.AndOr.Operator;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.mongodb.MergeDetails.Association;
import org.teiid.translator.mongodb.MongoDBUpdateExecution.RowInfo;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public class MongoDBUpdateVisitor extends MongoDBSelectVisitor {

    protected LinkedHashMap<String, Object> columnValues = new LinkedHashMap<String, Object>();
    private DB mongoDB;
    private BasicDBObject pull;
    private Condition condition;
    protected Stack<DBObject> onGoingPullCriteria = new Stack<DBObject>();
    protected TranslatorException pullException;

    public MongoDBUpdateVisitor(MongoDBExecutionFactory executionFactory, RuntimeMetadata metadata, DB mongoDB) {
        super(executionFactory, metadata);
        this.mongoDB = mongoDB;
    }

    @Override
    public void visit(Insert obj) {
        append(obj.getTable());

        List<ColumnReference> columns = obj.getColumns();
        List<Expression> values = ((ExpressionValueSource)obj.getValueSource()).getValues();

        try {
            IDRef pk = null;
            for (int i = 0; i < columns.size(); i++) {
                String colName = getColumnName(columns.get(i));
                Expression expr = values.get(i);
                Object value = resolveExpressionValue(colName, expr);

                if (this.mongoDoc.isPartOfPrimaryKey(colName)) {
                    if (pk == null) {
                        pk = new IDRef();
                    }
                    pk.addColumn(colName, value);
                }
                else {
                    this.columnValues.put(colName, value);
                }

                // Update he mongo document to keep track the reference values.
                this.mongoDoc.updateReferenceColumnValue(obj.getTable().getName(), colName, value);

                // if this FK column, replace with reference rather than simple key value
                if (this.mongoDoc.isPartOfForeignKey(colName)) {
                    MergeDetails ref = this.mongoDoc.getFKReference(colName);
                    this.columnValues.put(colName, ref.clone());
                }
            }
            if (pk != null) {
                this.columnValues.put("_id", pk.getValue()); //$NON-NLS-1$
            }
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }
    }

    private Object resolveExpressionValue(String colName, Expression expr) throws TranslatorException {
        Object value = null;
        if (expr instanceof Literal) {
            value = this.executionFactory.convertToMongoType(((Literal) expr).getValue(), this.mongoDB, colName);
        }
        else if (expr instanceof org.teiid.language.Array) {
            org.teiid.language.Array contents = (org.teiid.language.Array)expr;
            List<Expression> arrayExprs = contents.getExpressions();
            value = new BasicDBList();
            for (Expression exp:arrayExprs) {
                if (exp instanceof Literal) {
                    ((BasicDBList)value).add(this.executionFactory.convertToMongoType(((Literal) exp).getValue(), this.mongoDB, colName));
                }
                else {
                    this.exceptions.add(new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18001)));
                }
            }
        }
        else {
            this.exceptions.add(new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18001)));
        }
        return value;
    }

    @Override
    public void visit(Update obj) {
        this.condition = obj.getWhere();
        append(obj.getTable());

        List<SetClause> changes = obj.getChanges();
        try {
            IDRef pk = null;
            for (SetClause clause:changes) {
                String colName = getColumnName(clause.getSymbol());
                // make sure user not updating the linked keys
                if (this.mongoDoc.isMerged()) {
                    if (this.mongoDoc.getMergeKey().getAssociation() == Association.ONE
                            && this.mongoDoc.isPartOfPrimaryKey(colName)) {
                        throw new TranslatorException(MongoDBPlugin.Event.TEIID18035, MongoDBPlugin.Util.gs(
                                MongoDBPlugin.Event.TEIID18035, colName, obj.getTable().getName()));
                    } else if (this.mongoDoc.getMergeKey().getAssociation() == Association.MANY
                            && this.mongoDoc.isPartOfForeignKey(colName)) {
                        throw new TranslatorException(MongoDBPlugin.Event.TEIID18036, MongoDBPlugin.Util.gs(
                                MongoDBPlugin.Event.TEIID18036, colName, obj.getTable().getName()));
                    }
                }
                Expression expr = clause.getValue();
                Object value = resolveExpressionValue(colName, expr);

                if (this.mongoDoc.isPartOfPrimaryKey(colName)) {
                    if (pk == null) {
                        pk = new IDRef();
                    }
                    pk.addColumn(colName, value);
                }
                else {
                    this.columnValues.put(colName, value);
                }

                // Update the mongo document to keep track the reference values.
                this.mongoDoc.updateReferenceColumnValue(obj.getTable().getName(), colName, value);

                // if this FK column, replace with reference rather than simple key value
                if (this.mongoDoc.isPartOfForeignKey(colName)) {
                    MergeDetails ref = this.mongoDoc.getFKReference(colName);
                    this.columnValues.put(colName, ref.clone());
                }
            }
            if (pk != null) {
                this.columnValues.put("_id", pk.getValue()); //$NON-NLS-1$
            }
        } catch (TranslatorException e) {
            this.exceptions.add(e);
        }

        append(obj.getWhere());

        if (!this.onGoingExpression.isEmpty()) {
            this.match = (DBObject)this.onGoingExpression.pop();
        }
    }

    @Override
    public void visit(Delete obj) {
        this.condition = obj.getWhere();
        append(obj.getTable());
        append(obj.getWhere());

        if (!this.onGoingExpression.isEmpty()) {
            this.match = (DBObject)this.onGoingExpression.pop();
        }
    }

    public BasicDBObject getInsert(LinkedHashMap<String, DBObject> embeddedDocuments) {
        BasicDBObject insert = new BasicDBObject();
        for (String key:this.columnValues.keySet()) {
            Object obj = this.columnValues.get(key);

            if (obj instanceof MergeDetails) {
                obj =  ((MergeDetails)obj).getValue();
            }

            if (key.equals("_id")) { //$NON-NLS-1$
                insert.append("_id", obj); //$NON-NLS-1$
            }
            if (!this.mongoDoc.isPartOfPrimaryKey(key)) {
                if (this.mongoDoc.isPartOfForeignKey(key)) {
                    if (obj instanceof BasicDBObject) {
                        insert.append(key, ((BasicDBObject) obj).get(key));
                    }
                    else {
                        insert.append(key, obj);
                    }
                }
                else {
                    insert.append(key, obj);
                }
            }
        }

        if (this.mongoDoc.hasEmbeddedDocuments()) {
            for (String docName:this.mongoDoc.getEmbeddedDocumentNames()) {
                DBObject embedDoc = embeddedDocuments.get(docName);
                if (embedDoc != null) {
                    insert.append(docName, embedDoc);
                }
            }
        }
        return insert;
    }

    public BasicDBObject getUpdate(LinkedHashMap<String, DBObject> embeddedDocuments) throws TranslatorException {
        BasicDBObject update = new BasicDBObject();

        for (String key:this.columnValues.keySet()) {
            Object obj = this.columnValues.get(key);

            if (obj instanceof MergeDetails) {
                MergeDetails ref = ((MergeDetails)obj);

                if (this.mongoDoc.isMerged()) {
                    // do not allow updating the main document reference where this embedded document is embedded.
                    if (ref.getParentTable().equals(this.mongoDoc.getMergeTable().getName())) {
                        throw new TranslatorException(MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18007,
                                ref.getParentTable(), this.mongoDoc.getDocumentName()));
                    }
                }

                //update.append(key, ref.getDBRef(db, true));
                if (this.mongoDoc.isPartOfForeignKey(key)) {
                    if (ref.getValue() instanceof BasicDBObject) {
                        update.append(key, ((BasicDBObject) ref.getValue()).get(key));
                    }
                    else {
                        update.append(key, ref.getValue());
                    }
                }
                else {
                    update.append(key, ref.getValue());
                }

                // also update the embedded document
                if (this.mongoDoc.hasEmbeddedDocuments()) {
                    for (MergeDetails docKey: this.mongoDoc.getEmbeddedReferences()) {
                        if (ref.getParentTable().equals(docKey.getEmbeddedTable())) {
                            DBObject embedDoc = embeddedDocuments.get(docKey.getName());
                            if (embedDoc == null || ref.getValue() == null) {
                                update.append(docKey.getName(), null);
                            }
                            else {
                                update.append(docKey.getName(), embedDoc);
                            }
                        }
                    }
                }
            }
            else {
                if (this.mongoDoc.isMerged()) {
                    if (this.mongoDoc.getMergeAssociation() == Association.MANY) {
                        update.append(this.mongoDoc.getDocumentName()+".$."+key, obj); //$NON-NLS-1$
                    }
                    else {
                        update.append(this.mongoDoc.getDocumentName()+"."+key, obj); //$NON-NLS-1$
                    }
                }
                else {
                    if (this.mongoDoc.isPartOfPrimaryKey(key)) {
                        if (hasCompositePrimaryKey(this.mongoDoc.getTargetTable())) {
                            update.append("_id."+key, obj);//$NON-NLS-1$
                        }
                        else {
                            update.append("_id", obj); //$NON-NLS-1$
                        }
                    }
                    else {
                        update.append(key, obj);
                    }
                }
            }
        }
        return update;
    }

    public BasicDBObject getPullQuery() throws TranslatorException {
        if (this.pullException != null) {
            throw this.pullException;
        }
        if (this.pull == null) {
            if (this.onGoingPullCriteria.isEmpty()) {
                this.pull = new BasicDBObject();
            }
            else {
                this.pull =  new BasicDBObject(this.mongoDoc.getTable().getName(), this.onGoingPullCriteria.pop());
            }
        }
        return this.pull;
    }

    public boolean updateMerge(BasicDBList previousRows, RowInfo parentKey, BasicDBList updated) throws TranslatorException {
        boolean update = false;
        for (int i = 0; i < previousRows.size(); i++) {
            BasicDBObject row = (BasicDBObject)previousRows.get(i);
            if (this.match == null && getPullQuery() == null || ExpressionEvaluator.matches(this.executionFactory, this.mongoDB, this.condition, row, parentKey)) {
                update = true;
                for (String key:this.columnValues.keySet()) {
                    Object obj = this.columnValues.get(key);

                    if (obj instanceof MergeDetails) {
                        MergeDetails ref = ((MergeDetails)obj);
                        row.put(key, ref.getValue());
                    }
                    else {
                        row.put(key, obj);
                    }
                }
            }
            updated.add(row);
        }
        return update;
    }

    public boolean updateDelete(BasicDBList previousRows, RowInfo parentKey, BasicDBList updated) throws TranslatorException {
        for (int i = 0; i < previousRows.size(); i++) {
            BasicDBObject row = (BasicDBObject)previousRows.get(i);
            if (this.match == null && getPullQuery() == null
                    || ExpressionEvaluator.matches(this.executionFactory, this.mongoDB, this.condition, row, parentKey)) {
                //do not add
            }
            else {
                updated.add(row);
            }
        }
        return updated.size() != previousRows.size();
    }

    public boolean updateMerge(BasicDBObject previousRow, RowInfo parentKey) throws TranslatorException {
        boolean update = false;
        if (this.match == null || ExpressionEvaluator.matches(this.executionFactory, this.mongoDB, this.condition, previousRow, parentKey)) {
            for (String key:this.columnValues.keySet()) {
                Object obj = this.columnValues.get(key);

                update = true;

                if (obj instanceof MergeDetails) {
                    MergeDetails ref = ((MergeDetails)obj);
                    previousRow.put(key, ref.getValue());
                }
                else {
                    previousRow.put(key, obj);
                }
            }
        }
        return update;
    }

    @Override
    public void visit(Comparison obj) {
        if (!this.mongoDoc.isMerged() || this.mongoDoc.isMerged() && this.mongoDoc.getMergeAssociation() != Association.MANY) {
            super.visit(obj);
            return;
        }

        // this for the normal where clause
        ColumnDetail leftExpr = getExpressionAlias(obj.getLeftExpression());

        append(obj.getRightExpression());

        Object rightExpr = this.onGoingExpression.pop();
        if (this.expressionMap.get(rightExpr) != null) {
            rightExpr = this.expressionMap.get(rightExpr).getProjectedName();
        }
        // build pull criteria for delete; the pull criteria only applies in merge scenario
        // and only columns in the embedded document.
        boolean buildPullQuery = (includeInPullCriteria(obj.getLeftExpression()) && includeInPullCriteria(obj.getRightExpression()));

        if (!buildPullQuery) {
            QueryBuilder query = leftExpr.getQueryBuilder();
            buildComparisionQuery(obj, rightExpr, query);
            this.onGoingExpression.push(query.get());
        }
        else {
            QueryBuilder pullQuery = leftExpr.getPullQueryBuilder();
            buildComparisionQuery(obj, rightExpr, pullQuery);
            this.onGoingPullCriteria.push(pullQuery.get());
        }

        if (obj.getLeftExpression() instanceof ColumnReference) {
            ColumnReference column = (ColumnReference)obj.getLeftExpression();
            this.mongoDoc.updateReferenceColumnValue(column.getTable().getName(), column.getName(), rightExpr);
        }
    }

    private boolean includeInPullCriteria(Expression expr) {
        if (!this.mongoDoc.isMerged()) {
            return false;
        }
        Collection<ColumnReference> columns = CollectorVisitor.collectElements(expr);
        for (ColumnReference column:columns) {
            if (this.mongoDoc.isPartOfForeignKey(column.getName())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void visit(AndOr obj) {
        if (!this.mongoDoc.isMerged() || this.mongoDoc.isMerged() && this.mongoDoc.getMergeAssociation() != Association.MANY) {
            super.visit(obj);
            return;
        }

        append(obj.getLeftCondition());
        append(obj.getRightCondition());

        boolean valid = false;
        if (this.onGoingExpression.size() >= 2) {
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
            valid = true;
        }

        if (this.onGoingPullCriteria.size() >= 2) {
            DBObject pullRight = this.onGoingPullCriteria.pop();
            DBObject pullLeft = this.onGoingPullCriteria.pop();
            switch(obj.getOperator()) {
            case AND:
                this.onGoingPullCriteria.push(QueryBuilder.start().and(pullLeft, pullRight).get());
                break;
            case OR:
                this.onGoingPullCriteria.push(QueryBuilder.start().or(pullLeft, pullRight).get());
                break;
            }
            valid = true;
        }
        if (!valid && obj.getOperator() == Operator.OR) {
            this.pullException = new TranslatorException(MongoDBPlugin.Event.TEIID18029, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18029));
        }
    }

    @Override
    public void visit(Function obj) {
        if (!this.mongoDoc.isMerged() || this.mongoDoc.isMerged() && this.mongoDoc.getMergeAssociation() != Association.MANY) {
            super.visit(obj);
            return;
        }
        this.pullException = new TranslatorException(MongoDBPlugin.Event.TEIID18028, MongoDBPlugin.Util.gs(MongoDBPlugin.Event.TEIID18028));
    }

    @Override
    public void visit(In obj) {
        if (!this.mongoDoc.isMerged() || this.mongoDoc.isMerged() && this.mongoDoc.getMergeAssociation() != Association.MANY) {
            super.visit(obj);
            return;
        }
        boolean buildPullQuery = includeInPullCriteria(obj.getLeftExpression());
        if (buildPullQuery) {
            ColumnDetail exprAlias = getExpressionAlias(obj.getLeftExpression());
            this.onGoingPullCriteria.push(buildInQuery(obj, exprAlias.getPullQueryBuilder()).get());
        }
        else {
            ColumnDetail exprAlias = getExpressionAlias(obj.getLeftExpression());
            this.onGoingExpression.push(buildInQuery(obj, exprAlias.getQueryBuilder()).get());
        }
    }

    @Override
    public void visit(IsNull obj) {
        if (!this.mongoDoc.isMerged() || this.mongoDoc.isMerged() && this.mongoDoc.getMergeAssociation() != Association.MANY) {
            super.visit(obj);
            return;
        }

        boolean buildPullQuery = includeInPullCriteria(obj.getExpression());
        if (buildPullQuery) {
            ColumnDetail exprAlias = getExpressionAlias(obj.getExpression());
            this.onGoingPullCriteria.push(buildIsNullQuery(obj, exprAlias.getPullQueryBuilder()).get());
        }
        else {
            ColumnDetail exprAlias = getExpressionAlias(obj.getExpression());
            this.onGoingExpression.push(buildIsNullQuery(obj, exprAlias.getQueryBuilder()).get());
        }
    }

    @Override
    public void visit(Like obj) {
        if (!this.mongoDoc.isMerged() || this.mongoDoc.isMerged() && this.mongoDoc.getMergeAssociation() != Association.MANY) {
            super.visit(obj);
            return;
        }

        boolean buildPullQuery = includeInPullCriteria(obj.getLeftExpression());
        if (buildPullQuery) {
            ColumnDetail exprAlias = getExpressionAlias(obj.getLeftExpression());
            this.onGoingPullCriteria.push(buildLikeQuery(obj, exprAlias.getPullQueryBuilder()).get());
        }
        else {
            ColumnDetail exprAlias = getExpressionAlias(obj.getLeftExpression());
            this.onGoingExpression.push(buildLikeQuery(obj, exprAlias.getQueryBuilder()).get());
        }
    }
}
