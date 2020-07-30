package org.teiid.translator.dynamodb.execution;

import org.teiid.language.Delete;
import org.teiid.language.NamedTable;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.Table;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.dynamodb.DynamoDBPlugin;

import java.util.ArrayList;
import java.util.List;

public class DynamoDBDeleteVisitor extends HierarchyVisitor {
    private List<TranslatorException> translatorExceptions = new ArrayList<>();
    private Table table;
    private String criteria;

    public Table getTable() {
        return this.table;
    }

    public String getCriteria() {
        return criteria;
    }

    public DynamoDBDeleteVisitor(Delete delete) {
        visitNode(delete);
    }

    public void checkExceptions() throws TranslatorException {
        if (!this.translatorExceptions.isEmpty()) {
            throw this.translatorExceptions.get(0);
        }
    }

    @Override
    public void visit(NamedTable obj) {
        super.visit(obj);
        this.table = obj.getMetadataObject();
    }

    @Override
    public void visit(Delete obj) {
        if (obj.getParameterValues() != null) {
            this.translatorExceptions.add(new TranslatorException(
                    DynamoDBPlugin.Event.TEIID32001, DynamoDBPlugin.Util.gs(DynamoDBPlugin.Event.TEIID32001)));
        }

        visitNode(obj.getTable());
        if (obj.getWhere() != null) {
            this.criteria = DynamoDBSQLVisitor.getSQLString(obj.getWhere());
        }
    }
}
