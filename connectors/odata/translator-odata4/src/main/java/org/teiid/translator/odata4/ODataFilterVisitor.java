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
package org.teiid.translator.odata4;

import static org.teiid.language.SQLConstants.Reserved.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.teiid.language.*;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.metadata.BaseColumn;
import org.teiid.metadata.Column;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.olingo.common.ODataTypeManager;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.TranslatorException;

/**
 * Only walks the filter version of the query
 */
public class ODataFilterVisitor extends HierarchyVisitor {

    private static Map<String, String> infixFunctions = new HashMap<String, String>();
    static {
        infixFunctions.put("%", "mod");//$NON-NLS-1$ //$NON-NLS-2$
        infixFunctions.put("+", "add");//$NON-NLS-1$ //$NON-NLS-2$
        infixFunctions.put("-", "sub");//$NON-NLS-1$ //$NON-NLS-2$
        infixFunctions.put("*", "mul");//$NON-NLS-1$ //$NON-NLS-2$
        infixFunctions.put("/", "div");//$NON-NLS-1$ //$NON-NLS-2$
    }
    protected StringBuilder filter = new StringBuilder();
    private ODataExecutionFactory ef;
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    private ODataQuery query;
    private RuntimeMetadata metadata;
    private ODataDocumentNode filterOnElement;
    private BaseColumn currentExpression;
    
    public ODataFilterVisitor(ODataExecutionFactory ef, RuntimeMetadata metadata, ODataQuery query) {
        this.ef = ef;
        this.query = query;
        this.metadata = metadata;
    }
    
    public void appendFilter(Condition condition) throws TranslatorException{
        append(condition);
        if (!this.exceptions.isEmpty()) {
            throw this.exceptions.get(0);
        }
        if (this.filter.length() > 0) {
            this.filterOnElement.addFilter(this.filter.toString());
        }
    }
    
    @Override
    public void visit(Comparison obj) {
        append(obj.getLeftExpression());
        this.filter.append(Tokens.SPACE);
        switch(obj.getOperator()) {
        case EQ:
            this.filter.append("eq"); //$NON-NLS-1$
            break;
        case NE:
            this.filter.append("ne"); //$NON-NLS-1$
            break;
        case LT:
            this.filter.append("lt"); //$NON-NLS-1$
            break;
        case LE:
            this.filter.append("le"); //$NON-NLS-1$
            break;
        case GT:
            this.filter.append("gt"); //$NON-NLS-1$
            break;
        case GE:
            this.filter.append("ge"); //$NON-NLS-1$
            break;
        }
        this.filter.append(Tokens.SPACE);
        BaseColumn old = setCurrentExpression(obj.getLeftExpression());
        appendRightComparison(obj);
        this.currentExpression = old;
    }

    private BaseColumn setCurrentExpression(Expression leftExpression) {
        BaseColumn old = currentExpression;
        if (leftExpression instanceof ColumnReference) {
            ColumnReference cr = (ColumnReference)leftExpression;
            currentExpression = cr.getMetadataObject();
        } else if (leftExpression instanceof Function) {
            Function function = (Function)leftExpression;
            currentExpression = function.getMetadataObject().getOutputParameter();
        } else {
            currentExpression = null;
        }
        return old;
    }

    protected void appendRightComparison(Comparison obj) {
        append(obj.getRightExpression());
    }

    @Override
    public void visit(IsNull obj) {
        if (obj.isNegated()) {
            this.filter.append(NOT.toLowerCase()).append(Tokens.LPAREN);
        }
        appendNested(obj.getExpression());
        this.filter.append(Tokens.SPACE);
        this.filter.append("eq").append(Tokens.SPACE); //$NON-NLS-1$
        this.filter.append(NULL.toLowerCase());
        if (obj.isNegated()) {
            this.filter.append(Tokens.RPAREN);
        }
    }

    private void appendNested(Expression ex) {
        boolean useParens = ex instanceof Condition;
        if (useParens) {
            this.filter.append(Tokens.LPAREN);
        }
        append(ex);
        if (useParens) {
            this.filter.append(Tokens.RPAREN);
        }
    }

    @Override
    public void visit(AndOr obj) {
        String opString = obj.getOperator().name().toLowerCase();
        appendNestedCondition(obj, obj.getLeftCondition());
        this.filter.append(Tokens.SPACE)
              .append(opString)
              .append(Tokens.SPACE);
        appendNestedCondition(obj, obj.getRightCondition());
    }

    protected void appendNestedCondition(AndOr parent, Condition condition) {
        if (condition instanceof AndOr) {
            AndOr nested = (AndOr)condition;
            if (nested.getOperator() != parent.getOperator()) {
                this.filter.append(Tokens.LPAREN);
                append(condition);
                this.filter.append(Tokens.RPAREN);
                return;
            }
        }
        append(condition);
    }

    private String odataType(String type, String runtimeType) {
        if (type == null) {
            type = ODataTypeManager.odataType(runtimeType)
                    .getFullQualifiedName().getFullQualifiedNameAsString();
        }
        return type;
    }
    
    @Override
    public void visit(ColumnReference obj) {
        Column column = obj.getMetadataObject();
        // check if the column on psedo column, then move it to the parent.
        String pseudo = ODataMetadataProcessor.getPseudo(column);
        
        ODataDocumentNode schemaElement = this.query.getSchemaElement((Table)column.getParent());
        if (pseudo != null) {
            try {
                Table columnParent = (Table)column.getParent();
                Table pseudoColumnParent = this.metadata.getTable(
                        ODataMetadataProcessor.getMerge(columnParent));
                schemaElement = this.query.getSchemaElement(pseudoColumnParent);
            } catch (TranslatorException e) {
                this.exceptions.add(e);
            }
        }
        
        if (this.filterOnElement == null) {
            this.filterOnElement = schemaElement;
        } else if (schemaElement.isExpandType() && (!this.filterOnElement.isExpandType())) {
            this.exceptions.add(new TranslatorException(ODataPlugin.Util.gs(ODataPlugin.Event.TEIID17026)));
        }
        
        if (this.filterOnElement.isComplexType()) {            
            if (pseudo == null) {
                this.filter.append(this.filterOnElement.getName()).append("/").append(column.getName());
            } else {
                this.filter.append(pseudo);
            }
        } else {
            if (pseudo == null) {
                this.filter.append(column.getName());
            } else {
                this.filter.append(pseudo);
            }
        }        
    }
        
    protected boolean isInfixFunction(String function) {
        return infixFunctions.containsKey(function);
    }

    @Override
    public void visit(Function obj) {
        if (this.ef.getFunctionModifiers().containsKey(obj.getName())) {
            this.ef.getFunctionModifiers().get(obj.getName()).translate(obj);
        }

        String name = obj.getName();
        List<Expression> args = obj.getParameters();
        if(isInfixFunction(name)) {
            this.filter.append(Tokens.LPAREN);
            if(args != null) {
                for(int i=0; i<args.size(); i++) {
                    append(args.get(i));
                    if(i < (args.size()-1)) {
                        this.filter.append(Tokens.SPACE);
                        this.filter.append(infixFunctions.get(name));
                        this.filter.append(Tokens.SPACE);
                    }
                }
            }
            this.filter.append(Tokens.RPAREN);
        }
        else {
            FunctionMethod method = obj.getMetadataObject();
            if (name.startsWith(method.getCategory())) {
                name = name.substring(method.getCategory().length()+1);
            }
            this.filter.append(name).append(Tokens.LPAREN);
            if (name.equals("cast")) {
                append(args.get(0));
                this.filter.append(Tokens.COMMA);
                Literal literal = (Literal)args.get(1);
                String type = ODataTypeManager
                        .odataType((String) literal.getValue())
                        .getFullQualifiedName().getFullQualifiedNameAsString();
                this.filter.append(type);
            } else {
                if (args != null && args.size() != 0) {
                	if (SourceSystemFunctions.ENDSWITH.equalsIgnoreCase(name)) {
                		append(args.get(1));
                		this.filter.append(Tokens.COMMA);
                		append(args.get(0));
                	} else {
                	    BaseColumn old = currentExpression;
                        for (int i = 0; i < args.size(); i++) {
                            currentExpression = method.getInputParameters().get(Math.min(i, method.getInputParameters().size() -1));
    	                    append(args.get(i));
    	                    if (i < args.size()-1) {
    	                    	this.filter.append(Tokens.COMMA);
    	                    }
    	                }
                        currentExpression = old;
                	}
                }
            }
            this.filter.append(Tokens.RPAREN);
        }
    }
    public void visit(In obj) {
        for (int i = 0; i < obj.getRightExpressions().size(); i++) {
            Expression expr = obj.getRightExpressions().get(i);
            if (i != 0) {
                this.filter.append(" or ");
            }
            visitNode(obj.getLeftExpression());   
            this.filter.append(" eq ");
            BaseColumn old = setCurrentExpression(obj.getLeftExpression());
            visitNode(expr);
            this.currentExpression = old;
        }
    }   

    @Override
    public void visit(Literal obj) {
        try {
            String odataType = ODataTypeManager.odataType(obj.getType()).toString();
            if (currentExpression != null) {
                //TODO: this is an attempt at contextually figuring out the type, but it 
                //may not be sufficient in all cases
                odataType = odataType(currentExpression.getNativeType(), currentExpression.getRuntimeType());
            }
            this.filter.append(ODataTypeManager.convertToODataURIValue(obj.getValue(), odataType));
        } catch (EdmPrimitiveTypeException e) {
            this.exceptions.add(new TranslatorException(e));
        }
    }

    @Override
    public void visit(Not obj) {
        this.filter.append(NOT)
        .append(Tokens.SPACE)
        .append(Tokens.LPAREN);
        append(obj.getCriteria());
        this.filter.append(Tokens.RPAREN);
    }

    public void append(LanguageObject obj) {
        visitNode(obj);
    }

    protected void append(List<? extends LanguageObject> items) {
        if (items != null && items.size() != 0) {
            for (int i = 0; i < items.size(); i++) {
                append(items.get(i));
            }
        }
    }

    protected void append(LanguageObject[] items) {
        if (items != null && items.length != 0) {
            for (int i = 0; i < items.length; i++) {
                append(items[i]);
            }
        }
    }
}
