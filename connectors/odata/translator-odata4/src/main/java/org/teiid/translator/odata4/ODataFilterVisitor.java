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
package org.teiid.translator.odata4;

import static org.teiid.language.SQLConstants.Reserved.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.teiid.language.*;
import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.HierarchyVisitor;
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
    private Stack<String> exprType = new Stack<String>();
    protected ArrayList<TranslatorException> exceptions = new ArrayList<TranslatorException>();
    private ODataQuery query;
    private RuntimeMetadata metadata;
    private ODataDocumentNode filterOnElement;
    
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
        appendRightComparison(obj);
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
        
        this.exprType.push(odataType(column.getNativeType(), column.getRuntimeType()));
        
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
                this.exprType.push(type);
            } else {
                if (args != null && args.size() != 0) {
                	if (SourceSystemFunctions.ENDSWITH.equalsIgnoreCase(name)) {
                		append(args.get(1));
                		this.filter.append(Tokens.COMMA);
                		append(args.get(0));
                	} else {
    	                for (int i = 0; i < args.size(); i++) {
    	                    append(args.get(i));
    	                    if (i < args.size()-1) {
    	                    	this.filter.append(Tokens.COMMA);
    	                    }
    	                }
                	}
                }
                this.exprType.push(odataType(
                        method.getOutputParameter().getNativeType(), 
                        method.getOutputParameter().getRuntimeType()));
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
            visitNode(expr);
        }
    }   

    @Override
    public void visit(Literal obj) {
        try {
            String odataType = this.exprType.pop();
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
