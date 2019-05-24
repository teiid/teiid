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

package org.teiid.translator.jdbc.postgresql;

import org.teiid.language.Array;
import org.teiid.language.ColumnReference;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Expression;
import org.teiid.language.Literal;
import org.teiid.language.SQLConstants;
import org.teiid.language.With;
import org.teiid.language.WithItem;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.SQLConversionVisitor;

public class PostgreSQLConversionVisitor
        extends SQLConversionVisitor {

    private PostgreSQLExecutionFactory postgreSQLExecutionFactory;

    public PostgreSQLConversionVisitor(PostgreSQLExecutionFactory ef) {
        super(ef);
        this.postgreSQLExecutionFactory = ef;
    }

    @Override
    protected void appendWithKeyword(With obj) {
        super.appendWithKeyword(obj);
        for (WithItem with : obj.getItems()) {
            if (with.isRecusive()) {
                buffer.append(SQLConstants.Tokens.SPACE);
                buffer.append(SQLConstants.Reserved.RECURSIVE);
                break;
            }
        }
    }

    /**
     * Some literals in the select need a cast to prevent being seen as the unknown/string type
     */
    @Override
    public void visit(DerivedColumn obj) {
        if (obj.getExpression() instanceof Literal) {
            String castType = null;
            if (obj.getExpression().getType() == TypeFacility.RUNTIME_TYPES.STRING) {
                castType = "bpchar"; //$NON-NLS-1$
            } else if (obj.getExpression().getType() == TypeFacility.RUNTIME_TYPES.VARBINARY) {
                castType = "bytea"; //$NON-NLS-1$
            }
            if (castType != null) {
                obj.setExpression(postgreSQLExecutionFactory.getLanguageFactory().createFunction("cast", //$NON-NLS-1$
                        new Expression[] {obj.getExpression(),  postgreSQLExecutionFactory.getLanguageFactory().createLiteral(castType, TypeFacility.RUNTIME_TYPES.STRING)},
                        TypeFacility.RUNTIME_TYPES.STRING));
            }
        } else if (obj.isProjected() && obj.getExpression() instanceof ColumnReference) {
            ColumnReference elem = (ColumnReference)obj.getExpression();
            if (elem.getMetadataObject() != null) {
                String nativeType = elem.getMetadataObject().getNativeType();
                if (TypeFacility.RUNTIME_TYPES.STRING.equals(elem.getType())
                        && elem.getMetadataObject() != null
                        && nativeType != null
                        && nativeType.equalsIgnoreCase(PostgreSQLExecutionFactory.UUID_TYPE)) {
                    obj.setExpression(postgreSQLExecutionFactory.getLanguageFactory().createFunction("cast", //$NON-NLS-1$
                            new Expression[] {obj.getExpression(),  postgreSQLExecutionFactory.getLanguageFactory().createLiteral("varchar", TypeFacility.RUNTIME_TYPES.STRING)}, //$NON-NLS-1$
                            TypeFacility.RUNTIME_TYPES.STRING));
                }
            }
        }
        super.visit(obj);
    }

    @Override
    public void visit(Array array) {
        boolean allLiterals = true;
        Class<?> baseType = array.getBaseType();
        //the pg driver expects only values that are convertible to string
        //we could introduce some conversions, but for now we'll just fail
        //some cases- there's also potential issue with date time as this logic
        //won't consider the database timezone setting
        if (!baseType.isArray() &&
                postgreSQLExecutionFactory.convertModifier.getSimpleTypeMapping(ConvertModifier.getCode(baseType)) != null) {
            for (Expression ex : array.getExpressions()) {
                if (!(ex instanceof Literal)) {
                    allLiterals = false;
                    break;
                }
            }
            if (allLiterals) {
                //TODO: this could be pushed to the language bridge factory
                //to just push a literal array
                addBinding(new Literal(array, array.getType()));
                return;
            }
        }
        //mixed or lob case
        //TODO: if this is used in the context specifically of an array, rather than
        //a row value, then this will fail
        super.visit(array);
    }

    /*
    @Override
    public void visit(In obj) {
        //TODO: array binding TEIID-3537
        super.visit(obj);
    }
    */
}