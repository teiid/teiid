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

package org.teiid.language;

import org.teiid.language.visitor.LanguageObjectVisitor;
import org.teiid.metadata.ProcedureParameter;

public class Argument extends BaseLanguageObject implements MetadataReference<ProcedureParameter> {

    public enum Direction {
        IN,
        OUT,
        INOUT,
    }

    private Direction direction;
    private Expression argumentValue;
    private ProcedureParameter metadataObject;
    private Class<?> type;

    public Argument(Direction direction, Expression value, Class<?> type, ProcedureParameter metadataObject) {
        this.direction = direction;
        this.argumentValue = value;
        this.metadataObject = metadataObject;
        this.type = type;
    }

    /**
     * Typical constructor for an out/return parameter
     * @param direction
     * @param type
     * @param metadataObject
     */
    public Argument(Direction direction, Class<?> type, ProcedureParameter metadataObject) {
        this.direction = direction;
        this.metadataObject = metadataObject;
        this.type = type;
    }

    /**
     * Typical constructor for an in/in out parameter
     * @param direction
     * @param value
     * @param metadataObject
     */
    public Argument(Direction direction, Literal value, ProcedureParameter metadataObject) {
        this.direction = direction;
        this.argumentValue = value;
        this.metadataObject = metadataObject;
        if (value != null) {
            this.type = value.getType();
        }
    }

    public Direction getDirection() {
        return this.direction;
    }

    /**
     * Get the argument as a {@link Literal} value.
     * Will throw a {@link ClassCastException} if the {@link Expression} is not a {@link Literal}.
     * @return the value or null if this is an non-in parameter
     */
    public Literal getArgumentValue() {
        return (Literal)this.argumentValue;
    }

    public Class<?> getType() {
        return type;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public void setArgumentValue(Literal value) {
        this.argumentValue = value;
    }

    @Override
    public ProcedureParameter getMetadataObject() {
        return this.metadataObject;
    }

    public void setMetadataObject(ProcedureParameter metadataObject) {
        this.metadataObject = metadataObject;
    }

    public Expression getExpression() {
        return this.argumentValue;
    }

    public void setExpression(Expression ex) {
        this.argumentValue = ex;
    }

}
