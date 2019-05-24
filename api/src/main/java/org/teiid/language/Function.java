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

import java.util.ArrayList;
import java.util.List;

import org.teiid.language.visitor.LanguageObjectVisitor;
import org.teiid.metadata.FunctionMethod;

/**
 * Represents a function.  A function has a name and 0..n
 * Expressions that are parameters.
 */
public class Function extends BaseLanguageObject implements Expression, MetadataReference<FunctionMethod> {

    private String name;
    private List<Expression> parameters;
    private Class<?> type;
    private FunctionMethod metadataObject;

    public Function(String name, List<? extends Expression> params, Class<?> type) {
        this.name = name;
        if (params == null) {
            this.parameters = new ArrayList<Expression>(0);
        } else {
            this.parameters = new ArrayList<Expression>(params);
        }
        this.type = type;
    }

    @Override
    public FunctionMethod getMetadataObject() {
        return metadataObject;
    }

    public void setMetadataObject(FunctionMethod metadataObject) {
        this.metadataObject = metadataObject;
    }

    /**
     * Get name of the function
     * @return Function name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Get the parameters used in this function.
     * @return List of Expression defining the parameters
     */
    public List<Expression> getParameters() {
        return parameters;
    }

    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Set name of the function
     * @param name Function name
     */
    public void setName(String name) {
        this.name = name;
    }

    public Class<?> getType() {
        return this.type;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }

}
