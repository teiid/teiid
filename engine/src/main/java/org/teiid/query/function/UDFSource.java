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

package org.teiid.query.function;

import java.util.Collection;

import org.teiid.metadata.FunctionMethod;

public class UDFSource implements FunctionMetadataSource {

    protected Collection <FunctionMethod> functions;
    private ClassLoader classLoader;

    public UDFSource(Collection <FunctionMethod> methods) {
        this.functions = methods;
    }

    public Collection<FunctionMethod> getFunctionMethods() {
        return this.functions;
    }

    public Class<?> getInvocationClass(String className) throws ClassNotFoundException {
        return Class.forName(className, true, classLoader==null?Thread.currentThread().getContextClassLoader():classLoader);
    }

    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
