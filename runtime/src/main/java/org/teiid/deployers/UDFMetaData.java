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
package org.teiid.deployers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.teiid.metadata.FunctionMethod;
import org.teiid.query.function.UDFSource;


public class UDFMetaData {
    protected TreeMap<String, UDFSource> methods = new TreeMap<String, UDFSource>(String.CASE_INSENSITIVE_ORDER);
    private ClassLoader classLoader;

    public Map<String, UDFSource> getFunctions(){
        return this.methods;
    }

    public void addFunctions(String name, Collection <FunctionMethod> funcs){
        if (funcs.isEmpty()) {
            return;
        }
        UDFSource udfSource = this.methods.get(name);
        if (udfSource != null) {
            //this is ambiguous about as to what classloader to use, but we assume the first is good and that the user will have set
            //the Java method if that's not the case
            ArrayList<FunctionMethod> allMethods = new ArrayList<FunctionMethod>(udfSource.getFunctionMethods());
            allMethods.addAll(funcs);
            ClassLoader cl = udfSource.getClassLoader();
            udfSource = new UDFSource(allMethods);
            udfSource.setClassLoader(cl);
        } else {
            udfSource = new UDFSource(funcs);
            udfSource.setClassLoader(classLoader);
        }
        this.methods.put(name, udfSource);
    }

    public void addFunctions(UDFMetaData funcs){
        this.methods.putAll(funcs.methods);
        this.classLoader = funcs.classLoader;
    }

    public void setFunctionClassLoader(ClassLoader functionClassLoader) {
        for (UDFSource udf : methods.values()) {
            udf.setClassLoader(functionClassLoader);
        }
        this.classLoader = functionClassLoader;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
