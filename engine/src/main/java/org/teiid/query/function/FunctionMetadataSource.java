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


/**
 * A FunctionMetadataSource represents a source of function metadata for
 * the function library.  A FunctionMetadataSource needs to know how to
 * return a collection of all the function signatures it knows about.
 */
public interface FunctionMetadataSource {

    /**
     * This method requests that the source return all
     * {@link FunctionMethod}s
     * the source knows about.  This can occur in several situations -
     * on initial registration with the FunctionLibraryManager, on a
     * general reload, etc.  This may be called multiple times and should
     * always return the newest information available.
     * @return Collection of FunctionMethod objects
     */
    Collection<FunctionMethod> getFunctionMethods();

    /**
     * This method determines where the invocation classes specified in the
     * function metadata are actually retrieved from.
     * @param className Name of class
     * @return Class reference
     * @throws ClassNotFoundException If class could not be found
     */
    Class<?> getInvocationClass(String className) throws ClassNotFoundException;

    /**
     * Classloader used for functions
     * @return
     */
    ClassLoader getClassLoader();
}
