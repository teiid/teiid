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

package org.teiid.query.metadata;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.teiid.CommandContext;
import org.teiid.UserDefinedAggregate;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.query.function.TeiidFunction;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

/**
 * Simple metadata loader for functions
 *
 * TODO: make the {@link TeiidFunction} annotation public
 */
public class UDFMetadataRepository implements MetadataRepository {

    @Override
    public void loadMetadata(MetadataFactory factory,
            ExecutionFactory executionFactory, Object connectionFactory,
            String text) throws TranslatorException {
        String className = factory.getModelProperties().getProperty("importer.schemaName"); //$NON-NLS-1$
        String methodNames = factory.getModelProperties().getProperty("importer.methodNames"); //$NON-NLS-1$
        ClassLoader classLoader = factory.getVDBClassLoader();
        Set<String> allowed = null;
        if (methodNames != null) {
            allowed = new HashSet<>(Arrays.asList(methodNames.split(","))); //$NON-NLS-1$
        }
        try {
            Class<?> clazz = classLoader.loadClass(className);

            if (UserDefinedAggregate.class.isAssignableFrom(clazz)) {
                factory.addFunction(clazz.getSimpleName(), clazz.getMethod("getResult", new Class<?>[] {CommandContext.class}));
            } else {
                Method[] methods = clazz.getMethods();
                for (Method m : methods) {
                    if (!Modifier.isStatic(m.getModifiers())) {
                        continue;
                    }
                    if (m.getReturnType() == Void.TYPE) {
                        continue;
                    }
                    if (allowed != null && !allowed.contains(m.getName())) {
                        continue;
                    }
                    factory.addFunction(m.getName(), m);
                }
            }
        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException e) {
            throw new TranslatorException(e);
        }
    }

}
