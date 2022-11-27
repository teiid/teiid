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

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Vector;

/** A Classloader that takes in two Classloaders to delegate to */
public class CombinedClassLoader extends ClassLoader {
    private ClassLoader[] toSearch;

    public CombinedClassLoader(ClassLoader parent, ClassLoader... toSearch){
        super(parent);
        this.toSearch = toSearch;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        for (ClassLoader cl : toSearch) {
            if (cl == null) {
                continue;
            }
            try {
                return cl.loadClass(name);
            } catch (ClassNotFoundException e) {

            }
        }
        return super.loadClass(name);
    }

    @Override
    protected URL findResource(String name) {
        for (ClassLoader cl : toSearch) {
            if (cl == null) {
                continue;
            }
            URL url = cl.getResource(name);
            if (url != null) {
                return url;
            }
        }
        return super.getResource(name);
    }

    @Override
    protected Enumeration<URL> findResources(String name) throws IOException {
        Vector<URL> result = new Vector<URL>();
        for (ClassLoader cl : toSearch) {
            if (cl == null) {
                continue;
            }
            Enumeration<URL> url = cl.getResources(name);
            result.addAll(Collections.list(url));
        }
        return result.elements();
    }

}