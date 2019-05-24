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
package org.teiid.jboss;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.PrivilegedActionException;
import java.util.Properties;

import org.jboss.logging.Logger;

/**
 * Common login module utility methods
 *
 * @author Scott.Stark@jboss.org
 * @version $Revision: 68749 $
 */
public class Util {
    /**
     * Utility method which loads the given properties file and returns a
     * Properties object containing the key,value pairs in that file. The
     * properties files should be in the class path as this method looks to the
     * thread context class loader (TCL) to locate the resource. If the TCL is a
     * URLClassLoader the findResource(String) method is first tried. If this
     * fails or the TCL is not a URLClassLoader getResource(String) is tried. If
     * not, an absolute path is tried.
     *
     * @param propertiesName
     *            - the name of the properties file resource
     * @param log
     *            - the logger used for trace level messages
     * @return the loaded properties file if found
     * @exception java.io.IOException
     *                thrown if the properties file cannot be found or loaded
     */
    static Properties loadProperties(String propertiesName, Logger log) throws IOException {
        ClassLoader loader = ResourceActions.getContextClassLoader();
        URL url = null;
        // First check for local visibility via a URLClassLoader.findResource
        if (loader instanceof URLClassLoader) {
            URLClassLoader ucl = (URLClassLoader) loader;
            url = ResourceActions.findResource(ucl, propertiesName);
            log.trace("findResource: " + url); //$NON-NLS-1$
        }
        if (url == null)
            url = loader.getResource(propertiesName);
        if (url == null) {
            url = new URL(propertiesName);
        }

        log.trace("Properties file=" + url); //$NON-NLS-1$

        Properties bundle = new Properties();
        InputStream is = null;
        try {
            is = ResourceActions.openStream(url);
        } catch (PrivilegedActionException e) {
            throw new IOException(e.getLocalizedMessage());
        }
        if (is != null) {
            bundle.load(is);
            is.close();
        } else {
            throw new IOException("Properties file " + propertiesName + " not available");//$NON-NLS-1$ //$NON-NLS-2$
        }
        log.debug("Loaded properties, users=" + bundle.keySet());//$NON-NLS-1$
        return bundle;
    }
}
