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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;


/**
 *  Privileged Blocks
 *  @author Anil.Saldhana@redhat.com
 *  @since  Sep 26, 2007
 *  @version $Revision$
 */
class ResourceActions
{
   static ClassLoader getContextClassLoader()
   {
      return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>()
      {
         public ClassLoader run()
         {
            return Thread.currentThread().getContextClassLoader();
         }
       });
   }

   static URL findResource(final URLClassLoader cl, final String name)
   {
      return AccessController.doPrivileged(new PrivilegedAction<URL>()
      {
         public URL run()
         {
            return cl.findResource(name);
         }
       });
   }

   static InputStream openStream(final URL url) throws PrivilegedActionException
   {
      return AccessController.doPrivileged(new PrivilegedExceptionAction<InputStream>()
      {
         public InputStream run() throws IOException
         {
            return url.openStream();
         }
       });
   }
}