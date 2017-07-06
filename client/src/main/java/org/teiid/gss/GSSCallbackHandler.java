/*-------------------------------------------------------------------------
*
* Copyright (c) 2008, PostgreSQL Global Development Group
*
* IDENTIFICATION
*   $PostgreSQL: pgjdbc/org/postgresql/gss/GSSCallbackHandler.java,v 1.2 2008/11/29 07:43:47 jurka Exp $
*
*-------------------------------------------------------------------------
*/

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
package org.teiid.gss;

import java.io.IOException;
import javax.security.auth.callback.*;

import org.teiid.jdbc.JDBCPlugin;

public class GSSCallbackHandler implements CallbackHandler {

    private final String user;
    private final String password;

    public GSSCallbackHandler(String user, String password)
    {
        this.user = user;
        this.password = password;
    }

    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
    {
        for (int i=0; i<callbacks.length; i++) {
            if (callbacks[i] instanceof TextOutputCallback) {
                TextOutputCallback toc = (TextOutputCallback)callbacks[i];
                switch (toc.getMessageType()) {
                    case TextOutputCallback.INFORMATION:
                        System.out.println("INFO: " + toc.getMessage());//$NON-NLS-1$
                        break;
                    case TextOutputCallback.ERROR:
                        System.out.println("ERROR: " + toc.getMessage()); //$NON-NLS-1$
                        break;
                    case TextOutputCallback.WARNING:
                        System.out.println("WARNING: " + toc.getMessage());//$NON-NLS-1$
                        break;
                    default:
                        throw new IOException("Unsupported message type: " + toc.getMessageType()); //$NON-NLS-1$
                }
            } else if (callbacks[i] instanceof NameCallback) {
                NameCallback nc = (NameCallback)callbacks[i];
                nc.setName(user);
            } else if (callbacks[i] instanceof PasswordCallback) {
                PasswordCallback pc = (PasswordCallback)callbacks[i];
                if (password == null) {
                    throw new IOException(JDBCPlugin.Util.getString("no_krb_ticket")); //$NON-NLS-1$
                }
                pc.setPassword(password.toCharArray());
            } else {
                throw new UnsupportedCallbackException(callbacks[i], "Unrecognized Callback"); //$NON-NLS-1$
            }
        }
    }

}


