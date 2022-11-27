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

package org.teiid.query.function.source;

import org.teiid.dqp.internal.process.AuthorizationValidator;
import org.teiid.query.util.CommandContext;

public class SecuritySystemFunctions {

    public static final String DATA_ROLE = "data"; //$NON-NLS-1$

    public static boolean hasRole(CommandContext context, String roleName) {
        return hasRole(context, DATA_ROLE, roleName);
    }

    public static boolean hasRole(CommandContext context, String roleType, String roleName) {
        if (!DATA_ROLE.equalsIgnoreCase(roleType)) {
            return false;
        }
        if (context == null) {
            return true;
        }
        AuthorizationValidator authorizationValidator = context.getAuthorizationValidator();
        if (authorizationValidator == null) {
            return true;
        }
        return authorizationValidator.hasRole(roleName, context);
    }

}
