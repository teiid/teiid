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

import java.security.acl.Group;

import javax.security.auth.login.LoginException;

import org.jboss.security.SimpleGroup;
import org.jboss.security.auth.spi.UsernamePasswordLoginModule;

/**
 * A simple server login module to creates subject with passed in name and null
 * password
 */
public class SimpleLoginModule extends UsernamePasswordLoginModule {

    @Override
    protected boolean validatePassword(String inputPassword, String expectedPassword) {
        return true;
    }

    @Override
    protected String getUsersPassword() throws LoginException {
        return null;
    }

    @Override
    protected Group[] getRoleSets() throws LoginException {
        SimpleGroup roles = new SimpleGroup("Roles"); //$NON-NLS-1$
        Group[] roleSets = { roles };
        return roleSets;
    }
}