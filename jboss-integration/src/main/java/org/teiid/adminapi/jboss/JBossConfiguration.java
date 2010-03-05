/*
 * Jopr Management Platform
 * Copyright (C) 2005-2009 Red Hat, Inc.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2, as
 * published by the Free Software Foundation, and/or the GNU Lesser
 * General Public License, version 2.1, also as published by the Free
 * Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License and the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License
 * and the GNU Lesser General Public License along with this program;
 * if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 */
package org.teiid.adminapi.jboss;

import java.util.Map;
import java.util.HashMap;

import javax.security.auth.login.Configuration;
import javax.security.auth.login.AppConfigurationEntry;

/**
 * A JAAS configuration for a JBoss client. This is the programmatic equivalent of the following auth.conf file:
 *
 * <code>
 * jboss
 * {
 *   org.jboss.security.ClientLoginModule required
 *     multi-threaded=true;
 * };
 * </code>
 *
 * @author Ian Springer
 */
public class JBossConfiguration extends Configuration {
    public static final String JBOSS_ENTRY_NAME = "profileservice";

    private static final String JBOSS_LOGIN_MODULE_CLASS_NAME = "org.jboss.security.ClientLoginModule";
    private static final String MULTI_THREADED_OPTION = "multi-threaded";

    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        if (JBOSS_ENTRY_NAME.equals(name)) {
            Map options = new HashMap(1);
            options.put(MULTI_THREADED_OPTION, Boolean.TRUE.toString());
            AppConfigurationEntry appConfigurationEntry = new AppConfigurationEntry(JBOSS_LOGIN_MODULE_CLASS_NAME, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
            return new AppConfigurationEntry[] {appConfigurationEntry};
        } 
        throw new IllegalArgumentException("Unknown entry name: " + name);
    }

    public void refresh() {
        return;
    }
}
