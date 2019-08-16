/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.teiid.metadata.index;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;


/**
 * CommonPlugin
 * <p>Used here in <code>metadata.runtime</code> to have access to the new
 * logging framework for <code>LogManager</code>.
 */
public class RuntimeMetadataPlugin {

    /**
     * The plug-in identifier of this plugin
     */
    public static final String PLUGIN_ID = "org.teiid.metadata"; //$NON-NLS-1$

    public static final BundleUtil Util = new BundleUtil(PLUGIN_ID,
                                                             PLUGIN_ID + ".i18n", ResourceBundle.getBundle(PLUGIN_ID + ".i18n")); //$NON-NLS-1$ //$NON-NLS-2$

    public static enum Event implements BundleUtil.Event {
        TEIID80000,
        TEIID80002,
        TEIID80003,
        TEIID80004
    }
}
