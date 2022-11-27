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
package org.teiid.translator.swagger;

import java.util.ResourceBundle;

import org.teiid.core.BundleUtil;

public class SwaggerPlugin {

    public static final String PLUGIN_ID = "org.teiid.translator.swagger" ;  //$NON-NLS-1$

    private static final String BUNDLE_NAME = PLUGIN_ID + ".i18n"; //$NON-NLS-1$

    public static BundleUtil Util = new BundleUtil(PLUGIN_ID, BUNDLE_NAME, ResourceBundle.getBundle(BUNDLE_NAME));

    public static enum Event implements BundleUtil.Event{
        TEIID28001,
        TEIID28002,
        TEIID28003,
        TEIID28004,
        TEIID28005,
        TEIID28006,
        TEIID28007,
        TEIID28008,
        TEIID28009,
        TEIID28010,
        TEIID28011,
        TEIID28012,
        TEIID28013,
        TEIID28014,
        TEIID28015,
        TEIID28016,
        TEIID28017,
        TEIID28018,
        TEIID28019
    }

}
