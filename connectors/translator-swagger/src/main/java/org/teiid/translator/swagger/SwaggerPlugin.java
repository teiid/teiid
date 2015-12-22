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
    }   

}
