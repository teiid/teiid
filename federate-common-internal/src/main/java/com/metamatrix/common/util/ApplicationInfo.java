/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.common.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.jar.Attributes.Name;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.util.DateUtil;

/**
 * This class represents a container for the component information objects for
 * an application.  Each component information object, of type
 * <code>ApplicationInfo.Component</code>, contains various String attributes
 * about a particular component.
 * <p>The following guidelines are suggested to achieve consistent usage and
 * behavior.
 *
 * <li>The application's main method should load the information
 * for those components that are important; for server products, this will
 * typically entail all of the various products (such as 'common', 'platform',
 * etc.).  These should be contained in the metamatrix-complete.jar file.
 * GUI tools should add all of the package dependencies of the
 * tool using addComponent() and then should setMainComponent() passing in the package of the
 * tool application package.  This will allow the tool to use common widgets
 * in the toolbox package that are designed to show information about the GUI component.
 * See example below for typical usage.</li>
 *
 * <li>Immediately after loading the appropriate components, the application's
 * main method should also mark the application information as unmodifiable,
 * which prevents any other component from modifying the information.</li>
 * <li>For each component that is loaded into the local VM's application information,
 * The information must be contained in the jar's manifest information
 * during loading.  Note that these properties should be located within
 * the Jar's manifest file of the application, rather than a publicly accessible file
 * that may be subject to modification.</li>
 * <p>The following code snippet shows an example of an application that loads
 * two components, which should be done early in the main method of the application:</p>
 * <p><pre>
 *     try {
 *        ApplicationInfo info = ApplicationInfo.getInstance();
 *        info.addComponent("metamatrix-complete");
 *        info.addComponent("modeler");
 *        info.markUnmodifiable();
 *    } catch ( ComponentNotFoundException e ) {
 *        e.printStackTrace(System.out);
 *    } catch ( IllegalStateException e ) {
 *        e.printStackTrace(System.out);
 *     }
 * </pre></p>
 *
 * <p>The following code snippet shows an example of a GUI application that
 * loads two components (dependencies) and the loads the 'Main' component.
 * This should all be done early in the main method of the application:</p>
 *
 * <p><pre>
 *     try {
 *        ApplicationInfo info = ApplicationInfo.getInstance();
 *        info.addComponent("metamatrix-complete");
 *        info.setMainComponent("querybuilder/");
 *        info.markUnmodifiable();
 *    } catch ( ComponentNotFoundException e ) {
 *        e.printStackTrace(System.out);
 *    } catch ( IllegalStateException e ) {
 *        e.printStackTrace(System.out);
 *     }
 * </pre></p>
 *
 * <p>This class is serializable, and thus may be passed from one VM to another.
 * However, the application should always obtain the application information for
 * the current VM via the <code>ApplicationInfo.getInstance()</code> method.</p>
 */
public final class ApplicationInfo implements Serializable {
    
    public static final String APPLICATION_PRODUCT_INFORMATION       = "Product Information"; //$NON-NLS-1$

    public static final String APPLICATION_RELEASE_NUMBER_PROPERTY     = "Release-Number"; //$NON-NLS-1$
    public static final String APPLICATION_BUILD_NUMBER_PROPERTY       = "Build"; //$NON-NLS-1$
    

    private static final ApplicationInfo INSTANCE = new ApplicationInfo();

    private static final String LINE_SEPARATOR = "\n"; //$NON-NLS-1$

    private static final String METAMATRIX_JAR = "metamatrix.jar.location"; //$NON-NLS-1$

    /**
     * These static properties are contained in the manifest entry of the jar file
     *
     */

    private static final String LIBRARY                               = "Library"; //$NON-NLS-1$

    private static final String APPLICATION_TITLE_PROPERTY              = "Title"; //$NON-NLS-1$
    private static final String APPLICATION_DESCRIPTION_PROPERTY        = "Description"; //$NON-NLS-1$
    private static final String APPLICATION_RELEASE_DATE_PROPERTY       = "Release-Date"; //$NON-NLS-1$
    private static final String APPLICATION_BUILD_DATE_PROPERTY         = "Build-Date"; //$NON-NLS-1$
    private static final String APPLICATION_COPYRIGHT_PROPERTY          = "Copyright"; //$NON-NLS-1$
    private static final String APPLICATION_LICENSE_ID_PROPERTY         = "License-ID"; //$NON-NLS-1$
    private static final String APPLICATION_LICENSE_VERSION_PROPERTY    = "License-version"; //$NON-NLS-1$

    
    private static final String APPLICATION_ABOUT_TITLE_PROPERTY        = "About-Title"; //$NON-NLS-1$
    private static final String APPLICATION_ABOUT_ICON_PROPERTY         = "About-Icon"; //$NON-NLS-1$
    private static final String APPLICATION_ABOUT_DESCRIPTION_PROPERTY  = "About-Description"; //$NON-NLS-1$
    private static final String APPLICATION_ABOUT_URL_PROPERTY          = "About-URL"; //$NON-NLS-1$

    private static final String PATCH                                   = "Patch"; //$NON-NLS-1$
    private static final String PATCH_VERSION                         = "Patch-Version"; //$NON-NLS-1$
    private static final String PATCH_DATE                            = "Patch-Date"; //$NON-NLS-1$

    private String originatingVM;
    private String originatingHostname;
    private LinkedList components;
    private boolean unmodifiable;

    private ApplicationInfo() {
        components = new LinkedList();
        unmodifiable = false;
        originatingVM = VMNaming.getVMName();
        if ( originatingVM == null ) {
            originatingVM = "VM ID " + VMNaming.getVMID(); //$NON-NLS-1$
        }
        try {
            originatingHostname = InetAddress.getLocalHost().getHostName();
        } catch ( UnknownHostException e ) {
            originatingHostname = "localhost"; //$NON-NLS-1$
        }
    }

    /**
     * Get the application information instance for this VM.
     * @return the singleton instance for this VM; never null
     */
    public static ApplicationInfo getInstance() {
        return INSTANCE;
    }

    /**
     * Add the component information stored in the specified package.
     * @param parentPackage This is the jar file where the application
     * specific information for the product is stored in the manifest of the .jar file
     * @throws PropertyLoaderException if there is an error loading
     * the application information from the specified location
     * @throws IllegalStateException if this object can not be modified when this
     * method is called
     */
    public synchronized void addComponent( String parentPackage ) throws ComponentNotFoundException {
        if ( isUnmodifiable() ) {
            throw new IllegalStateException(CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0001));
        }

        if ( this.containsComponent(parentPackage) ) {
            return;
        }
        components.add(createComponent(parentPackage));
    }

    private Component createComponent(String parentPackage) throws ComponentNotFoundException{

        ApplicationInfo.Component c = new ApplicationInfo.Component(parentPackage);
        try {

            StringTokenizer tokenClasspath = setupClassPath();
            JarFile jarFile = findManifestInClasspath(tokenClasspath,LIBRARY, parentPackage);

            if(jarFile == null){
                throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0002));
            }

            Attributes manifestAttributes = jarFile.getManifest().getAttributes(APPLICATION_PRODUCT_INFORMATION);

            c.setTitle(           (String)manifestAttributes.get(new Name(APPLICATION_TITLE_PROPERTY)));
            c.setDescription(     (String)manifestAttributes.get(new Name(APPLICATION_DESCRIPTION_PROPERTY)));
            c.setReleaseNumber(   (String)manifestAttributes.get(new Name(APPLICATION_RELEASE_NUMBER_PROPERTY)));
            c.setReleaseDate(     (String)manifestAttributes.get(new Name(APPLICATION_RELEASE_DATE_PROPERTY)));
            c.setBuildNumber(     (String)manifestAttributes.get(new Name(APPLICATION_BUILD_NUMBER_PROPERTY)));
            c.setBuildDate(       (String)manifestAttributes.get(new Name(APPLICATION_BUILD_DATE_PROPERTY)));
            c.setCopyright(       (String)manifestAttributes.get(new Name(APPLICATION_COPYRIGHT_PROPERTY)));
            c.setLicenseID(       (String)manifestAttributes.get(new Name(APPLICATION_LICENSE_ID_PROPERTY)));
            c.setLicenseVersion(  (String)manifestAttributes.get(new Name(APPLICATION_LICENSE_VERSION_PROPERTY)));
            c.setAboutTitle(      (String)manifestAttributes.get(new Name(APPLICATION_ABOUT_TITLE_PROPERTY)));
            c.setAboutIcon(       (String)manifestAttributes.get(new Name(APPLICATION_ABOUT_ICON_PROPERTY)));
            c.setAboutDescription((String)manifestAttributes.get(new Name(APPLICATION_ABOUT_DESCRIPTION_PROPERTY)));
            c.setAboutURL(        (String)manifestAttributes.get(new Name(APPLICATION_ABOUT_URL_PROPERTY)));

            JarFile patchJarFile = findManifestInClasspath(tokenClasspath,LIBRARY,PATCH);
            if(patchJarFile != null){
                // this is a patch so include this information
                c.setPatchDate(  (String)manifestAttributes.get(new Name(PATCH_DATE)));
                c.setPatchLevel( (String)manifestAttributes.get(new Name(PATCH_VERSION)));
            }

        } catch (IOException io){
            try {
                LogManager.logWarning(LogCommonConstants.CTX_CONFIG, CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0004, METAMATRIX_JAR));
                //I18nLogManager.logWarning(LogCommonConstants.CTX_CONFIG, ErrorMessageKeys.CM_UTIL_ERR_0004, METAMATRIX_JAR);
            } catch (NoClassDefFoundError err) {
                // TODO: Convert change logging to work on both client and server.
                // ignore, if running in JDBC driver the I18nLogManager does not exist.
            }
            throw new ComponentNotFoundException(ErrorMessageKeys.CM_UTIL_ERR_0003, CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0003, parentPackage));
        }


        return c;
    }
    
    private static String getClassPath() {
        return System.getProperty( "java.class.path" ); //$NON-NLS-1$
    }


    private static StringTokenizer setupClassPath(){
        String classPath = getClassPath(); 
            //System.getProperty( "java.class.path" ); //$NON-NLS-1$
        String separator = System.getProperty( "path.separator"  ); //$NON-NLS-1$

        // If metamatrix jar file is not located in the system classpath then
        // a property needs to be set to point to the jar file.
        String jarFile = System.getProperty(METAMATRIX_JAR);
        if (jarFile != null && jarFile.length() > 0) {
            classPath = classPath + separator + jarFile;
        }
        return new StringTokenizer( classPath, separator );
    }

    /**
     * Attempts to find Vendor information in the manifest entries of jar files in the classpath
     *
     * @param path
     * @param VENDOR
     * @return JarFile
     */
    private static JarFile findManifestInClasspath(StringTokenizer path,String attributeToFind, String attributeMatch ){
        JarFile file = null;
        while(path.hasMoreTokens()){
            String pathElement = path.nextToken();
            File pathFile = new File( pathElement );
            if(!pathFile.isDirectory()){
                if((pathElement.indexOf(".jar")>0) || ((pathElement.indexOf(".zip")>0))){ //$NON-NLS-1$ //$NON-NLS-2$
                    //This is a jar file so look for it's Manifest information
                    try{
                        file = new JarFile(pathElement);
                        Manifest manifest = file.getManifest();
                        if(manifest != null){
                            Attributes manifestAttributes = manifest.getAttributes(APPLICATION_PRODUCT_INFORMATION);
                            if(manifestAttributes != null){
                                String vendor = (String)manifestAttributes.get(new Name(attributeToFind));
                                if(vendor != null){
                                    if(vendor.equals(attributeMatch)){
                                        return file;
                                    }
                                }
                            }
                        }
                    } catch(IOException io){
                        // ignore
                    }
                }
            }
        }
        return null;
    }
    
    private boolean containsComponent( String packageName ) {
        Iterator iter = this.components.iterator();
        while ( iter.hasNext() ) {
            ApplicationInfo.Component comp = (ApplicationInfo.Component) iter.next();
            if ( comp.getComponentPackage().equals(packageName) ) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the list of the component information objects for this application.
     * @return the unmodifiable list of components (in the order they were added);
     * never null, but maybe empty.
     */
    public synchronized List getComponents() {
        return Collections.unmodifiableList(components);

    }

    /**
     * Determine whether this application information object is unmodifable.
     * @return true if this instance may not be modified, or false otherwise
     */
    public boolean isUnmodifiable() {
        return this.unmodifiable;
    }

    /**
     * Make this application instance be unmodifable, even if serialized.
     */
    public void markUnmodifiable() {
        // Return immediately if already unmodifiable
        if ( this.unmodifiable ) {
            return;
        }

        // Otherwise, mark as unmodifiable and log the release and build numbers of each component
        this.unmodifiable = true;
        if ( ApplicationInfo.getInstance() == this ) {
            Iterator iter = this.getComponents().iterator();
            while (iter.hasNext()) {
                ApplicationInfo.Component comp = (ApplicationInfo.Component) iter.next();
                try {
                    LogManager.logInfo(LogCommonConstants.CTX_CONFIG, CommonPlugin.Util.getString("MSG.003.030.0001",  new Object[] {comp.getTitle(), comp.getReleaseNumber(), comp.getBuildNumber()} ) ); //$NON-NLS-1$
                    //I18nLogManager.logInfo(LogCommonConstants.CTX_CONFIG, LogMessageKeys.CM_UTIL_MSG_0001,  new Object[] {comp.getTitle(), comp.getReleaseNumber(), comp.getBuildNumber()} );
                } catch (NoClassDefFoundError err) {
                    // TODO: Convert change logging to work on both client and server.
                    // ignore, if running in JDBC driver the I18nLogManager does not exist.
                }
            }
        }
    }

    /**
     * Return the stringified representation of this application information object.
     * @return the string form of this object; never null
     */
    public synchronized String toString() {
        StringBuffer sb = new StringBuffer("Application Information"); //$NON-NLS-1$ 
        sb.append( LINE_SEPARATOR );
        sb.append(" VM Name:               " + this.originatingVM ); //$NON-NLS-1$
        sb.append(LINE_SEPARATOR);
        sb.append(" Hostname:              " + this.originatingHostname ); //$NON-NLS-1$
        sb.append(LINE_SEPARATOR);
        Iterator iter = this.components.iterator();
        if ( iter.hasNext() ) {
            sb.append("Main Component:"); //$NON-NLS-1$
            sb.append(LINE_SEPARATOR);
            sb.append( iter.next().toString() );
        }
        while ( iter.hasNext() ) {
            sb.append(LINE_SEPARATOR);
            sb.append("Dependency Component:"); //$NON-NLS-1$
            sb.append( LINE_SEPARATOR );
            sb.append( iter.next().toString() );
        }
        return sb.toString();
    }

    /**
     * Return the name of the VM (or the VM ID if the name was not set)
     * in which this application information object was created.
     * @return the name of the VM or the VM numeric identifier; never null
     */
    public String getVMName() {
        return this.originatingVM;
    }
    /**
     * Return the name of the host in which this application information object
     * was created.
     * @return the hostname (or "localhost" if the host does not have a name)
     * of the machine on which the VM in which this application information object
     * was created; never null
     */
    public String getHostname() {
        return this.originatingHostname;
    }

    /**
    * This method will return the 'Main' Component of the application.  This
    * is typically used in the case of GUI applications where information
    * associated with the 'Main' component is used in the generation of
    * certain common toolbox widgets.  If no main component has been set,
    * this method will return the first component that was added to this
    * ApplicationInfo instance.
    *
    * @return if setMainComponent has been called, that component will be returned
    * if not, the first Component that was added to the Application info class
    * will be returned.  If no components have yet been added, null will be
    * returned.
    */
    public Component getMainComponent() {
        if (components.size()>0) {
            return (Component)components.getFirst();
        }
        return null;
    }

    /**
    * This method will set the 'Main' Component of the application.  This
    * is typically used in the case of GUI applications where information
    * associated with the 'Main' component is used in the generation of
    * certain common toolbox widgets.  If this method has not yet been called
    * to set the 'Main' component then calls to the getMainComponent method
    * will return the first component that was added to this
    * ApplicationInfo instance.
    *
    * @param componentPackage the name of the package of the component to be
    * designated as the 'Main' component for this ApplicationInfo instance.
    * @throws PropertyLoaderException if there is an error loading
    * the application information from the specified location
    * @throws IllegalStateException if this object can not be modified when this
    * method is called
    * @throws ComponentNotFoundException if the application.properties file
    * for that packags cannot be found or is invalid
    */
    public void setMainComponent(String componentPackage) throws ComponentNotFoundException{

        if ( isUnmodifiable() ) {
            throw new IllegalStateException(CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0001));
        }

        Component mainComponent = createComponent(componentPackage);
        if ( components.contains(mainComponent) ) {
            if(getMainComponent()!=null && getMainComponent().equals(mainComponent)){
                return;
            }
            components.remove(mainComponent);
            components.addFirst(mainComponent);
        }else {
            components.addFirst(createComponent(componentPackage));
        }
    }
    
    /**
     * The getClasspathInfo method is used to capture the current classpath 
     * information.  The initial intent is to write this information
     * to a file at VM startup time for debugging purposes and ensuring
     * patches are applied. 
     * @param outputstream
     * @since 4.2
     */
    public synchronized String getClasspathInfo() {
        String classPath = getClassPath(); 

        StringBuffer sb = new StringBuffer();
        sb.append("\nDate: " + DateUtil.getCurrentDateAsString());//$NON-NLS-1$
        sb.append( LINE_SEPARATOR );        
        
        Map pathResults = new HashMap();
        List reversetList = new ArrayList();
        StringTokenizer path = setupClassPath();
        while(path.hasMoreTokens()){
            String pathElement = path.nextToken();
            File pathFile = new File( pathElement );
            if (pathFile.exists()) {
//                        if (resourceExistInClassPath(pathElement)) {
                pathResults.put(pathElement, Boolean.TRUE);
                    
            } else {
                pathResults.put(pathElement, Boolean.FALSE);
            }
            reversetList.add(pathElement);
        }       
        sb.append("Classpath Information" ); //$NON-NLS-1$
        sb.append( LINE_SEPARATOR );
        sb.append("CLASSPATH: "); //$NON-NLS-1$
        sb.append(classPath);
        sb.append( LINE_SEPARATOR );
        sb.append( LINE_SEPARATOR );
        
        Iterator iter = reversetList.iterator();
        sb.append("---- Classpath Entries ----"); //$NON-NLS-1$
         sb.append( LINE_SEPARATOR );       
        while ( iter.hasNext() ) {
            String epath = (String) iter.next();
            Boolean doesExist = (Boolean) pathResults.get(epath);
            sb.append(epath);
            if (!doesExist.booleanValue()) {
                sb.append(" (MISSING)");//$NON-NLS-1$
            }
            sb.append( LINE_SEPARATOR );
            
        }
        
        
        sb.append( LINE_SEPARATOR );
        sb.append("Note the (MISSING) at the end to designate that the classpath entry is missing");//$NON-NLS-1$
        
        
        try {
            // find all the patch readme files and print those out.
            Enumeration readmes =  ClassLoader.getSystemResources("patch_readme.txt");  //$NON-NLS-1$        
            sb.append( LINE_SEPARATOR );
            sb.append( LINE_SEPARATOR );

            sb.append("---- Patch Readme Entries----"); //$NON-NLS-1$
            sb.append( LINE_SEPARATOR );
            int cnt = 0;
            if (readmes != null) {
                
                while(readmes.hasMoreElements()) {
                    ++cnt;
                    URL url = (URL) readmes.nextElement();
                    sb.append("Patch " + url.getFile() + ":"); //$NON-NLS-1$ //$NON-NLS-2$
                    sb.append( LINE_SEPARATOR );
                    InputStream is = url.openStream();
                    byte[] data = ByteArrayHelper.toByteArray(is);
                    sb.append(new String(data));
                    sb.append("-------------------------------------");//$NON-NLS-1$
                    sb.append( LINE_SEPARATOR );
                    is.close();

                }
            } 
            if (cnt == 0) {
                sb.append("no Patch Readme Entries found"); //$NON-NLS-1$                
            }
        } catch (IOException ioe) {
            
        }
        
        return sb.toString();
    }
    
    
    /**
     * This class represents the information about a single, major component
     * of the application.  Instances of this class are always immutable, and
     * may only exist within an ApplicationInfo instance (it is a nested class,
     * and therefore always has a reference to the owning ApplicationInfo, even
     * after serialization and deserialization).
     */
    public final class Component implements Serializable {
        private String packageName;
        private String title;
        private String desc;
        private String releaseNumber;
        private String releaseDate;
        private String buildNumber;
        private String buildDate;
        private String copyright;
        private String licenseID;
        private String licenseVersion;
        private String aboutTitle;
        private String aboutIcon;
        private String aboutDesc;
        private String aboutURL;
        private String patchLevel;
        private String patchDate;

        private Component(String componentPackage) {
            this.packageName = componentPackage;
        }
        public String getComponentPackage() {
            return packageName;
        }
        public String getTitle() {
            return this.title;
        }
        public String getDescription() {
            return this.desc;
        }
        public String getReleaseNumber() {
            return this.releaseNumber;
        }
        public String getReleaseDate() {
            return this.releaseDate;
        }
        public String getBuildNumber() {
            return this.buildNumber;
        }
        public String getBuildDate() {
            return this.buildDate;
        }
        public String getCopyright() {
            return this.copyright;
        }
        public String getLicenseID() {
            return this.licenseID;
        }
        public String getLicenseVersion() {
            return this.licenseVersion;
        }
        public String getAboutTitle() {
            return this.aboutTitle;
        }
        public String getAboutIcon() {
            return this.aboutIcon;
        }
        public String getAboutDescription() {
            return this.aboutDesc;
        }
        public String getAboutURL() {
            return this.aboutURL;
        }
        public String getPatchLevel() {
            return this.patchLevel;
        }
        public String getPatchDate() {
            return this.patchDate;
        }
        void setPatchLevel( String value ) {
            if ( value == null ) {
                value = ""; //$NON-NLS-1$
            }
            this.patchLevel = value;
        }
        void setPatchDate( String value ) {
            if ( value == null ) {
                value = ""; //$NON-NLS-1$
            }
            this.patchDate = value;
        }
        void setTitle( String value ) {
            if ( value == null ) {
                value = ""; //$NON-NLS-1$
            }
            this.title = value;
        }
        void setDescription( String value ) {
            if ( value == null ) {
                value = ""; //$NON-NLS-1$
            }
            this.desc = value;
        }
        void setReleaseNumber( String value ) {
            if ( value == null ) {
                value = ""; //$NON-NLS-1$
            }
            this.releaseNumber = value;
        }
        void setReleaseDate( String value ) {
            if ( value == null ) {
                value = ""; //$NON-NLS-1$
            }
            this.releaseDate = value;
        }
        void setBuildNumber( String value ) {
            if ( value == null ) {
                value = ""; //$NON-NLS-1$
            }
            this.buildNumber = value;
        }
        void setBuildDate( String value ) {
            if ( value == null ) {
                value = ""; //$NON-NLS-1$
            }
            this.buildDate = value;
        }
        void setCopyright( String value ) {
            if ( value == null ) {
                value = ""; //$NON-NLS-1$
            }
            this.copyright = value;
        }
        void setLicenseID( String value ) {
            if ( value == null ) {
                value = ""; //$NON-NLS-1$
            }
            this.licenseID = value;
        }
        void setLicenseVersion( String value ) {
            if ( value == null ) {
                value = ""; //$NON-NLS-1$
            }
            this.licenseVersion = value;
        }
        void setAboutTitle( String value ) {
            if ( value == null ) {
                value = ""; //$NON-NLS-1$
            }
            this.aboutTitle = value;
        }
        void setAboutIcon( String value ) {
            if ( value == null ) {
                value = ""; //$NON-NLS-1$
            }
            this.aboutIcon = value;
        }
        void setAboutDescription( String value ) {
            if ( value == null ) {
                value = ""; //$NON-NLS-1$
            }
            this.aboutDesc = value;
        }
        void setAboutURL( String value ) {
            if ( value == null ) {
                value = "www.metamatrix.com"; //$NON-NLS-1$
            }
            this.aboutURL = value;
        }

        public boolean equals(Object obj) {
            if (obj==this) {
                return true;
            }

            if (obj instanceof Component) {
                Component component = (Component)obj;
                if (component.getComponentPackage().equals(this.getComponentPackage())) {
                    return true;
                }
            }
            return false;
        }


        public String toString() {
            StringBuffer sb = new StringBuffer(" Component Package:     "); //$NON-NLS-1$
            sb.append(packageName);
            sb.append(LINE_SEPARATOR);
            sb.append("   Title:               "); //$NON-NLS-1$
            sb.append(title);
            sb.append(LINE_SEPARATOR);
            sb.append("   Description:         "); //$NON-NLS-1$
            sb.append(LINE_SEPARATOR);
            sb.append(desc);
            sb.append("   Release Number:      "); //$NON-NLS-1$
            sb.append(releaseNumber);
            sb.append(LINE_SEPARATOR);
            sb.append("   Release Date:        "); //$NON-NLS-1$
            sb.append(releaseDate);
            sb.append(LINE_SEPARATOR);
            sb.append("   Build Number:        "); //$NON-NLS-1$
            sb.append(buildNumber);
            sb.append(LINE_SEPARATOR);
            sb.append("   Build Date:          "); //$NON-NLS-1$
            sb.append(buildDate);
            sb.append(LINE_SEPARATOR);
            sb.append("   Copyright:           "); //$NON-NLS-1$
            sb.append(copyright);
            sb.append(LINE_SEPARATOR);
            sb.append("   License ID:          "); //$NON-NLS-1$
            sb.append(licenseID);
            sb.append(LINE_SEPARATOR);
            sb.append("   License Version:     "); //$NON-NLS-1$
            sb.append(licenseVersion);
            sb.append(LINE_SEPARATOR);
            sb.append("   About Title:         "); //$NON-NLS-1$
            sb.append(aboutTitle);
            sb.append(LINE_SEPARATOR);
            sb.append("   About Icon:          "); //$NON-NLS-1$
            sb.append(aboutIcon);
            sb.append(LINE_SEPARATOR);
            sb.append("   About Description:   "); //$NON-NLS-1$
            sb.append(aboutDesc);
            sb.append(LINE_SEPARATOR);
            sb.append("   About URL:           "); //$NON-NLS-1$
            sb.append(aboutURL);
            return sb.toString();
        }
    }




}
