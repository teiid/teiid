/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
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

package com.metamatrix.api.core.xmi;

import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.output.XMLOutputter;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.internal.core.xml.JdomHelper;

/**
 * Class that represents the content of an XMI file's header.
 * <p>
 * The XMI processor is designed to hide as much as possible the semantics of XMI by simply constructing
 * {@link FeatureInfo} and {@link EntityInfo} instances as an XMI stream is processed.  These info classes are
 * thus similar to SAX events of a SAX XML document parser.
 * </p>
 */
public final class XMIHeader {

    /** Default indentation upon write */
    public static final String DEFAULT_INDENT = "  "; //$NON-NLS-1$

    private Documentation documentation;
    private List models;
    private List metamodels;
    private List metametamodels;
    private List imports;

    /**
     * Constructor for XMIHeader.
     */
    public XMIHeader() {
        this.documentation = new Documentation();
        this.models = new ArrayList();
        this.metamodels = new ArrayList();
        this.metametamodels = new ArrayList();
        this.imports = new ArrayList();
    }

    /**
     * Get the Documentation object.
     * @return the documentation; never null
     */
    public Documentation getDocumentation() {
        return this.documentation;
    }
    /**
     * Get the models
     * @return the Collection of {@link Model} instances
     */
    public Collection getModels() {
        return this.models;
    }
    /**
     * Get the metamodels
     * @return the Collection of {@link MetaModel} instances
     */
    public Collection getMetaModels() {
        return this.metamodels;
    }
    /**
     * Get the meta-metamodels
     * @return the Collection of {@link MetaMetaModel} instances
     */
    public Collection getMetaMetaModels() {
        return this.metametamodels;
    }
    /**
     * Get the imports
     * @return the Collection of {@link Import} instances
     */
    public Collection getImports() {
        return this.imports;
    }

    /**
     * Add a model to this header.
     * @param name the name of the model
     * @param version the version of the model
     * @param href the URI of the model
     */
    public void addModel( String name, String version, String href ) {
        this.models.add( new Model(name,version,href) );
    }
    /**
     * Add a model to this header.
     * @param name the name of the model
     * @param version the version of the model
     * @param href the URI of the model
     * @param content the value of the "xmi.model" tag
     */
    public void addModel( String name, String version, String href, String content ) {
        this.models.add( new Model(name,version,href,content) );
    }
    /**
     * Add a metamodel to this header.
     * @param name the name of the metamodel
     * @param version the version of the metamodel
     * @param href the URI of the metamodel
     */
    public void addMetaModel( String name, String version, String href ) {
        this.metamodels.add( new MetaModel(name,version,href) );
    }
    /**
     * Add a metamodel to this header.
     * @param name the name of the metamodel
     * @param version the version of the metamodel
     * @param href the URI of the metamodel
     * @param content the value of the "xmi.metamodel" tag
     */
    public void addMetaModel( String name, String version, String href, String content ) {
        this.metamodels.add( new MetaModel(name,version,href,content) );
    }
    /**
     * Add a metametamodel to this header.
     * @param name the name of the metametamodel
     * @param version the version of the metametamodel
     * @param href the URI of the metametamodel
     */
    public void addMetaMetaModel( String name, String version, String href ) {
        this.metametamodels.add( new MetaMetaModel(name,version,href) );
    }
    /**
     * Add a metametamodel to this header.
     * @param name the name of the metametamodel
     * @param version the version of the metametamodel
     * @param href the URI of the metametamodel
     * @param content the value of the "xmi.metametamodel" tag
     */
    public void addMetaMetaModel( String name, String version, String href, String content ) {
        this.metametamodels.add( new MetaMetaModel(name,version,href,content) );
    }
    /**
     * Add an import to this header.
     * @param name the name of the imported file/model
     * @param version the version of the imported file/model
     * @param href the URI of the imported file/model
     */
    public void addImport( String name, String version, String href ) {
        this.imports.add( new Import(name,version,href) );
    }
    /**
     * Add an import to this header.
     * @param name the name of the imported file/model
     * @param version the version of the imported file/model
     * @param href the URI of the imported file/model
     * @param content the value of the "xmi.import" tag
     */
    public void addImport( String name, String version, String href, String content ) {
        this.imports.add( new Import(name,version,href,content) );
    }

    /**
     * Get the XML fragment that represents the header
     * @param indent the String value to use for indentation
     * @param newlines true if tags should be written on separate lines, or false
     * if the output should be condensed
     * @return the stringified form of the XML fragment
     */
    public String getXMIFragment( String indent, boolean newlines ) {
    	//StringWriter writer = new StringWriter();
        Document doc = new Document( new Element(XMIConstants.ElementName.XMI) );
        XMIHeader.applyHeader(doc, this);

        try{
            XMLOutputter outputter = new XMLOutputter(JdomHelper.getFormat(indent, newlines));
            StringWriter writer = new StringWriter();
            outputter.output( doc, writer );

            return writer.getBuffer().toString();
        }catch(IOException e){
        	throw new MetaMatrixRuntimeException(e, ErrorMessageKeys.API_ERR_0022, CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0022) );
        }
    }

    /**
     * Get the XML fragment that represents the header, using the default indentation
     * and new lines for each tag.
     * @return the stringified form of the XML fragment
     */
    public String getXMIFragment() {
        return getXMIFragment(DEFAULT_INDENT,true);
    }


    /**
     * Determine whether this object is equivalent to the supplied object.
     * @param obj the object to be compared with this
     * @return true if the objects are equivalent, or false otherwise.
     */
    public boolean equals(Object obj) {
        if ( !(obj instanceof XMIHeader)) {
                return false;
            }
        XMIHeader that = (XMIHeader) obj;

        if ( ! that.getDocumentation().equals(this.documentation))  {
            return false;
        }

        List thatModels = (List) that.getModels();
        if (this.models.size() != thatModels.size()) {
            return false;
        }
        for (int i=0; i<this.models.size(); i++) {
            if (! this.models.get(i).equals(thatModels.get(i)) ) {
                return false;
            }
        }

        List thatMetamodels = (List) that.getMetaModels();
        if (this.metamodels.size() != thatMetamodels.size()) {
            return false;
        }
        for (int i=0; i<this.metamodels.size(); i++) {
            if (! this.metamodels.get(i).equals(thatMetamodels.get(i)) ) {
                return false;
            }
        }

        List thatMetametamodels = (List) that.getMetaMetaModels();
        if (this.metametamodels.size() != thatMetametamodels.size()) {
            return false;
        }
        for (int i=0; i<this.metametamodels.size(); i++) {
            if (! this.metametamodels.get(i).equals(thatMetametamodels.get(i)) ) {
                return false;
            }
        }

        List thatImports = (List) that.getImports();
        if (this.imports.size() != thatImports.size()) {
            return false;
        }
        for (int i=0; i<this.imports.size(); i++) {
            if (! this.imports.get(i).equals(thatImports.get(i)) ) {
                return false;
            }
        }
        return true;
    }

    /**
     * Class that represents the "xmi.document" portion of the header
     */
    public static class Documentation {
        private String owner;
        private String contact;
        private String longDescription;
        private String shortDescription;
        private String exporter;
        private String exporterVersion;
        private String exporterID;
        private String notice;
        private String text;

        Documentation() {
        }
        /**
         * Return the owner
         * @return the owner
         */
        public String getOwner() {
            return this.owner;
        }
        /**
         * Set the owner
         * @param owner the owner
         */
        public void setOwner(String owner) {
            this.owner = owner;
        }
        /**
         * Return the contact
         * @return the contact
         */
        public String getContact() {
            return this.contact;
        }
        /**
         * Set the contact
         * @param contact the contact
         */
        public void setContact(String contact) {
            this.contact = contact;
        }
        /**
         * Return the long description
         * @return the long description
         */
        public String getLongDescription() {
            return this.longDescription;
        }
        /**
         * Set the long description
         * @param longDescription the long description
         */
        public void setLongDescription(String longDescription) {
            this.longDescription = longDescription;
        }
        /**
         * Return the short description
         * @return the short description
         */
        public String getShortDescription() {
            return this.shortDescription;
        }
        /**
         * Set the short description
         * @param shortDescription the short description
         */
        public void setShortDescription(String shortDescription) {
            this.shortDescription = shortDescription;
        }
        /**
         * Return the exporter
         * @return the exporter
         */
        public String getExporter() {
            return this.exporter;
        }
        /**
         * Set the exporter
         * @param exporter the exporter
         */
        public void setExporter(String exporter) {
            this.exporter = exporter;
        }
        /**
         * Return the exporter version
         * @return the exporter version
         */
        public String getExporterVersion() {
            return this.exporterVersion;
        }
        /**
         * Set the exporter version
         * @param exporterVersion the exporter version
         */
        public void setExporterVersion(String exporterVersion) {
            this.exporterVersion = exporterVersion;
        }
        /**
         * Return the exporter ID
         * @return the exporter ID
         */
        public String getExporterID() {
            return this.exporterID;
        }
        /**
         * Set the exporter ID
         * @param exporterID the exporter ID
         */
        public void setExporterID(String exporterID) {
            this.exporterID = exporterID;
        }
        /**
         * Return the notice
         * @return the notice
         */
        public String getNotice() {
            return this.notice;
        }
        /**
         * Set the notice
         * @param notice the notice
         */
        public void setNotice(String notice) {
            this.notice = notice;
        }
        /**
         * Return the content of the "xmi.document" tag
         * @return the text
         */
        public String getText() {
            return this.text;
        }
        /**
         * Set the content of the "xmi.document" tag
         * @param text the text
         */
        public void setText(String text) {
            this.text = text;
        }
        /**
         * Determine whether two objects are equivalent
         * @param obj the object to be compared
         * @return true if both Documentation objects are equivalent, or false otherwise
         */
        public boolean equals(Object obj) {
            if ( !(obj instanceof Documentation)) {
                return false;
            }
            Documentation that = (Documentation) obj;
            return (that.getOwner() == null ? this.owner == null : that.getOwner().equals(this.owner)) &&
                   (that.getContact() == null ? this.contact == null : that.getContact().equals(this.contact)) &&
                   (that.getLongDescription() == null ? this.longDescription == null : that.getLongDescription().equals(this.longDescription)) &&
                   (that.getShortDescription() == null ? this.shortDescription == null : that.getShortDescription().equals(this.shortDescription)) &&
                   (that.getExporter() == null ? this.exporter == null : that.getExporter().equals(this.exporter)) &&
                   (that.getExporterVersion() == null ? this.exporterVersion == null : that.getExporterVersion().equals(this.exporterVersion)) &&
                   (that.getExporterID() == null ? this.exporterID == null : that.getExporterID().equals(this.exporterID)) &&
                   (that.getNotice() == null ? this.notice == null : that.getNotice().equals(this.notice)) &&
                   (that.getText() == null ? this.text == null : that.getText().equals(this.text));
        }
    }

    /**
     * Class that represents the "xmi.model" tag in the header
     */
    public static class Model {
        private String href;
        private String name;
        private String version;
        private String content;

        Model( String name, String version, String href, String content ) {
            if ( name != null && name.length() !=  0 ) {
                this.name = name;
            }
            if ( version != null && version.length() !=  0 ) {
                this.version = version;
            }
            if ( href != null && href.length() !=  0 ) {
                this.href = href;
            }
            if ( content != null && content.length() !=  0 ) {
                this.content = content;
            }
        }
        Model( String name, String version, String href ) {
            this(name,version,href,null);
        }
        /**
         * Return the model name
         * @return the model name
         */
        public String getName() {
            return this.name;
        }
        /**
         * Return the model version
         * @return the version
         */
        public String getVersion() {
            return this.version;
        }
        /**
         * Return the URI of the model
         * @return the URI
         */
        public String getHref() {
            return this.href;
        }
        /**
         * Set the URI of the model
         * @param href the URI of the model
         */
        public void setHref(String href) {
            this.href = href;
        }
        /**
         * Return the content of the "xmi.model" tag
         * @return the text
         */
        public String getContent() {
            if (this.content == null) {
                return ""; //$NON-NLS-1$
            }
            return this.content;
        }
        /**
         * Set the content of the "xmi.model" tag
         * @param content the text
         */
        public void addContent(String content) {
            this.content = content;
        }
        /**
         * Determine whether two objects are equivalent
         * @param obj the object to be compared
         * @return true if both Model objects are equivalent, or false otherwise
         */
        public boolean equals(Object obj) {
            if ( !(obj instanceof Model)) {
                return false;
            }
            Model that = (Model) obj;
            return (that.getHref() == null ? this.getHref() == null : that.getHref().equals(this.getHref())) &&
                   (that.getName() == null ? this.getName() == null : that.getName().equals(this.getName())) &&
                   (that.getVersion() == null ? this.getVersion() == null : that.getVersion().equals(this.getVersion())) &&
                   (that.getContent() == null ? this.getContent() == null : that.getContent().equals(this.getContent()));
        }
    }

    /**
     * Class that represents the "xmi.metamodel" tag in the header.
     */
    public static class MetaModel extends Model {
        MetaModel( String name, String version, String href ) {
            super(name,version,href);
        }
        MetaModel( String name, String version, String href, String content ) {
            super(name,version,href,content);
        }
    }
    /**
     * Class that represents the "xmi.metametamodel" tag in the header.
     */
    public static class MetaMetaModel extends Model {
        MetaMetaModel( String name, String version, String href ) {
            super(name,version,href);
        }
        MetaMetaModel( String name, String version, String href, String content ) {
            super(name,version,href,content);
        }
    }
    /**
     * Class that represents the "xmi.import" tag in the header.
     */
    public static class Import {
        private String href;
        private String name;
        private String version;
        private String content;

        Import( String name, String version, String href, String content ) {
            if ( name != null && name.length() !=  0 ) {
                this.name = name;
            }
            if ( version != null && version.length() !=  0 ) {
                this.version = version;
            }
            if ( href != null && href.length() !=  0 ) {
                this.href = href;
            }
            if ( content != null && content.length() !=  0 ) {
                this.content = content;
            }
        }
        Import( String name, String version, String href ) {
            this(name,version,href,null);
        }
        /**
         * Return the import name
         * @return the name
         */
        public String getName() {
            return this.name;
        }
        /**
         * Return the import version
         * @return the version
         */
        public String getVersion() {
            return this.version;
        }
        /**
         * Return the URI for the import
         * @return the URI
         */
        public String getHref() {
            return this.href;
        }
        /**
         * Set the URI for the import
         * @param href the URI
         */
        public void setHref(String href) {
            this.href = href;
        }
        /**
         * Return the content of the "xmi.model" tag
         * @return the text
         */
        public String getContent() {
            if (this.content == null) {
                return ""; //$NON-NLS-1$
            }
            return this.content;
        }
        /**
         * Set the content of the "xmi.model" tag
         * @param content the content
         */
        public void addContent(String content) {
            this.content = content;
        }
        /**
         * Determine whether two objects are equivalent
         * @param obj the object to be compared
         * @return true if both Import objects are equivalent, or false otherwise
         */
        public boolean equals(Object obj) {
            if ( !(obj instanceof Import)) {
                return false;
            }
            Import that = (Import) obj;
            return (that.getHref() == null ? this.getHref() == null : that.getHref().equals(this.getHref())) &&
                   (that.getName() == null ? this.getName() == null : that.getName().equals(this.getName())) &&
                   (that.getVersion() == null ? this.getVersion() == null : that.getVersion().equals(this.getVersion())) &&
                   (that.getContent() == null ? this.getContent() == null : that.getContent().equals(this.getContent()));
        }
    }

    /**
     * Apply the supplied XMI header to the XML (JDOM) Document.
     * @param doc the JDOM document that is to contain the XMI header
     * @param xmiHeader the header information to be written to the document
     */
    public static void applyHeader(Document doc, XMIHeader xmiHeader ) {
        Element headerTag = new Element(XMIConstants.ElementName.HEADER);
        doc.setRootElement(headerTag);

        applyHeader(headerTag, xmiHeader, true);
    }

    /**
     * Apply the supplied XMI header to the specified element in an XML (JDOM) Document.
     * @param root the element under which the header fragment is to be written;
     * this should be the "XMI" root tag of an XMI document.
     * @param xmiHeader the header information to be written to the document
     * @param newDocument true if this document may be a new document, or false if
     * an existing header (if one exists) should be replaced
     */
    public static void applyHeader( Element root, XMIHeader xmiHeader, boolean newDocument ) {
        Element headerTag = root;
        if(!newDocument){
            // If there is already a header, the remove it and add a new one ...
            headerTag = null;
            headerTag = root.getChild(XMIConstants.ElementName.HEADER);
            Element contentTag = root.getChild(XMIConstants.ElementName.CONTENT);
            if ( headerTag != null ) {
                root.removeContent(headerTag);
                root.removeContent(contentTag);
            }
            headerTag = new Element(XMIConstants.ElementName.HEADER);
            root.addContent(headerTag);
            if ( contentTag != null ) {
                root.addContent(contentTag);        // to maintain order !!
            }
        }

        // Add the documentation ...
        Element documentation = new Element(XMIConstants.ElementName.DOCUMENTATION);

        XMIHeader.Documentation xmiDoc = xmiHeader.getDocumentation();
        addElement( documentation, XMIConstants.ElementName.OWNER, xmiDoc.getOwner() );
        addElement( documentation, XMIConstants.ElementName.LONG_DESCRIPTION, xmiDoc.getLongDescription() );
        addElement( documentation, XMIConstants.ElementName.SHORT_DESCRIPTION, xmiDoc.getShortDescription() );
        addElement( documentation, XMIConstants.ElementName.CONTACT, xmiDoc.getContact() );
        addElement( documentation, XMIConstants.ElementName.EXPORTER, xmiDoc.getExporter() );
        addElement( documentation, XMIConstants.ElementName.EXPORTER_ID, xmiDoc.getExporterID() );
        addElement( documentation, XMIConstants.ElementName.EXPORTER_VERSION, xmiDoc.getExporterVersion() );
        addElement( documentation, XMIConstants.ElementName.NOTICE, xmiDoc.getNotice() );
        addText( documentation, xmiDoc.getText() );
        if ( documentation.getChildren().size() != 0 || documentation.getText().length() != 0 ) {
            headerTag.addContent(documentation);
        }

        // Add the models ...
        Iterator iter = xmiHeader.getModels().iterator();
        while ( iter.hasNext() ) {
            addModel(headerTag,(XMIHeader.Model)iter.next());
        }
        iter = xmiHeader.getMetaModels().iterator();
        while ( iter.hasNext() ) {
            addModel(headerTag,(XMIHeader.Model)iter.next());
        }
        iter = xmiHeader.getMetaMetaModels().iterator();
        while ( iter.hasNext() ) {
            addModel(headerTag,(XMIHeader.Model)iter.next());
        }
        iter = xmiHeader.getImports().iterator();
        while ( iter.hasNext() ) {
            addImport(headerTag,(XMIHeader.Import)iter.next());
        }
    }

    /**
     * Helper method.  This method does nothing
     * (and returns false) if there is no need or insufficient information to create the Element.
     * @param parent the parent Element
     * @param tagName the name of the new Element
     * @param text the content for the new Element
     * @return true if a new Element was created, or false otherwise
     */
    protected static boolean addElement( Element parent, String tagName, String text ) {
        if ( parent == null || text == null || text.length() == 0 ) {
            return false;
        }
        Element newTag = new Element(tagName);
        newTag.setText(text);
        parent.addContent(newTag);
        return true;
    }

    /**
     * Helper method to set the content on an Element.  This method does nothing
     * (and returns false) if there is no need to set the content.
     * @param tag the parent Element; may be null
     * @param text the content for the new Element; may be null
     * @return true if the Element's content was set, or false otherwise
     */
    protected static boolean addText( Element tag, String text ) {
        if ( tag == null || text == null || text.length() == 0 ) {
            return false;
        }
        tag.setText(text);
        return true;
    }

    /**
     * Helper method to add a model to the parent.  This method determines the correct
     * tag name given the subclass of XMIHeader.Model.
     * @param parent the parent Element; may be null
     * @param model the model to be added
     * @return true if a model tag was created, or false otherwise.
     */
    protected static boolean addModel( Element parent, XMIHeader.Model model ) {
        if ( parent == null || model == null ) {
            return false;
        }
        String tagName = XMIConstants.ElementName.MODEL;
        if ( model instanceof XMIHeader.MetaModel ) {
            tagName = XMIConstants.ElementName.METAMODEL;
        } else if ( model instanceof XMIHeader.MetaMetaModel ) {
            tagName = XMIConstants.ElementName.METAMETAMODEL;
        }
        Element newTag = new Element(tagName);
        boolean added = false;
        String name = model.getName();
        if ( name != null && name.length() != 0 ) {
            newTag.setAttribute(XMIConstants.AttributeName.XMI_NAME,name);
            added = true;
        }
        String version = model.getVersion();
        if ( version != null && version.length() != 0 ) {
            newTag.setAttribute(XMIConstants.AttributeName.XMI_VERSION,version);
            added = true;
        }
        String href = model.getHref();
        if ( href != null && href.length() != 0 ) {
            newTag.setAttribute(XMIConstants.AttributeName.HREF,href);
            added = true;
        }
        String content = model.getContent();
        if(content != null && href != null && href.length() != 0 ){
            newTag.addContent(content);
        }
        if ( added ) {
            parent.addContent(newTag);
        }
        return true;
    }

    /**
     * Helper method to add an import under the parent
     * @param parent the parent
     * @param importObj the import object to be added
     * @return true if the import object was added, or false otherwise
     */
    protected static boolean addImport( Element parent, XMIHeader.Import importObj ) {
        if ( parent == null || importObj == null ) {
            return false;
        }
        Element newTag = new Element(XMIConstants.ElementName.IMPORT);
        boolean added = false;
        String name = importObj.getName();
        if ( name != null && name.length() != 0 ) {
            newTag.setAttribute(XMIConstants.AttributeName.XMI_NAME,name);
            added = true;
        }
        String version = importObj.getVersion();
        if ( version != null && version.length() != 0 ) {
            newTag.setAttribute(XMIConstants.AttributeName.XMI_VERSION,version);
            added = true;
        }
        String href = importObj.getHref();
        if ( href != null && href.length() != 0 ) {
            newTag.setAttribute(XMIConstants.AttributeName.HREF,href);
            added = true;
        }
        String content = importObj.getContent();
        if(content != null && content.length() != 0 ){
            newTag.addContent(content);
            added = true;
        }
        if ( added ) {
            parent.addContent(newTag);
        }
        return true;
    }
    /**
     * Method to print the contents of the XMI Header object.
     * @param stream the stream
     */
    public void print( PrintStream stream ) {
        stream.println("XMI Header:"); //$NON-NLS-1$
        //stream.println("    XMI Version   " + this.getXMIVersion() );
        //stream.println("    Timestamp     " + this.getTimestamp() );
        stream.println("  Documentation"); //$NON-NLS-1$
        Documentation doc = this.getDocumentation();
        stream.println("    Owner:        " + doc.getOwner() ); //$NON-NLS-1$
        stream.println("    Contact:      " + doc.getContact() ); //$NON-NLS-1$
        stream.println("    Long Desc:    " + doc.getLongDescription() ); //$NON-NLS-1$
        stream.println("    Short Desc:   " + doc.getShortDescription() ); //$NON-NLS-1$
        stream.println("    Notice:       " + doc.getNotice() ); //$NON-NLS-1$
        stream.println("    Exporter:     " + doc.getExporter() ); //$NON-NLS-1$
        stream.println("    ExporterID:   " + doc.getExporterID() ); //$NON-NLS-1$
        stream.println("    Exporter Ver: " + doc.getExporterVersion() ); //$NON-NLS-1$
        stream.println("  Namespaces"); //$NON-NLS-1$
        stream.println("  Models"); //$NON-NLS-1$
        Iterator iter = this.getModels().iterator();
        while ( iter.hasNext() ) {
            Model model = (Model) iter.next();
            stream.println("    " + model.getName() + ", " + model.getVersion() + ", " + model.getHref() + ", \"" + model.getContent() + "\"" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        }
        stream.println("  MetaModels"); //$NON-NLS-1$
        iter = this.getMetaModels().iterator();
        while ( iter.hasNext() ) {
            Model model = (Model) iter.next();
            stream.println("    " + model.getName() + ", " + model.getVersion() + ", " + model.getHref() + ", \"" + model.getContent() + "\"" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        }
        stream.println("  MetaMetaModels"); //$NON-NLS-1$
        iter = this.getMetaMetaModels().iterator();
        while ( iter.hasNext() ) {
            Model model = (Model) iter.next();
            stream.println("    " + model.getName() + ", " + model.getVersion() + ", " + model.getHref() + ", \"" + model.getContent() + "\"" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        }
        stream.println("  Imports"); //$NON-NLS-1$
        iter = this.getImports().iterator();
        while ( iter.hasNext() ) {
            Import imp = (Import) iter.next();
            stream.println("    " + imp.getName() + ", " + imp.getVersion() + ", " + imp.getHref() + ", \"" + imp.getContent() + "\"" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        }
    }
}

