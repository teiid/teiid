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

package com.metamatrix.vdb.materialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.vdb.materialization.template.ExpandedTemplate;
import com.metamatrix.vdb.materialization.template.Template;
import com.metamatrix.vdb.materialization.template.TemplateData;
import com.metamatrix.vdb.materialization.template.TemplateExpander;


/** 
 * @since 4.2
 */
public class MaterializedViewScriptGeneratorImpl implements MaterializedViewScriptGenerator {
    
    private static final String TEMPLATE_PATH = "com/metamatrix/vdb/materialization/template/"; //$NON-NLS-1$
    private static final String PARENT_TEMPLATE = "scriptMaterializedView"; //$NON-NLS-1$
    private static final String PLATFORM_SPECIALIZED_TEMPLATE = "loadMaterializedView_"; //$NON-NLS-1$
    private static final String TEMPLATE_EXT = ".stg"; //$NON-NLS-1$
    
    private TemplateData templateData;
    
    /**
     * Default ctor. 
     * @param materializedViewTemplateData The template data for a materialization.
     * 
     * @since 4.2
     */
    public MaterializedViewScriptGeneratorImpl(TemplateData materializedViewTemplateData) {
        this.templateData = materializedViewTemplateData;
    }
    
    /** 
     * @throws IOException
     * @see com.metamatrix.vdb.materialization.MaterializedViewScriptGenerator#generateMaterializationTruncateScript(OutputStream, DatabaseDialect)
     * @since 4.2
     */
    public void generateMaterializationTruncateScript(OutputStream stream, DatabaseDialect dialect) throws IOException {
        generateTemplate(Template.TRUNCATE, this.templateData, dialect, stream);
    }

    /** 
     * @see com.metamatrix.vdb.materialization.MaterializedViewScriptGenerator#generateMaterializationLoadScript(OutputStream)
     * @since 4.2
     */
    public void generateMaterializationLoadScript(OutputStream stream) throws IOException {
        generateTemplate(Template.LOAD, this.templateData, DatabaseDialect.METAMATRIX, stream);
    }

    /** 
     * @see com.metamatrix.vdb.materialization.MaterializedViewScriptGenerator#generateMaterializationSwapScript(OutputStream, DatabaseDialect)
     * @since 4.2
     */
    public void generateMaterializationSwapScript(OutputStream stream, DatabaseDialect dialect) throws IOException {
        generateTemplate(Template.SWAP, this.templateData, dialect, stream);
    }
    
    /** 
     * @see com.metamatrix.vdb.materialization.MaterializedViewScriptGenerator#generateMaterializationConnectionPropFile(java.io.OutputStream)
     * @since 4.2
     */
    public void generateMaterializationConnectionPropFile(OutputStream stream) throws IOException {
        generateTemplate(Template.CONN_PROPS, this.templateData, DatabaseDialect.CONNECTION_PROPS, stream);
    }
    
    /**
     *  
     * @param command
     * @param data
     * @param database
     * @param stream
     * @throws IOException
     * @since 4.2
     */
    private void generateTemplate(String command, TemplateData data, DatabaseDialect database, OutputStream stream) throws IOException {
        String name = database.getType() + "_" + command; //$NON-NLS-1$
        TemplateExpander expander = new TemplateExpander(data, database);
        Reader[] templateReaders = getTemplateReaders(command, database);
        ExpandedTemplate template = expander.expand(name, templateReaders, command);
        closeReaders(templateReaders);
        toStream(template, stream);
    }

    /**
     * Get the heirarchy of template readers for the given database platform. 
     * <p><b>Close readers when done!!!</b></p>
     * @param command
     * @param database
     * @return
     * @since 4.2
     */
    private Reader[] getTemplateReaders(String command, DatabaseDialect database) throws IOException {
        InputStreamReader parent = getReader(PARENT_TEMPLATE);
        InputStreamReader child = getReader(PLATFORM_SPECIALIZED_TEMPLATE + database);
        return new Reader[] {parent, child};
    }
    
    private void closeReaders(Reader[] readers) throws IOException {
        for ( int i=0; i<readers.length; ++i ) {
            readers[i].close();
        }
    }

    /**
     * Get an InputStreamReader for the given filename. 
     * @param fileName the file for which to get the InputStreamReader.
     * @return InputStreamReader for the file.
     * @since 4.2
     */
    private InputStreamReader getReader(String fileName) throws IOException {
        String templateName = TEMPLATE_PATH + fileName + TEMPLATE_EXT; 
        InputStream inputStream = MaterializedViewScriptGeneratorImpl.class.getClassLoader().getResourceAsStream(templateName);
        if ( inputStream == null ) {
            throw new MetaMatrixRuntimeException("Unable to find resource: " + templateName); //$NON-NLS-1$
        }
        return new InputStreamReader(inputStream);
    }

    
    /**
     * Stream the contents to an <code>OutputStream</code>. 
     * @param contents the string to stream
     * @param stream the stream to write to
     * @throws IOException if the given OutputStream has problems
     */
    private void toStream(ExpandedTemplate template, OutputStream stream) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(stream);
        writer.write(template.contents, 0, template.contents.length());
        writer.flush();
    }
}
