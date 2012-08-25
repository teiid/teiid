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
package org.teiid.jboss.rest;

import static org.objectweb.asm.Opcodes.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.FileUtils;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.jboss.IntegrationPlugin;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.Schema;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.query.metadata.TransformationMetadata;



@SuppressWarnings("nls")
public class RestASMBasedWebArchiveBuilder {
	 

	public byte[] createRestArchive(VDBMetaData vdb) throws FileNotFoundException, IOException  {
		MetadataStore metadataStore = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
		
		Properties props = new Properties();
		props.setProperty("${context-name}", vdb.getName() + "_" + vdb.getVersion());
		props.setProperty("${vdb-name}", vdb.getName());
		props.setProperty("${vdb-version}", String.valueOf(vdb.getVersion()));
		
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		ZipOutputStream out = new ZipOutputStream(byteStream); 
		writeEntry("WEB-INF/web.xml", out, replaceTemplates(getFileContents("rest-war/web.xml"), props).getBytes());
		writeEntry("WEB-INF/jboss-web.xml", out, replaceTemplates(getFileContents("rest-war/jboss-web.xml"), props).getBytes());
		
		ArrayList<String> applicationViews = new ArrayList<String>();
		for (ModelMetaData model:vdb.getModelMetaDatas().values()) {
			Schema schema = metadataStore.getSchema(model.getName());
			byte[] viewContents = getViewClass(vdb.getName(), vdb.getVersion(), model.getName(), schema);
			if (viewContents != null) {
				writeEntry("WEB-INF/classes/org/teiid/jboss/rest/"+model.getName()+".class", out, viewContents);
				applicationViews.add(schema.getName());
			}
		}
		writeEntry("WEB-INF/classes/org/teiid/jboss/rest/TeiidRestApplication.class", out, getApplicationClass(applicationViews));
		writeEntry("META-INF/MANIFEST.MF", out, getFileContents("rest-war/MANIFEST.MF").getBytes());
		
		out.close();
		return byteStream.toByteArray();
	}

	private void writeEntry(String name, ZipOutputStream out, byte[] contents) throws IOException {
		ZipEntry e = new ZipEntry(name); 
		out.putNextEntry(e);
		FileUtils.write(new ByteArrayInputStream(contents), out, 1024);
		out.closeEntry();
	}

	private String getFileContents(String file) throws IOException {
		InputStream in = this.getClass().getClassLoader().getResourceAsStream(file); 
		Reader reader = new InputStreamReader(in); 
		String webXML = ObjectConverterUtil.convertToString(reader);
		return webXML;
	}
	
	private String replaceTemplates(String orig, Properties replacements) {
		for (String key:replacements.stringPropertyNames()) {
			orig = StringUtil.replace(orig, key, replacements.getProperty(key));	
		}
		return orig;
	}
    
    private static ArrayList<String> getPathParameters(String uri ) {
        ArrayList<String> pathParams = new ArrayList<String>();
        String param;
        if (uri.contains("{")) { 
            while (uri.indexOf("}") > -1) { 
                int start = uri.indexOf("{"); 
                int end = uri.indexOf("}"); 
                param = uri.substring(start + 1, end);
                uri = uri.substring(end + 1);
                pathParams.add(param);
            }
        }
        return pathParams;
    }
    
    public byte[] getApplicationClass(ArrayList<String> models) {
    	ClassWriter cw = new ClassWriter(0);
    	FieldVisitor fv;
    	MethodVisitor mv;

    	cw.visit(V1_5, ACC_PUBLIC + ACC_SUPER, "org/teiid/jboss/rest/TeiidRestApplication", null, "javax/ws/rs/core/Application", null);

    	{
    	fv = cw.visitField(ACC_PRIVATE, "singletons", "Ljava/util/Set;", "Ljava/util/Set<Ljava/lang/Object;>;", null);
    	fv.visitEnd();
    	}
    	
    	{
    	fv = cw.visitField(ACC_PRIVATE, "empty", "Ljava/util/Set;", "Ljava/util/Set<Ljava/lang/Class<*>;>;", null);
    	fv.visitEnd();
    	}
    	
    	{
    	mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
    	mv.visitCode();
    	mv.visitVarInsn(ALOAD, 0);
    	mv.visitMethodInsn(INVOKESPECIAL, "javax/ws/rs/core/Application", "<init>", "()V");
    	mv.visitVarInsn(ALOAD, 0);
    	mv.visitTypeInsn(NEW, "java/util/HashSet");
    	mv.visitInsn(DUP);
    	mv.visitMethodInsn(INVOKESPECIAL, "java/util/HashSet", "<init>", "()V");
    	mv.visitFieldInsn(PUTFIELD, "org/teiid/jboss/rest/TeiidRestApplication", "singletons", "Ljava/util/Set;");
    	mv.visitVarInsn(ALOAD, 0);
    	mv.visitTypeInsn(NEW, "java/util/HashSet");
    	mv.visitInsn(DUP);
    	mv.visitMethodInsn(INVOKESPECIAL, "java/util/HashSet", "<init>", "()V");
    	mv.visitFieldInsn(PUTFIELD, "org/teiid/jboss/rest/TeiidRestApplication", "empty", "Ljava/util/Set;");
    	mv.visitVarInsn(ALOAD, 0);
    	
    	for (int i = 0; i < models.size(); i++) {
	    	mv.visitFieldInsn(GETFIELD, "org/teiid/jboss/rest/TeiidRestApplication", "singletons", "Ljava/util/Set;");
	    	mv.visitTypeInsn(NEW, "org/teiid/jboss/rest/"+models.get(i));
	    	mv.visitInsn(DUP);
	    	mv.visitMethodInsn(INVOKESPECIAL, "org/teiid/jboss/rest/"+models.get(i), "<init>", "()V");
	    	mv.visitMethodInsn(INVOKEINTERFACE, "java/util/Set", "add", "(Ljava/lang/Object;)Z");
	    	mv.visitInsn(POP);
	    	if (i < models.size()-1) {
	    		mv.visitVarInsn(ALOAD, 0);
	    	}
    	}
    	
    	mv.visitInsn(RETURN);
    	mv.visitMaxs(3, 1);
    	mv.visitEnd();
    	}
    	
    	{
    	mv = cw.visitMethod(ACC_PUBLIC, "getClasses", "()Ljava/util/Set;", "()Ljava/util/Set<Ljava/lang/Class<*>;>;", null);
    	mv.visitCode();
    	mv.visitVarInsn(ALOAD, 0);
    	mv.visitFieldInsn(GETFIELD, "org/teiid/jboss/rest/TeiidRestApplication", "empty", "Ljava/util/Set;");
    	mv.visitInsn(ARETURN);
    	mv.visitMaxs(1, 1);
    	mv.visitEnd();
    	}
    	{
    	mv = cw.visitMethod(ACC_PUBLIC, "getSingletons", "()Ljava/util/Set;", "()Ljava/util/Set<Ljava/lang/Object;>;", null);
    	mv.visitCode();
    	mv.visitVarInsn(ALOAD, 0);
    	mv.visitFieldInsn(GETFIELD, "org/teiid/jboss/rest/TeiidRestApplication", "singletons", "Ljava/util/Set;");
    	mv.visitInsn(ARETURN);
    	mv.visitMaxs(1, 1);
    	mv.visitEnd();
    	}
    	cw.visitEnd();

    	return cw.toByteArray();
    }
    
    private byte[] getViewClass(String vdbName, int vdbVersion, String modelName, Schema schema) {
    	ClassWriter cw = new ClassWriter(0);
    	FieldVisitor fv;
    	MethodVisitor mv;
    	AnnotationVisitor av0;
    	boolean hasValidProcedures = false;
    	
    	cw.visit(V1_6, ACC_PUBLIC + ACC_SUPER, "org/teiid/jboss/rest/"+modelName, null, "org/teiid/jboss/rest/TeiidRSProvider", null);

    	{
    	av0 = cw.visitAnnotation("Ljavax/ws/rs/Path;", true);
    	av0.visit("value", "/"+modelName.toLowerCase());
    	av0.visitEnd();
    	}
    	cw.visitInnerClass("javax/ws/rs/core/Response$Status", "javax/ws/rs/core/Response", "Status", ACC_PUBLIC + ACC_FINAL + ACC_STATIC + ACC_ENUM);

    	{
    	mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
    	mv.visitCode();
    	mv.visitVarInsn(ALOAD, 0);
    	mv.visitMethodInsn(INVOKESPECIAL, "org/teiid/jboss/rest/TeiidRSProvider", "<init>", "()V");
    	mv.visitInsn(RETURN);
    	mv.visitMaxs(1, 1);
    	mv.visitEnd();
    	}
    	
		Collection<Procedure> procedures = schema.getProcedures().values();
		for (Procedure procedure:procedures) {
			String uri = procedure.getProperty(ResteasyEnabler.REST_NAMESPACE+"URI", false);
			String method = procedure.getProperty(ResteasyEnabler.REST_NAMESPACE+"METHOD", false);
			String contentType = procedure.getProperty(ResteasyEnabler.REST_NAMESPACE+"PRODUCES", false);
			String charSet = procedure.getProperty(ResteasyEnabler.REST_NAMESPACE+"CHARSET", false);
			if (getPathParameters(uri).size() != procedure.getParameters().size()) {
				LogManager.logWarning(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50091, procedure.getFullName()));
				continue;
			}
			if (uri != null && method != null) {
				if (contentType == null) {
					contentType = findContentType(procedure);
				}
				
				if (contentType != null) {
					contentType = contentType.toLowerCase();
					if (contentType.equals("xml")) {
						contentType = "application/xml"; 
					}
					else if (contentType.equals("json")) {
						contentType = "application/json";
					}
					else if (contentType.equals("plain")) {
						contentType = "text/plain";
					}
			    	buildRestService(modelName, procedure, method, uri, cw, contentType, charSet);
			    	hasValidProcedures = true;
				}
			}
		}    	
    	
    	// getConnection method
    	{
    	mv = cw.visitMethod(ACC_PROTECTED, "getConnection", "()Ljava/sql/Connection;", null, new String[] { "java/sql/SQLException" });
    	mv.visitCode();
    	mv.visitTypeInsn(NEW, "org/teiid/jdbc/TeiidDriver");
    	mv.visitInsn(DUP);
    	mv.visitMethodInsn(INVOKESPECIAL, "org/teiid/jdbc/TeiidDriver", "<init>", "()V");
    	mv.visitVarInsn(ASTORE, 1);
    	mv.visitVarInsn(ALOAD, 1);
    	mv.visitLdcInsn("jdbc:teiid:"+vdbName+"."+vdbVersion);
    	mv.visitInsn(ACONST_NULL);
    	mv.visitMethodInsn(INVOKEVIRTUAL, "org/teiid/jdbc/TeiidDriver", "connect", "(Ljava/lang/String;Ljava/util/Properties;)Lorg/teiid/jdbc/ConnectionImpl;");
    	mv.visitInsn(ARETURN);
    	mv.visitMaxs(3, 2);
    	mv.visitEnd();
    	}
    	cw.visitEnd();

    	if (!hasValidProcedures) {
    		return null;
    	}
    	return cw.toByteArray();
    }

	private String findContentType(Procedure procedure) {
		String contentType = "plain";
		ColumnSet<Procedure> rs = procedure.getResultSet();
		if (rs != null) {
			Column returnColumn = rs.getColumns().get(0);
			if (returnColumn.getDatatype().getRuntimeTypeName().equals(DataTypeManager.DefaultDataTypes.XML)) {
				contentType = "xml"; //$NON-NLS-1$
			}
			else if (returnColumn.getDatatype().getRuntimeTypeName().equals(DataTypeManager.DefaultDataTypes.CLOB)) {
				contentType = "json";
			}
		}
		else {
			for (ProcedureParameter pp:procedure.getParameters()) {
				if (pp.getType().equals(ProcedureParameter.Type.ReturnValue)) {
					if (pp.getDatatype().getRuntimeTypeName().equals(DataTypeManager.DefaultDataTypes.XML)) {
						contentType = "xml"; //$NON-NLS-1$
					}
					else if (pp.getDatatype().getRuntimeTypeName().equals(DataTypeManager.DefaultDataTypes.CLOB)) {
						contentType = "json"; //$NON-NLS-1$
					}
				}
			}
		}
		return contentType;
	}

	private void buildRestService(String modelName, Procedure procedure,
			String method, String uri, ClassWriter cw, String contentType,
			String charSet) {
		
		List<ProcedureParameter> params = new ArrayList<ProcedureParameter>(procedure.getParameters().size());
		boolean usingReturn = false;
		for (ProcedureParameter p : procedure.getParameters()) {
			if (p.getType() == Type.In || p.getType() == Type.InOut) {
				params.add(p);
			} else if (p.getType() == Type.ReturnValue && procedure.getResultSet() == null) {
				usingReturn = true;
			}
		}
		int paramsSize = params.size();
		MethodVisitor mv;
		
		AnnotationVisitor av0;
		{
        	StringBuilder paramSignature = new StringBuilder();
        	paramSignature.append("(");
        	for (int i = 0; i < paramsSize; i++) {
        		paramSignature.append("Ljava/lang/String;");
        	}
        	paramSignature.append(")");
    		
    	mv = cw.visitMethod(ACC_PUBLIC, procedure.getName()+contentType, paramSignature+"Ljava/io/InputStream;", null, new String[] { "javax/ws/rs/WebApplicationException" });
    	{
    	av0 = mv.visitAnnotation("Ljavax/ws/rs/Produces;", true);
    	{
    	AnnotationVisitor av1 = av0.visitArray("value");
		av1.visit(null, contentType);
    	av1.visitEnd();
    	}
    	av0.visitEnd();
    	}
    	{
    	av0 = mv.visitAnnotation("Ljavax/ws/rs/"+method.toUpperCase()+";", true);
    	av0.visitEnd();
    	}
    	{
    	av0 = mv.visitAnnotation("Ljavax/ws/rs/Path;", true);
    	av0.visit("value", uri);
    	av0.visitEnd();
    	}
    	{
    	av0 = mv.visitAnnotation("Ljavax/annotation/security/PermitAll;", true);
    	av0.visitEnd();
    	}
    	
    	for (int i = 0; i < paramsSize; i++)
    	{
		av0 = mv.visitParameterAnnotation(i, "Ljavax/ws/rs/PathParam;", true);
		av0.visit("value", params.get(i).getName());
		av0.visitEnd();
		}
    	
    	mv.visitCode();
    	Label l0 = new Label();
    	Label l1 = new Label();
    	Label l2 = new Label();
    	mv.visitTryCatchBlock(l0, l1, l2, "java/sql/SQLException");
    	mv.visitLabel(l0);
    	mv.visitTypeInsn(NEW, "java/util/LinkedHashMap");
    	mv.visitInsn(DUP);
    	mv.visitMethodInsn(INVOKESPECIAL, "java/util/LinkedHashMap", "<init>", "()V");

    	mv.visitVarInsn(ASTORE, paramsSize+1);
    	for (int i = 0; i < paramsSize; i++) {
    		mv.visitVarInsn(ALOAD, paramsSize+1);
    		mv.visitLdcInsn(params.get(i).getName());
    		mv.visitVarInsn(ALOAD, i+1);
    		mv.visitMethodInsn(INVOKEINTERFACE, "java/util/Map", "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    		mv.visitInsn(POP);
    	}
    	
    	mv.visitVarInsn(ALOAD, 0);
    	
    	// send parametr type information the procedure name
    	StringBuilder sb = new StringBuilder();
    	sb.append("{ ");
    	if (usingReturn) {
    		sb.append("? = ");
    	}
    	sb.append("CALL ");
    	procedure.getSQLString(sb);
    	sb.append("(");
    	for (int i = 0; i < paramsSize; i++) {
    		if (i > 0) {
    			sb.append(", ");
    		}
    		sb.append("?");
    	}
    	sb.append(") }");
    	mv.visitLdcInsn(sb.toString());
    	
    	mv.visitVarInsn(ALOAD, paramsSize+1);
    	mv.visitLdcInsn(charSet);
    	mv.visitMethodInsn(INVOKEVIRTUAL, "org/teiid/jboss/rest/"+modelName, "execute", "(Ljava/lang/String;Ljava/util/Map;Ljava/lang/String;)Ljava/io/InputStream;");
    	mv.visitLabel(l1);
    	mv.visitInsn(ARETURN);
    	mv.visitLabel(l2);
    	mv.visitFrame(F_SAME1, 0, null, 1, new Object[] {"java/sql/SQLException"});
    	mv.visitVarInsn(ASTORE, paramsSize+1);
    	mv.visitTypeInsn(NEW, "javax/ws/rs/WebApplicationException");
    	mv.visitInsn(DUP);
    	mv.visitVarInsn(ALOAD, paramsSize+1);
    	mv.visitFieldInsn(GETSTATIC, "javax/ws/rs/core/Response$Status", "INTERNAL_SERVER_ERROR", "Ljavax/ws/rs/core/Response$Status;");
    	mv.visitMethodInsn(INVOKESPECIAL, "javax/ws/rs/WebApplicationException", "<init>", "(Ljava/lang/Throwable;Ljavax/ws/rs/core/Response$Status;)V");
    	mv.visitInsn(ATHROW);
    	mv.visitMaxs(4, paramsSize+2);
    	mv.visitEnd();
    	}
	}
}
