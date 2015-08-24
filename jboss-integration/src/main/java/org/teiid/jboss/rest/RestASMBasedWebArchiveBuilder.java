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
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.FileUtils;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.TransformationMetadata;



@SuppressWarnings("nls")
public class RestASMBasedWebArchiveBuilder {
	 

	public byte[] createRestArchive(VDBMetaData vdb) throws FileNotFoundException, IOException  {
		MetadataStore metadataStore = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
		
		Properties props = new Properties();
		props.setProperty("${context-name}", vdb.getName() + "_" + vdb.getVersion());
		props.setProperty("${vdb-name}", vdb.getName());
		props.setProperty("${vdb-version}", String.valueOf(vdb.getVersion()));
		
		boolean passthroughAuth = false;
		String securityType = vdb.getPropertyValue(ResteasyEnabler.REST_NAMESPACE+"security-type");
		String securityDomain = vdb.getPropertyValue(ResteasyEnabler.REST_NAMESPACE+"security-domain");
		String securityRole = vdb.getPropertyValue(ResteasyEnabler.REST_NAMESPACE+"security-role");
		String passthoughAuthStr = vdb.getPropertyValue(ResteasyEnabler.REST_NAMESPACE+"passthrough-auth");
		if (passthoughAuthStr != null) {
			passthroughAuth = Boolean.parseBoolean(passthoughAuthStr);
		}
		
		props.setProperty("${security-role}", ((securityRole == null)?"rest":securityRole));
		props.setProperty("${security-domain}", ((securityDomain == null)?"teiid-security":securityDomain));
		
		if (securityType == null) {
			securityType = "httpbasic";
		}

		if (securityType.equalsIgnoreCase("none")) {
			props.setProperty("${security-content}", "");
		}
		else if (securityType.equalsIgnoreCase("httpbasic")) {
			props.setProperty("${security-content}", replaceTemplates(getFileContents("rest-war/httpbasic.xml"), props));
		}
		
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		ZipOutputStream out = new ZipOutputStream(byteStream); 
		writeEntry("WEB-INF/web.xml", out, replaceTemplates(getFileContents("rest-war/web.xml"), props).getBytes());
		writeEntry("WEB-INF/jboss-web.xml", out, replaceTemplates(getFileContents("rest-war/jboss-web.xml"), props).getBytes());
		
		writeSwagger(out, props, vdb);
				
		ArrayList<String> applicationViews = new ArrayList<String>();
		for (ModelMetaData model:vdb.getModelMetaDatas().values()) {
			Schema schema = metadataStore.getSchema(model.getName());
			byte[] viewContents = getViewClass(vdb.getName(), vdb.getVersion(), model.getName(), schema, passthroughAuth);
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

	private void writeSwagger(ZipOutputStream out, Properties props, VDBMetaData vdb) throws IOException {
	    writeEntry("api-doc.html", out, replaceTemplates(getFileContents("swagger/api-doc.html"), props).getBytes());
	    writeEntry("o2c.html", out, replaceTemplates(getFileContents("swagger/o2c.html"), props).getBytes());
	    writeEntry("swagger-ui.min.js", out, replaceTemplates(getFileContents("swagger/swagger-ui.min.js"), props).getBytes());
	    writeEntry("swagger-ui.js", out, replaceTemplates(getFileContents("swagger/swagger-ui.js"), props).getBytes());
	    
	    writeEntry("css/print.css", out, replaceTemplates(getFileContents("swagger/css/print.css"), props).getBytes());
	    writeEntry("css/reset.css", out, replaceTemplates(getFileContents("swagger/css/reset.css"), props).getBytes());
	    writeEntry("css/screen.css", out, replaceTemplates(getFileContents("swagger/css/screen.css"), props).getBytes());
	    writeEntry("css/style.css", out, replaceTemplates(getFileContents("swagger/css/style.css"), props).getBytes());
	    writeEntry("css/typography.css", out, replaceTemplates(getFileContents("swagger/css/typography.css"), props).getBytes());
	    
	    
	    writeEntry("fonts/droid-sans-v6-latin-700.eot", out, replaceTemplates(getFileContents("swagger/fonts/droid-sans-v6-latin-700.eot"), props).getBytes());
	    writeEntry("fonts/droid-sans-v6-latin-700.svg", out, replaceTemplates(getFileContents("swagger/fonts/droid-sans-v6-latin-700.svg"), props).getBytes());
	    writeEntry("fonts/droid-sans-v6-latin-700.ttf", out, replaceTemplates(getFileContents("swagger/fonts/droid-sans-v6-latin-700.ttf"), props).getBytes());
	    writeEntry("fonts/droid-sans-v6-latin-700.woff", out, replaceTemplates(getFileContents("swagger/fonts/droid-sans-v6-latin-700.woff"), props).getBytes());
	    writeEntry("fonts/droid-sans-v6-latin-700.woff2", out, replaceTemplates(getFileContents("swagger/fonts/droid-sans-v6-latin-700.woff2"), props).getBytes());
	    writeEntry("fonts/droid-sans-v6-latin-regular.eot", out, replaceTemplates(getFileContents("swagger/fonts/droid-sans-v6-latin-regular.eot"), props).getBytes());
	    writeEntry("fonts/droid-sans-v6-latin-regular.svg", out, replaceTemplates(getFileContents("swagger/fonts/droid-sans-v6-latin-regular.svg"), props).getBytes());
	    writeEntry("fonts/droid-sans-v6-latin-regular.ttf", out, replaceTemplates(getFileContents("swagger/fonts/droid-sans-v6-latin-regular.ttf"), props).getBytes());
	    writeEntry("fonts/droid-sans-v6-latin-regular.woff", out, replaceTemplates(getFileContents("swagger/fonts/droid-sans-v6-latin-regular.woff"), props).getBytes());
	    writeEntry("fonts/droid-sans-v6-latin-regular.woff2", out, replaceTemplates(getFileContents("swagger/fonts/droid-sans-v6-latin-regular.woff2"), props).getBytes());
	    
	    
	    writeEntry("images/explorer_icons.png", out, replaceTemplates(getFileContents("swagger/images/explorer_icons.png"), props).getBytes());
	    writeEntry("images/favicon-16x16.png", out, replaceTemplates(getFileContents("swagger/images/favicon-16x16.png"), props).getBytes());
	    writeEntry("images/favicon-32x32.png", out, replaceTemplates(getFileContents("swagger/images/favicon-32x32.png"), props).getBytes());
	    writeEntry("images/favicon.ico", out, replaceTemplates(getFileContents("swagger/images/favicon.ico"), props).getBytes());
	    writeEntry("images/logo_small.png", out, replaceTemplates(getFileContents("swagger/images/logo_small.png"), props).getBytes());
	    writeEntry("images/pet_store_api.png", out, replaceTemplates(getFileContents("swagger/images/pet_store_api.png"), props).getBytes());
	    writeEntry("images/wordnik_api.png", out, replaceTemplates(getFileContents("swagger/images/wordnik_api.png"), props).getBytes());
	    
	    
	    writeEntry("lang/en.js", out, replaceTemplates(getFileContents("swagger/lang/en.js"), props).getBytes());
	    writeEntry("lang/es.js", out, replaceTemplates(getFileContents("swagger/lang/es.js"), props).getBytes());
	    writeEntry("lang/pt.js", out, replaceTemplates(getFileContents("swagger/lang/pt.js"), props).getBytes());
	    writeEntry("lang/ru.js", out, replaceTemplates(getFileContents("swagger/lang/ru.js"), props).getBytes());
	    writeEntry("lang/translator.js", out, replaceTemplates(getFileContents("swagger/lang/translator.js"), props).getBytes());
	    
	    
	    writeEntry("lib/backbone-min.js", out, replaceTemplates(getFileContents("swagger/lib/backbone-min.js"), props).getBytes());
	    writeEntry("lib/handlebars-2.0.0.js", out, replaceTemplates(getFileContents("swagger/lib/handlebars-2.0.0.js"), props).getBytes());
	    writeEntry("lib/highlight.7.3.pack.js", out, replaceTemplates(getFileContents("swagger/lib/highlight.7.3.pack.js"), props).getBytes());
	    writeEntry("lib/jquery-1.8.0.min.js", out, replaceTemplates(getFileContents("swagger/lib/jquery-1.8.0.min.js"), props).getBytes());
	    writeEntry("lib/jquery.ba-bbq.min.js", out, replaceTemplates(getFileContents("swagger/lib/jquery.ba-bbq.min.js"), props).getBytes());
	    writeEntry("lib/jquery.slideto.min.js", out, replaceTemplates(getFileContents("swagger/lib/jquery.slideto.min.js"), props).getBytes());
	    writeEntry("lib/jquery.wiggle.min.js", out, replaceTemplates(getFileContents("swagger/lib/jquery.wiggle.min.js"), props).getBytes());
	    writeEntry("lib/marked.js", out, replaceTemplates(getFileContents("swagger/lib/marked.js"), props).getBytes());
	    writeEntry("lib/swagger-oauth.js", out, replaceTemplates(getFileContents("swagger/lib/swagger-oauth.js"), props).getBytes());
	    writeEntry("lib/underscore-min.js", out, replaceTemplates(getFileContents("swagger/lib/underscore-min.js"), props).getBytes());
	    writeEntry("lib/underscore-min.map", out, replaceTemplates(getFileContents("swagger/lib/underscore-min.map"), props).getBytes());
	    
	    String desc = vdb.getDescription();
	    if(null == desc) {
	        desc = vdb.getName();
	    }
	    byte[] bytes = getBootstrapServletClass(vdb.getName(), desc, vdb.getVersion() + ".0", new String[]{"http"}, props.getProperty("${context-name}"), "org.teiid.jboss.rest", true);
	    writeEntry("WEB-INF/classes/org/teiid/jboss/rest/BootstrapServlet.class", out, bytes);
	    writeEntry("WEB-INF/classes/org/teiid/jboss/rest/ApiOriginFilter.class", out, getApiOriginFilterClass("Access-Control-Allow-Origin", "*", "Access-Control-Allow-Methods", "GET, POST, DELETE, PUT", "Access-Control-Allow-Headers", "Content-Type"));
    }
	
	private byte[] getApiOriginFilterClass(String k1, String v1, String k2, String v2, String k3, String v3) {
        
	    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
        MethodVisitor mv;
        AnnotationVisitor av0;
        
        cw.visit(V1_5, ACC_PUBLIC + ACC_SUPER, "org/teiid/jboss/rest/ApiOriginFilter", null, "java/lang/Object", new String[]{"javax/servlet/Filter"});
        
        {
            mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
            mv.visitVarInsn(ALOAD, 0);
            mv.visitMethodInsn(INVOKESPECIAL,"java/lang/Object", "<init>", "()V");
            mv.visitInsn(RETURN);
            mv.visitMaxs(1, 1);
            mv.visitEnd();
        }
        
        {
            mv = cw.visitMethod(ACC_PUBLIC, "init", "(Ljavax/servlet/FilterConfig;)V", null, new String[] {"javax/servlet/ServletException"});
            av0 = mv.visitAnnotation("Ljava/lang/Override;", true);
            av0.visitEnd();
            mv.visitCode();
            mv.visitInsn(RETURN);
            mv.visitMaxs(1, 1);
            mv.visitEnd();
        }
        
        {
            mv = cw.visitMethod(ACC_PUBLIC, "doFilter", "(Ljavax/servlet/ServletRequest;Ljavax/servlet/ServletResponse;Ljavax/servlet/FilterChain;)V", null, new String[] {"javax/servlet/ServletException", "java/io/IOException"});
            av0 = mv.visitAnnotation("Ljava/lang/Override;", true);
            av0.visitEnd();
            mv.visitCode();
            mv.visitTypeInsn(NEW, "javax/servlet/http/HttpServletResponse");
            mv.visitInsn(DUP);
            mv.visitVarInsn(ALOAD, 2);
            mv.visitTypeInsn(CHECKCAST, "javax/servlet/http/HttpServletResponse");
            mv.visitVarInsn(ASTORE, 4);
            mv.visitVarInsn(ALOAD, 4);
            mv.visitLdcInsn(k1);
            mv.visitLdcInsn(v1);
            mv.visitMethodInsn(INVOKEINTERFACE, "javax/servlet/http/HttpServletResponse", "addHeader", "(Ljava/lang/String;Ljava/lang/String;)V");
            mv.visitVarInsn(ALOAD, 4);
            mv.visitLdcInsn(k2);
            mv.visitLdcInsn(v2);
            mv.visitMethodInsn(INVOKEINTERFACE, "javax/servlet/http/HttpServletResponse", "addHeader", "(Ljava/lang/String;Ljava/lang/String;)V");
            mv.visitVarInsn(ALOAD, 4);
            mv.visitLdcInsn(k3);
            mv.visitLdcInsn(v3);
            mv.visitMethodInsn(INVOKEINTERFACE, "javax/servlet/http/HttpServletResponse", "addHeader", "(Ljava/lang/String;Ljava/lang/String;)V");
            mv.visitVarInsn(ALOAD, 3);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitVarInsn(ALOAD, 2);
            mv.visitMethodInsn(INVOKEINTERFACE, "javax/servlet/FilterChain", "doFilter", "(Ljavax/servlet/ServletRequest;Ljavax/servlet/ServletResponse;)V");
            mv.visitInsn(RETURN);
            mv.visitMaxs(6, 3);
            mv.visitEnd();
        }
        
        {
            mv = cw.visitMethod(ACC_PUBLIC, "destroy", "()V", null, null);
            av0 = mv.visitAnnotation("Ljava/lang/Override;", true);
            av0.visitEnd();
            mv.visitCode();
            mv.visitInsn(RETURN);
            mv.visitMaxs(1, 1);
            mv.visitEnd();
        }
        
        cw.visitEnd();
        
        return cw.toByteArray();
           
    }
	
	private byte[] getBootstrapServletClass(String vdbName, String desc, String version, String[] schamas, String baseUrl, String packages, Boolean scan) {
	    ClassWriter cw = new ClassWriter(0);
        MethodVisitor mv;
        AnnotationVisitor av0;
   
        cw.visit(V1_5, ACC_PUBLIC + ACC_SUPER, "org/teiid/jboss/rest/BootstrapServlet", null, "javax/servlet/http/HttpServlet", null);
        
        {
            mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
            mv.visitCode();
            mv.visitVarInsn(ALOAD, 0);
            mv.visitMethodInsn(INVOKESPECIAL, "javax/servlet/http/HttpServlet", "<init>", "()V");
            mv.visitInsn(RETURN);
            mv.visitMaxs(1, 1);
            mv.visitEnd();
        }
        
        //init method
        {
            mv = cw.visitMethod(ACC_PUBLIC, "init", "(Ljavax/servlet/ServletConfig;)V", null, new String[] {"javax/servlet/ServletException"});
            av0 = mv.visitAnnotation("Ljava/lang/Override;", true);
            av0.visitEnd();
            mv.visitCode();
            mv.visitTypeInsn(NEW, "io/swagger/jaxrs/config/BeanConfig");
            mv.visitInsn(DUP);
            mv.visitMethodInsn(INVOKESPECIAL, "io/swagger/jaxrs/config/BeanConfig", "<init>", "()V");
            mv.visitVarInsn(ASTORE, 2);
            mv.visitVarInsn(ALOAD, 2);
            mv.visitLdcInsn(vdbName);
            mv.visitMethodInsn(INVOKEINTERFACE, "io/swagger/jaxrs/config/BeanConfig", "setTitle", "(Ljava/lang/String;)V");
            mv.visitVarInsn(ALOAD, 2);
            mv.visitLdcInsn(desc);
            mv.visitMethodInsn(INVOKEINTERFACE, "io/swagger/jaxrs/config/BeanConfig", "setDescription", "(Ljava/lang/String;)V");
            mv.visitVarInsn(ALOAD, 2);
            mv.visitLdcInsn(version);
            mv.visitMethodInsn(INVOKEINTERFACE, "io/swagger/jaxrs/config/BeanConfig", "setVersion", "(Ljava/lang/String;)V");
//            mv.visitVarInsn(ALOAD, 2);
//            mv.visitLdcInsn(schamas);
//            mv.visitMethodInsn(INVOKEINTERFACE, "io/swagger/jaxrs/config/BeanConfig", "setSchemes", "([Ljava/lang/String;)V");
            mv.visitVarInsn(ALOAD, 2);
            mv.visitLdcInsn(baseUrl);
            mv.visitMethodInsn(INVOKEINTERFACE, "io/swagger/jaxrs/config/BeanConfig", "setBasePath", "(Ljava/lang/String;)V");
            mv.visitVarInsn(ALOAD, 2);
            mv.visitLdcInsn(packages);
            mv.visitMethodInsn(INVOKEINTERFACE, "io/swagger/jaxrs/config/BeanConfig", "setResourcePackage", "(Ljava/lang/String;)V");
            mv.visitVarInsn(ALOAD, 2);
            mv.visitInsn(scan?ICONST_1:ICONST_0);
            mv.visitMethodInsn(INVOKEINTERFACE, "io/swagger/jaxrs/config/BeanConfig", "setScan", "(Z)V");
            mv.visitMaxs(6, 3);
            mv.visitEnd();
        }
        
        // doGet method
        {
            mv = cw.visitMethod(ACC_PUBLIC, "doGet", "(Ljavax/servlet/http/HttpServletRequest;Ljavax/servlet/http/HttpServletResponse;)V", null, new String[] {"javax/servlet/ServletException", "java/io/IOException"});
            av0 = mv.visitAnnotation("Ljava/lang/Override;", true);
            av0.visitEnd();
            mv.visitCode();
            mv.visitVarInsn(ALOAD, 0);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitVarInsn(ALOAD, 2);
            mv.visitMethodInsn(INVOKEVIRTUAL, "org/teiid/jboss/rest/BootstrapServlet", "doPost", "(Ljavax/servlet/http/HttpServletRequest;Ljavax/servlet/http/HttpServletResponse;)V");
            mv.visitInsn(RETURN);
            mv.visitMaxs(3, 1);
            mv.visitEnd();
        }
        
        // doPost method
        mv = cw.visitMethod(ACC_PUBLIC, "doPost", "(Ljavax/servlet/http/HttpServletRequest;Ljavax/servlet/http/HttpServletResponse;)V", null, new String[] {"javax/servlet/ServletException", "java/io/IOException"});
        AnnotationVisitor av2 = mv.visitAnnotation("Ljava/lang/Override;", true);
        av2.visitEnd();
        mv.visitCode();
//        mv.visitVarInsn(ALOAD, 0);
//        mv.visitVarInsn(ALOAD, 1);
//        mv.visitMethodInsn(INVOKEVIRTUAL, "org/teiid/jboss/rest/BootstrapServlet", "doGet", "(Ljavax/servlet/http/HttpServletRequest;Ljavax/servlet/http/HttpServletResponse;)V");
        mv.visitInsn(RETURN);
        mv.visitMaxs(3, 2);
        mv.visitEnd();
        
        cw.visitEnd();
        
        return cw.toByteArray();
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

    private static HashSet<String> getPathParameters(String uri ) {
        HashSet<String> pathParams = new HashSet<String>();
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
    	
    	mv.visitFieldInsn(GETFIELD, "org/teiid/jboss/rest/TeiidRestApplication", "singletons", "Ljava/util/Set;");
        mv.visitTypeInsn(NEW, "io/swagger/jaxrs/listing/ApiListingResource");
        mv.visitInsn(DUP);
        mv.visitMethodInsn(INVOKESPECIAL, "io/swagger/jaxrs/listing/ApiListingResource", "<init>", "()V");
        mv.visitMethodInsn(INVOKEINTERFACE, "java/util/Set", "add", "(Ljava/lang/Object;)Z");
        mv.visitInsn(POP);
        mv.visitVarInsn(ALOAD, 0);
        
        mv.visitFieldInsn(GETFIELD, "org/teiid/jboss/rest/TeiidRestApplication", "singletons", "Ljava/util/Set;");
        mv.visitTypeInsn(NEW, "io/swagger/jaxrs/listing/SwaggerSerializers");
        mv.visitInsn(DUP);
        mv.visitMethodInsn(INVOKESPECIAL, "io/swagger/jaxrs/listing/SwaggerSerializers", "<init>", "()V");
        mv.visitMethodInsn(INVOKEINTERFACE, "java/util/Set", "add", "(Ljava/lang/Object;)Z");
        mv.visitInsn(POP);
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
    
    public byte[] getViewClass(String vdbName, int vdbVersion, String modelName, Schema schema, boolean passthroughAuth) {
    	ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    	MethodVisitor mv;
    	AnnotationVisitor av0;
    	boolean hasValidProcedures = false;
    	
    	cw.visit(V1_6, ACC_PUBLIC + ACC_SUPER, "org/teiid/jboss/rest/"+modelName, null, "org/teiid/jboss/rest/TeiidRSProvider", null);
    	
    	{
    	av0 = cw.visitAnnotation("Lio/swagger/annotations/Api;", true);
        av0.visit("value", "/"+modelName.toLowerCase());
        av0.visitEnd();
    	}
    	
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
			    	buildRestService(vdbName, vdbVersion, modelName, procedure, method, uri, cw, contentType, charSet, passthroughAuth);
			    	hasValidProcedures = true;
				}
			}
		}    	
		
		buildQueryProcedure(vdbName, vdbVersion, modelName, "xml", cw, passthroughAuth);
		buildQueryProcedure(vdbName, vdbVersion, modelName, "json", cw, passthroughAuth);
    	
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

	private void buildRestService(String vdbName, int vdbVersion, String modelName, Procedure procedure,
			String method, String uri, ClassWriter cw, String contentType,
			String charSet, boolean passthroughAuth) {
		
		List<ProcedureParameter> params = new ArrayList<ProcedureParameter>(procedure.getParameters().size());
		boolean usingReturn = false;
		boolean hasLobInput = false;
		for (ProcedureParameter p : procedure.getParameters()) {
			if (p.getType() == Type.In || p.getType() == Type.InOut) {
				params.add(p);
			} else if (p.getType() == Type.ReturnValue && procedure.getResultSet() == null) {
				usingReturn = true;
			}
			if (!hasLobInput) {
			    String runtimeType = p.getRuntimeType();
			    hasLobInput = DataTypeManager.isLOB(runtimeType);
			}
		}
		int paramsSize = params.size();
		MethodVisitor mv;
		
		boolean useMultipart = false;
        if (method.toUpperCase().equals("POST") && hasLobInput) {
            useMultipart = true;
        }
		
		AnnotationVisitor av0;
		{
    	
		StringBuilder paramSignature = new StringBuilder();
    	paramSignature.append("(");
    	for (int i = 0; i < paramsSize; i++) {
    		paramSignature.append("Ljava/lang/String;");
    	}
    	paramSignature.append(")");
    	
    	if (useMultipart) {
    	    mv = cw.visitMethod(ACC_PUBLIC, procedure.getName()+contentType.replace('/', '_'), "(Lorg/jboss/resteasy/plugins/providers/multipart/MultipartFormDataInput;)Ljavax/ws/rs/core/StreamingOutput;", null, new String[] { "javax/ws/rs/WebApplicationException" });    	    
    	}
    	else {
    	    mv = cw.visitMethod(ACC_PUBLIC, procedure.getName()+contentType.replace('/', '_'), paramSignature+"Ljavax/ws/rs/core/StreamingOutput;", null, new String[] { "javax/ws/rs/WebApplicationException" });
    	}
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
    	
    	{
    	av0 = mv.visitAnnotation("Lio/swagger/annotations/ApiOperation;", true);
    	av0.visit("value", procedure.getName());
    	av0.visitEnd();
    	}
    	
    	{
//        av0 = mv.visitAnnotation("Lio/swagger/annotations/ApiResponses;", true);
//        av0.visit("value", new ApiResponse[]{});
//        av0.visitEnd();
        }
        
    	if(useMultipart)
    	{
    	    av0 = mv.visitAnnotation("Ljavax/ws/rs/Consumes;", true);
    	    {
    	    AnnotationVisitor av1 = av0.visitArray("value");
    	    av1.visit(null, "multipart/form-data");
    	    av1.visitEnd();
    	    }
    	    av0.visitEnd();
        }
        
    	if(!useMultipart) {
        	// post only accepts Form inputs, not path params
        	HashSet<String> pathParms = getPathParameters(uri);
        	for (int i = 0; i < paramsSize; i++)
        	{
                String paramType = "Ljavax/ws/rs/FormParam;";
                if (method.toUpperCase().equals("GET")) {
                    paramType = "Ljavax/ws/rs/QueryParam;";
                }
                if (pathParms.contains(params.get(i).getName())){
                    paramType = "Ljavax/ws/rs/PathParam;";
                }
        
        		av0 = mv.visitParameterAnnotation(i, paramType, true);
        		av0.visit("value", params.get(i).getName());
        		av0.visitEnd();
        		
        		av0 = mv.visitParameterAnnotation(i, "Lio/swagger/annotations/ApiParam;", true);
                av0.visit("value", params.get(i).getName());
                av0.visitEnd();
    		}
    	}
    	
    	mv.visitCode();
    	Label l0 = new Label();
    	Label l1 = new Label();
    	Label l2 = new Label();
    	mv.visitTryCatchBlock(l0, l1, l2, "java/sql/SQLException");
    	mv.visitLabel(l0);
    	
    	if (!useMultipart) {
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
        	mv.visitLdcInsn(vdbName);
        	mv.visitIntInsn(BIPUSH, vdbVersion);
        	
        	mv.visitLdcInsn(procedure.getSQLString());
        	
        	mv.visitVarInsn(ALOAD, paramsSize+1);
        	mv.visitLdcInsn(charSet==null?"":charSet);
        	mv.visitInsn(passthroughAuth?ICONST_1:ICONST_0);
        	mv.visitInsn(usingReturn?ICONST_1:ICONST_0);
        	mv.visitMethodInsn(INVOKEVIRTUAL, "org/teiid/jboss/rest/"+modelName, "execute", "(Ljava/lang/String;ILjava/lang/String;Ljava/util/LinkedHashMap;Ljava/lang/String;ZZ)Ljavax/ws/rs/core/StreamingOutput;");
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
        	mv.visitMaxs(7, paramsSize+2);
        	mv.visitEnd();
    	}
    	else {
    	    mv.visitVarInsn(ALOAD, 0);
    	    mv.visitLdcInsn(vdbName);
    	    mv.visitIntInsn(BIPUSH, vdbVersion);
    	    mv.visitLdcInsn(procedure.getSQLString());
    	    mv.visitVarInsn(ALOAD, 1);
    	    mv.visitLdcInsn(charSet==null?"":charSet);
    	    mv.visitInsn(passthroughAuth?ICONST_1:ICONST_0);
    	    mv.visitInsn(usingReturn?ICONST_1:ICONST_0);
    	    mv.visitMethodInsn(INVOKEVIRTUAL, "org/teiid/jboss/rest/"+modelName, "executePost", "(Ljava/lang/String;ILjava/lang/String;Lorg/jboss/resteasy/plugins/providers/multipart/MultipartFormDataInput;Ljava/lang/String;ZZ)Ljavax/ws/rs/core/StreamingOutput;");
    	    mv.visitLabel(l1);
    	    mv.visitInsn(ARETURN);
    	    mv.visitLabel(l2);
    	    mv.visitFrame(Opcodes.F_SAME1, 0, null, 1, new Object[] {"java/sql/SQLException"});
    	    mv.visitVarInsn(ASTORE, 2);
    	    mv.visitTypeInsn(NEW, "javax/ws/rs/WebApplicationException");
    	    mv.visitInsn(DUP);
    	    mv.visitVarInsn(ALOAD, 2);
    	    mv.visitFieldInsn(GETSTATIC, "javax/ws/rs/core/Response$Status", "INTERNAL_SERVER_ERROR", "Ljavax/ws/rs/core/Response$Status;");
    	    mv.visitMethodInsn(INVOKESPECIAL, "javax/ws/rs/WebApplicationException", "<init>", "(Ljava/lang/Throwable;Ljavax/ws/rs/core/Response$Status;)V");
    	    mv.visitInsn(ATHROW);
    	    mv.visitMaxs(8, 3);
    	    mv.visitEnd();
    	}
    	}
	}
	
	private void buildQueryProcedure(String vdbName, int vdbVersion, String modelName, String context, ClassWriter cw, boolean passthroughAuth) {
		MethodVisitor mv;
		{
			AnnotationVisitor av0;
			mv = cw.visitMethod(ACC_PUBLIC, "sqlQuery"+context, "(Ljava/lang/String;)Ljavax/ws/rs/core/StreamingOutput;", null, null);
			{
			av0 = mv.visitAnnotation("Ljavax/ws/rs/Produces;", true);
			{
			AnnotationVisitor av1 = av0.visitArray("value");
			av1.visit(null, "application/"+context);
			av1.visitEnd();
			}
			av0.visitEnd();
			}
			{
			av0 = mv.visitAnnotation("Ljavax/ws/rs/POST;", true);
			av0.visitEnd();
			}
			{
			av0 = mv.visitAnnotation("Ljavax/ws/rs/Path;", true);
			av0.visit("value", "/query");
			av0.visitEnd();
			}
			{
			av0 = mv.visitAnnotation("Lio/swagger/annotations/ApiOperation;", true);
			av0.visit("value", context);
            av0.visitEnd();
			}
			{
//	        av0 = mv.visitAnnotation("Lio/swagger/annotations/ApiResponses;", true);
//	        av0.visit("value", new ApiResponse[]{});
//	        av0.visitEnd();
	        }
			{
	        av0 = mv.visitParameterAnnotation(0, "Lio/swagger/annotations/ApiParam;", true);
	        av0.visit("value", context);
	        av0.visitEnd();
	        }
			
			{
			av0 = mv.visitParameterAnnotation(0, "Ljavax/ws/rs/FormParam;", true);
			av0.visit("value", "sql");
			av0.visitEnd();
			}
			
			mv.visitCode();
			Label l0 = new Label();
			Label l1 = new Label();
			Label l2 = new Label();
			mv.visitTryCatchBlock(l0, l1, l2, "java/sql/SQLException");
			mv.visitLabel(l0);
			mv.visitVarInsn(ALOAD, 0);
			mv.visitLdcInsn(vdbName);
			mv.visitIntInsn(BIPUSH, vdbVersion);
			mv.visitVarInsn(ALOAD, 1);
			mv.visitInsn(context.equals("xml")?ICONST_0:ICONST_1);
			mv.visitInsn(passthroughAuth?ICONST_1:ICONST_0);
			mv.visitMethodInsn(INVOKEVIRTUAL, "org/teiid/jboss/rest/"+modelName, "executeQuery", "(Ljava/lang/String;ILjava/lang/String;ZZ)Ljavax/ws/rs/core/StreamingOutput;");
			mv.visitLabel(l1);
			mv.visitInsn(ARETURN);
			mv.visitLabel(l2);
			mv.visitFrame(F_SAME1, 0, null, 1, new Object[] {"java/sql/SQLException"});
			mv.visitVarInsn(ASTORE, 2);
			mv.visitTypeInsn(NEW, "javax/ws/rs/WebApplicationException");
			mv.visitInsn(DUP);
			mv.visitVarInsn(ALOAD, 2);
			mv.visitFieldInsn(GETSTATIC, "javax/ws/rs/core/Response$Status", "INTERNAL_SERVER_ERROR", "Ljavax/ws/rs/core/Response$Status;");
			mv.visitMethodInsn(INVOKESPECIAL, "javax/ws/rs/WebApplicationException", "<init>", "(Ljava/lang/Throwable;Ljavax/ws/rs/core/Response$Status;)V");
			mv.visitInsn(ATHROW);
			mv.visitMaxs(6, 3);
			mv.visitEnd();
		}		
	}
}
