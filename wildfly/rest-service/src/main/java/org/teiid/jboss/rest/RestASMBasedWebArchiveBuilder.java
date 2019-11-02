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
package org.teiid.jboss.rest;

import static org.objectweb.asm.Opcodes.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
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
import org.teiid.deployers.RestWarGenerator;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.NamespaceContainer;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.vdb.runtime.VDBKey;

import io.swagger.annotations.ApiResponse;

@SuppressWarnings("nls")
public class RestASMBasedWebArchiveBuilder implements RestWarGenerator {

    @Override
    public boolean hasRestMetadata(VDBMetaData vdb) {
        String generate = vdb.getPropertyValue(REST_NAMESPACE+"auto-generate"); //$NON-NLS-1$
        if (generate == null || !Boolean.parseBoolean(generate)) {
            return false;
        }

        String securityType = vdb.getPropertyValue(REST_NAMESPACE+"security-type"); //$NON-NLS-1$
        if (securityType != null && !securityType.equalsIgnoreCase("none") && !securityType.equalsIgnoreCase("httpbasic")) { //$NON-NLS-1$ //$NON-NLS-2$
            return false;
        }

        MetadataStore metadataStore = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
        for (ModelMetaData model: vdb.getModelMetaDatas().values()) {
            Schema schema = metadataStore.getSchema(model.getName());
            if (schema == null) {
                continue; //OTHER type, which does not have a corresponding Teiid schema
            }
            Collection<Procedure> procedures = schema.getProcedures().values();
            for (Procedure procedure:procedures) {
                String uri = procedure.getProperty(NamespaceContainer.REST_PREFIX+"URI", false); //$NON-NLS-1$
                String method = procedure.getProperty(NamespaceContainer.REST_PREFIX+"METHOD", false); //$NON-NLS-1$
                if (uri != null && method != null) {
                    return true;
                }
            }

        }
        return false;
    }

    @Override
    public byte[] getContent(VDBMetaData vdb, String fullName) throws IOException  {
        MetadataStore metadataStore = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();

        Properties props = new Properties();

        props.setProperty("${context-name}", fullName);
        props.setProperty("${vdb-name}", vdb.getName());
        props.setProperty("${vdb-version}", String.valueOf(vdb.getVersion()));
        props.setProperty("${api-page-title}", fullName + " API");

        String securityType = vdb.getPropertyValue(REST_NAMESPACE+"security-type");
        String securityDomain = vdb.getPropertyValue(REST_NAMESPACE+"security-domain");
        String securityRole = vdb.getPropertyValue(REST_NAMESPACE+"security-role");

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
        writeEntry("api.html", out, replaceTemplates(getFileContents("rest-war/api.html"), props).getBytes());

        writeDirectoryEntry(out, "swagger-ui-2.1.1.zip");

        String version = vdb.getVersion();
        VDBKey vdbKey = new VDBKey(vdb.getName(), vdb.getVersion());

        ArrayList<String> applicationViews = new ArrayList<String>();
        for (ModelMetaData model:vdb.getModelMetaDatas().values()) {
            Schema schema = metadataStore.getSchema(model.getName());
            if (schema == null) {
                continue; //OTHER type, which does not have a corresponding Teiid schema
            }

            byte[] viewContents = getViewClass(vdb.getName(), version, model.getName(), schema, true);
            if (viewContents != null) {
                writeEntry("WEB-INF/classes/org/teiid/jboss/rest/"+model.getName()+".class", out, viewContents);
                applicationViews.add(schema.getName());
            }
        }
        writeEntry("WEB-INF/classes/org/teiid/jboss/rest/TeiidRestApplication.class", out, getApplicationClass(applicationViews));
        writeEntry("META-INF/MANIFEST.MF", out, getFileContents("rest-war/MANIFEST.MF").getBytes());

        byte[] bytes = getBootstrapServletClass(vdb.getName(), vdb.getDescription() == null ? vdb.getName() : vdb.getDescription(), vdbKey.getSemanticVersion(), new String[]{"http"}, File.separator + props.getProperty("${context-name}"), "org.teiid.jboss.rest", true);
        writeEntry("WEB-INF/classes/org/teiid/jboss/rest/Bootstrap.class", out, bytes);

        writeEntry("images/teiid_logo_450px.png", out, getBinaryFileContents("rest-war/teiid_logo_450px.png"));

        out.close();
        return byteStream.toByteArray();
    }

    protected byte[] getBootstrapServletClass(String vdbName, String desc, String version, String[] schamas, String baseUrl, String packages, boolean scan) {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
        MethodVisitor mv;
        AnnotationVisitor av0;

        cw.visit(V1_5, ACC_PUBLIC + ACC_SUPER, "org/teiid/jboss/rest/Bootstrap", null, "org/teiid/jboss/rest/BootstrapServlet", null);

        {
            mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
            mv.visitCode();
            mv.visitVarInsn(ALOAD, 0);
            mv.visitMethodInsn(INVOKESPECIAL, "org/teiid/jboss/rest/BootstrapServlet", "<init>", "()V");
            mv.visitInsn(RETURN);
            mv.visitMaxs(1, 1);
            mv.visitEnd();
        }

        //init method
        {
            mv = cw.visitMethod(ACC_PUBLIC, "init", "(Lio/swagger/jaxrs/config/BeanConfig;)V", null, null);
            av0 = mv.visitAnnotation("Ljava/lang/Override;", true);
            av0.visitEnd();
            mv.visitCode();
            mv.visitVarInsn(ALOAD, 1);
            mv.visitLdcInsn(vdbName);
            mv.visitMethodInsn(INVOKEVIRTUAL, "io/swagger/jaxrs/config/BeanConfig", "setTitle", "(Ljava/lang/String;)V");
            mv.visitVarInsn(ALOAD, 1);
            mv.visitLdcInsn(desc);
            mv.visitMethodInsn(INVOKEVIRTUAL, "io/swagger/jaxrs/config/BeanConfig", "setDescription", "(Ljava/lang/String;)V");
            mv.visitVarInsn(ALOAD, 1);
            mv.visitLdcInsn(version);
            mv.visitMethodInsn(INVOKEVIRTUAL, "io/swagger/jaxrs/config/BeanConfig", "setVersion", "(Ljava/lang/String;)V");
            mv.visitVarInsn(ALOAD, 1);
            mv.visitInsn(ICONST_1);
            mv.visitTypeInsn(ANEWARRAY, "java/lang/String");
            Integer[] array = new Integer[]{ICONST_0, ICONST_1, ICONST_2, ICONST_3, ICONST_4, ICONST_5};
            for(int i = 0 ; i < schamas.length ; i ++) {
                mv.visitInsn(DUP);
                mv.visitInsn(array[i]);
                mv.visitLdcInsn(schamas[i]);
                mv.visitInsn(AASTORE);
            }
            mv.visitMethodInsn(INVOKEVIRTUAL, "io/swagger/jaxrs/config/BeanConfig", "setSchemes", "([Ljava/lang/String;)V");
            mv.visitVarInsn(ALOAD, 1);
            mv.visitLdcInsn(baseUrl);
            mv.visitMethodInsn(INVOKEVIRTUAL, "io/swagger/jaxrs/config/BeanConfig", "setBasePath", "(Ljava/lang/String;)V");
            mv.visitVarInsn(ALOAD, 1);
            mv.visitLdcInsn(packages);
            mv.visitMethodInsn(INVOKEVIRTUAL, "io/swagger/jaxrs/config/BeanConfig", "setResourcePackage", "(Ljava/lang/String;)V");
            mv.visitVarInsn(ALOAD, 1);
            mv.visitInsn(scan?ICONST_1:ICONST_0);
            mv.visitMethodInsn(INVOKEVIRTUAL, "io/swagger/jaxrs/config/BeanConfig", "setScan", "(Z)V");
            mv.visitInsn(RETURN);
            mv.visitMaxs(2, 1);
            mv.visitEnd();
        }

        cw.visitEnd();

        return cw.toByteArray();
    }

    private void writeDirectoryEntry(ZipOutputStream out, String name) throws IOException {
        ZipFile zipFile = getZipFile(name);
        Enumeration<?> en = zipFile.entries();
        while(en.hasMoreElements()) {
            ZipEntry entry = (ZipEntry) en.nextElement();
            if(!entry.isDirectory()) {
                writeEntry(entry.getName(), out, IOUtils.toByteArray(zipFile.getInputStream(entry)));
            }
        }
        FileUtils.remove(new File(zipFile.getName()));
    }

    private ZipFile getZipFile(String name) throws IOException {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(name);
        File file = new File(System.getProperty("java.io.tmpdir") + File.separator + System.currentTimeMillis() + ".zip");
        FileOutputStream fos = new FileOutputStream(file);
        byte[] buff = new byte[1024 * 4];
        int read;
        while((read = in.read(buff, 0, buff.length)) != -1) {
            fos.write(buff, 0, read);
        }
        fos.flush();
        fos.close();
        return new ZipFile(file);
    }

    private void writeEntry(String name, ZipOutputStream out, byte[] contents) throws IOException {
        ZipEntry e = new ZipEntry(name);
        out.putNextEntry(e);
        ObjectConverterUtil.write(out, new ByteArrayInputStream(contents), -1, false);
        out.closeEntry();
    }

    private String getFileContents(String file) throws IOException {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(file);
        Reader reader = new InputStreamReader(in);
        String webXML = ObjectConverterUtil.convertToString(reader);
        return webXML;
    }

    private byte[] getBinaryFileContents(String file) throws IOException {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(file);
        return IOUtils.toByteArray(in);
    }

    private String replaceTemplates(String orig, Properties replacements) {
        for (String key:replacements.stringPropertyNames()) {
            orig = StringUtil.replaceAll(orig, key, replacements.getProperty(key));
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
        mv.visitTypeInsn(NEW, "org/teiid/jboss/rest/CustomApiListingResource");
        mv.visitInsn(DUP);
        mv.visitMethodInsn(INVOKESPECIAL, "org/teiid/jboss/rest/CustomApiListingResource", "<init>", "()V");
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

    protected byte[] getViewClass(String vdbName, String vdbVersion, String modelName, Schema schema, boolean passthroughAuth) {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
        MethodVisitor mv;
        AnnotationVisitor av0;
        boolean hasValidProcedures = false;

        cw.visit(V1_6, ACC_PUBLIC + ACC_SUPER, "org/teiid/jboss/rest/"+modelName, null, "org/teiid/jboss/rest/TeiidRSProvider", null);

        {
        av0 = cw.visitAnnotation("Ljavax/ws/rs/Path;", true);
        av0.visit("value", "/"+modelName);
        av0.visitEnd();
        }

        {
            av0 = cw.visitAnnotation("Lio/swagger/annotations/Api;", true);
            av0.visit("value", "/" + modelName);
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
            String uri = procedure.getProperty(REST_NAMESPACE+"URI", false);
            String method = procedure.getProperty(REST_NAMESPACE+"METHOD", false);
            String contentType = procedure.getProperty(REST_NAMESPACE+"PRODUCES", false);
            String charSet = procedure.getProperty(REST_NAMESPACE+"CHARSET", false);

            if (uri != null && method != null) {
                if (contentType == null) {
                    contentType = findContentType(procedure);
                }

                if (contentType != null) {
                    contentType = contentType.toLowerCase().trim();
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

        //don't expose a general interface by default
        String sqlQuery = schema.getProperty(REST_NAMESPACE+"sqlquery", false);
        if (sqlQuery != null && Boolean.valueOf(sqlQuery)) {
            buildQueryProcedure(vdbName, vdbVersion, modelName, "xml", cw, passthroughAuth);
            buildQueryProcedure(vdbName, vdbVersion, modelName, "json", cw, passthroughAuth);
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

    private void buildRestService(String vdbName, String vdbVersion, String modelName, Procedure procedure,
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
            mv = cw.visitMethod(ACC_PUBLIC, procedure.getName()+contentType.replaceAll("[^\\w]", "_"), "(Lorg/jboss/resteasy/plugins/providers/multipart/MultipartFormDataInput;)Ljavax/ws/rs/core/StreamingOutput;", null, new String[] { "javax/ws/rs/WebApplicationException" });
        }
        else {
            mv = cw.visitMethod(ACC_PUBLIC, procedure.getName()+contentType.replaceAll("[^\\w]", "_"), paramSignature+"Ljavax/ws/rs/core/StreamingOutput;", null, new String[] { "javax/ws/rs/WebApplicationException" });
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
            av0 = mv.visitAnnotation("Lio/swagger/annotations/ApiResponses;", true);
            ApiResponse[] array = new ApiResponse[]{};
            AnnotationVisitor av1 = av0.visitArray("value");
            for(int i = 0 ; i < array.length ; i ++) {
                av1.visit("value", array[i]);
            }
            av1.visitEnd();
            av0.visitEnd();
        }

        if(useMultipart)
        {
            addConsumes(mv, "multipart/form-data");
        } else {
            // post only accepts Form inputs, not path params
            boolean get = method.toUpperCase().equals("GET");
            if (paramsSize > 0 && !get) {
                addConsumes(mv, "application/x-www-form-urlencoded");
            }

            HashSet<String> pathParms = getPathParameters(uri);
            for (int i = 0; i < paramsSize; i++)
            {
                String paramType = "Ljavax/ws/rs/FormParam;";
                if (get) {
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
            mv.visitLdcInsn(vdbVersion);
            mv.visitLdcInsn(procedure.getSQLString());

            mv.visitVarInsn(ALOAD, paramsSize+1);
            mv.visitLdcInsn(charSet==null?"":charSet);
            mv.visitInsn(passthroughAuth?ICONST_1:ICONST_0);
            mv.visitInsn(usingReturn?ICONST_1:ICONST_0);
            mv.visitMethodInsn(INVOKEVIRTUAL, "org/teiid/jboss/rest/"+modelName, "execute", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/util/LinkedHashMap;Ljava/lang/String;ZZ)Ljavax/ws/rs/core/StreamingOutput;");
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
            mv.visitLdcInsn(vdbVersion);
            mv.visitLdcInsn(procedure.getSQLString());
            mv.visitVarInsn(ALOAD, 1);
            mv.visitLdcInsn(charSet==null?"":charSet);
            mv.visitInsn(passthroughAuth?ICONST_1:ICONST_0);
            mv.visitInsn(usingReturn?ICONST_1:ICONST_0);
            mv.visitMethodInsn(INVOKEVIRTUAL, "org/teiid/jboss/rest/"+modelName, "executePost", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Lorg/jboss/resteasy/plugins/providers/multipart/MultipartFormDataInput;Ljava/lang/String;ZZ)Ljavax/ws/rs/core/StreamingOutput;");
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

    private void addConsumes(MethodVisitor mv, String contentType) {
        AnnotationVisitor av0;
        av0 = mv.visitAnnotation("Ljavax/ws/rs/Consumes;", true);
        {
        AnnotationVisitor av1 = av0.visitArray("value");
        av1.visit(null, contentType);
        av1.visitEnd();
        }
        av0.visitEnd();
    }

    private void buildQueryProcedure(String vdbName, String vdbVersion, String modelName, String context, ClassWriter cw, boolean passthroughAuth) {
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
            addConsumes(mv, "application/x-www-form-urlencoded");
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
                av0 = mv.visitAnnotation("Lio/swagger/annotations/ApiResponses;", true);
                ApiResponse[] array = new ApiResponse[]{};
                AnnotationVisitor av1 = av0.visitArray("value");
                for(int i = 0 ; i < array.length ; i ++) {
                    av1.visit("value", array[i]);
                }
                av1.visitEnd();
                av0.visitEnd();
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
            mv.visitLdcInsn(vdbVersion);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitInsn(context.equals("xml")?ICONST_0:ICONST_1);
            mv.visitInsn(passthroughAuth?ICONST_1:ICONST_0);
            mv.visitMethodInsn(INVOKEVIRTUAL, "org/teiid/jboss/rest/"+modelName, "executeQuery", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;ZZ)Ljavax/ws/rs/core/StreamingOutput;");
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
