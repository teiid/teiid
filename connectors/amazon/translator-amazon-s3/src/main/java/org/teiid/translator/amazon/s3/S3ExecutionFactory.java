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

package org.teiid.translator.amazon.s3;

import java.nio.charset.Charset;

import org.teiid.core.BundleUtil;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Call;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.resource.api.ConnectionFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.ws.WSConnection;
import org.teiid.util.CharsetUtils;

@Translator(name="amazon-s3", description="Amazon S3 Translator, reads contents of files or writes to them")
public class S3ExecutionFactory extends ExecutionFactory<ConnectionFactory, WSConnection> {


    public static final String US_EAST_1 = "US-EAST-1"; //$NON-NLS-1$

    public static BundleUtil UTIL = BundleUtil.getBundleUtil(S3ExecutionFactory.class);

    public static final String GETTEXTFILE = "getTextFile"; //$NON-NLS-1$
    public static final String GETFILE = "getFile"; //$NON-NLS-1$
    public static final String SAVEFILE = "saveFile"; //$NON-NLS-1$
    public static final String DELETEFILE = "deleteFile"; //$NON-NLS-1$
    public static final String LISTBUCKET = "list"; //$NON-NLS-1$
    public static final String LISTBUCKETV1 = "listv1"; //$NON-NLS-1$

    private Charset encoding = Charset.defaultCharset();
    private String awsAccessKey;
    private String awsSecretKey;
    private String bucket;
    private String region = US_EAST_1;
    private String encryption;
    private String encryptionKey;

    public S3ExecutionFactory() {
        setTransactionSupport(TransactionSupport.NONE);
        setSourceRequiredForMetadata(false);
    }

    @TranslatorProperty(display="File Encoding",advanced=true)
    public String getEncoding() {
        return encoding.name();
    }

    public void setEncoding(String encoding) {
        this.encoding = CharsetUtils.getCharset(encoding);
    }

    @TranslatorProperty(display="Amazon Access Key",advanced=true)
    public String getAccesskey() {
        return awsAccessKey;
    }

    public void setAccesskey(String value) {
        this.awsAccessKey = value;
    }

    @TranslatorProperty(display="Amazon Secret Key",advanced=true)
    public String getSecretkey() {
        return awsSecretKey;
    }

    public void setSecretkey(String value) {
        this.awsSecretKey = value;
    }

    @TranslatorProperty(display="Amazon Region",advanced=true)
    public String getRegion() {
        return region;
    }

    public void setRegion(String value) {
        this.region = value;
    }

    @TranslatorProperty(display="Amazon Bucket",advanced=true)
    public String getBucket() {
        return bucket;
    }

    public void setBucket(String value) {
        this.bucket = value;
    }

    @TranslatorProperty(display="Server Side Customer Encryption Algorithm Used",advanced=true)
    public String getEncryption() {
        return encryption;
    }

    public void setEncryption(String value) {
        this.encryption = value;
    }

    @TranslatorProperty(display="Server Side Customer Encryption Key to be used to decrypt the object",advanced=true)
    public String getEncryptionkey() {
        return encryptionKey;
    }

    public void setEncryptionkey(String value) {
        this.encryptionKey = value;
    }

    @Override
    public ProcedureExecution createProcedureExecution(final Call command,
            final ExecutionContext executionContext, final RuntimeMetadata metadata,
            final WSConnection conn) throws TranslatorException {
        return new S3ProcedureExecution(command, this, metadata, executionContext, conn);
    }

    @Override
    public void getMetadata(MetadataFactory metadataFactory, WSConnection connection) throws TranslatorException {
        addGetTextFileMethod(metadataFactory);
        addGetFileMethod(metadataFactory);

        saveFile(metadataFactory);
        deleteFile(metadataFactory);
        listBucket(metadataFactory);
        listBucketV1(metadataFactory);

        Table t = metadataFactory.addTable("Bucket");
        t.setVirtual(true);
        t.setSupportsUpdate(false);
        metadataFactory.addColumn("Key", DataTypeManager.DefaultDataTypes.STRING, t);
        metadataFactory.addColumn("LastModified", DataTypeManager.DefaultDataTypes.STRING, t);
        metadataFactory.addColumn("ETag", DataTypeManager.DefaultDataTypes.STRING, t);
        metadataFactory.addColumn("Size", DataTypeManager.DefaultDataTypes.STRING, t);
        metadataFactory.addColumn("StorageClass", DataTypeManager.DefaultDataTypes.STRING, t);
        metadataFactory.addColumn("IsTruncated", DataTypeManager.DefaultDataTypes.BOOLEAN, t);
        t.setSelectTransformation("select b.* from (exec \""+metadataFactory.getSchema().getName().replace("\"", "\"\"")+"\".listv1()) as a, " +
                " XMLTABLE(XMLNAMESPACES(DEFAULT 'http://s3.amazonaws.com/doc/2006-03-01/'), '/ListBucketResult/Contents' \n" +
                " PASSING XMLPARSE(CONTENT a.result WELLFORMED) COLUMNS Key string, LastModified string," +
                " ETag string, Size string, StorageClass string, \n" +
                " IsTruncated boolean PATH '../IsTruncated') as b;");
    }

    private void saveFile(MetadataFactory metadataFactory) {
        Procedure p = metadataFactory.addProcedure(SAVEFILE);
        p.setAnnotation("Saves the given value to the given bucket.  Any existing file will be overriden."); //$NON-NLS-1$

        ProcedureParameter param = metadataFactory.addProcedureParameter("name", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("The name of the object to save"); //$NON-NLS-1$

        addCommonParameters(metadataFactory, p);

        param = metadataFactory.addProcedureParameter("contents", TypeFacility.RUNTIME_NAMES.OBJECT, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("The contents to save.  Can be one of CLOB, BLOB, or XML"); //$NON-NLS-1$
    }

    private void listBucketV1(MetadataFactory metadataFactory) {
        Procedure p = metadataFactory.addProcedure(LISTBUCKET);
        p.setAnnotation("Lists the Objects in a given bucket."); //$NON-NLS-1$

        ProcedureParameter param;
        param = metadataFactory.addProcedureParameter("bucket", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("The name of the bucket in Amazon S3"); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("region", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("region in which the bucket exists on Amazon S3"); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("accesskey", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("Security Access Key, if not provided will use translator configured keys."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("secretkey", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("Security Secret Key, if not provided will use translator configured keys."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.CLOB, p); //$NON-NLS-1$
    }

    private void listBucket(MetadataFactory metadataFactory) {
        Procedure p = metadataFactory.addProcedure(LISTBUCKETV1);
        p.setAnnotation("Lists the Objects in a given bucket."); //$NON-NLS-1$

        ProcedureParameter param;
        param = metadataFactory.addProcedureParameter("bucket", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("The name of the bucket in Amazon S3"); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("region", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("region in which the bucket exists on Amazon S3"); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("accesskey", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("Security Access Key, if not provided will use translator configured keys."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("secretkey", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("Security Secret Key, if not provided will use translator configured keys."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("nexttoken", TypeFacility.RUNTIME_NAMES.STRING, //$NON-NLS-1$
                Type.In, p);
        param.setAnnotation("If the response is truncated, Amazon S3 returns this parameter with a "
                + "continuation token that you can specify as the continuation-token in your next request "
                + "to retrieve the next set of keys"); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        metadataFactory.addProcedureResultSetColumn("result", TypeFacility.RUNTIME_NAMES.CLOB, p); //$NON-NLS-1$
    }

    private void deleteFile(MetadataFactory metadataFactory) {
        Procedure p = metadataFactory.addProcedure(DELETEFILE);
        p.setAnnotation("Delete the given file from bucket."); //$NON-NLS-1$

        ProcedureParameter param = metadataFactory.addProcedureParameter("name", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("Name of the file"); //$NON-NLS-1$

        addCommonParameters(metadataFactory, p);
    }

    private void addCommonParameters(MetadataFactory metadataFactory, Procedure p) {
        ProcedureParameter param;
        param = metadataFactory.addProcedureParameter("bucket", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("The name of the bucket in Amazon S3"); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("region", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("region in which the bucket exists on Amazon S3"); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("endpoint", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("Endpoint point of the Object, if provided this overwirtes the name and bucket properties"); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("accesskey", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("Security Access Key, if not provided will use translator configured keys."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("secretkey", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("Security Secret Key, if not provided will use translator configured keys."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);
    }

    private void addGetTextFileMethod(MetadataFactory metadataFactory) {
        Procedure p = metadataFactory.addProcedure(GETTEXTFILE);
        p.setAnnotation("Returns text files that match the given path as CLOBs"); //$NON-NLS-1$

        ProcedureParameter param = metadataFactory.addProcedureParameter("name", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("The name of the file to return.  Currently the patterns like *.<ext> are not supported"); //$NON-NLS-1$

        addCommonParameters(metadataFactory, p);

        param = metadataFactory.addProcedureParameter("encryption", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("Server side encryption algorithm used"); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("encryptionkey", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("Server side encryption key to decrypt the object"); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("stream", TypeFacility.RUNTIME_NAMES.BOOLEAN, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("If the result should be streamed."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);
        param.setDefaultValue("false"); //$NON-NLS-1$

        metadataFactory.addProcedureResultSetColumn("file", TypeFacility.RUNTIME_NAMES.CLOB, p); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("endpoint", TypeFacility.RUNTIME_NAMES.STRING, p); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("lastModified", TypeFacility.RUNTIME_NAMES.TIMESTAMP, p); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("etag", TypeFacility.RUNTIME_NAMES.STRING, p); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("size", TypeFacility.RUNTIME_NAMES.LONG, p); //$NON-NLS-1$
    }

    private void addGetFileMethod(MetadataFactory metadataFactory) {
        Procedure p = metadataFactory.addProcedure(GETFILE);
        p.setAnnotation("Returns file that match the given path as BLOB"); //$NON-NLS-1$

        ProcedureParameter param = metadataFactory.addProcedureParameter("name", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("The name of the file to return.  Currently the patterns like *.<ext> are not supported"); //$NON-NLS-1$

        addCommonParameters(metadataFactory, p);

        param = metadataFactory.addProcedureParameter("encryption", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("Server side encryption algorithm used"); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("encryptionkey", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("Server side encryption key to decrypt the object"); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);

        param = metadataFactory.addProcedureParameter("stream", TypeFacility.RUNTIME_NAMES.BOOLEAN, Type.In, p); //$NON-NLS-1$
        param.setAnnotation("If the result should be streamed."); //$NON-NLS-1$
        param.setNullType(NullType.Nullable);
        param.setDefaultValue("false"); //$NON-NLS-1$

        metadataFactory.addProcedureResultSetColumn("file", TypeFacility.RUNTIME_NAMES.BLOB, p); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("endpoint", TypeFacility.RUNTIME_NAMES.STRING, p); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("lastModified", TypeFacility.RUNTIME_NAMES.TIMESTAMP, p); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("etag", TypeFacility.RUNTIME_NAMES.STRING, p); //$NON-NLS-1$
        metadataFactory.addProcedureResultSetColumn("size", TypeFacility.RUNTIME_NAMES.LONG, p); //$NON-NLS-1$
    }

    @Override
    public boolean areLobsUsableAfterClose() {
        return false;
    }

}
