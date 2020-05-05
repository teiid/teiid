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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.InputStreamFactory.BlobInputStreamFactory;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.language.Argument;
import org.teiid.language.Argument.Direction;
import org.teiid.language.Call;
import org.teiid.language.Literal;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.ws.BinaryWSProcedureExecution;
import org.teiid.translator.ws.WSConnection;

public class S3ProcedureExecution implements ProcedureExecution {

    private static String AWS_S3_URL_PREFIX = "https://s3."; //$NON-NLS-1$
    private static String AWS_S3_HOSTHAME = "amazonaws.com"; //$NON-NLS-1$

    private final Call command;
    private final WSConnection conn;
    private S3ExecutionFactory ef;
    private RuntimeMetadata metadata;
    private ExecutionContext ec;
    private String baseEndpoint;

    private String endpoint;

    private BinaryWSProcedureExecution execution = null;
    boolean isText = false;
    boolean isList = false;
    boolean streaming = false;

    public S3ProcedureExecution(Call command, S3ExecutionFactory ef, RuntimeMetadata metadata, ExecutionContext ec,
            WSConnection conn) {
        this.command = command;
        this.conn = conn;
        this.ef = ef;
        this.metadata = metadata;
        this.ec = ec;
        if (this.conn.getEndPoint() != null) {
            baseEndpoint = this.conn.getEndPoint();
        }
    }

    @Override
    public void execute() throws TranslatorException {
        List<Argument> arguments = this.command.getArguments();

        if (command.getProcedureName().equalsIgnoreCase(S3ExecutionFactory.SAVEFILE)) {
            this.execution = saveFile(arguments);
            this.execution.execute();
            if (this.execution.getResponseCode() != 200) {
                throw new TranslatorException(S3ExecutionFactory.UTIL.gs("error_writing", this.endpoint,
                        this.execution.getResponseCode(), getErrorDescription()));
            }
        } else if (command.getProcedureName().equalsIgnoreCase(S3ExecutionFactory.DELETEFILE)) {
            this.execution = deleteFile(arguments);
            this.execution.execute();
            if (this.execution.getResponseCode() != 204) {
                throw new TranslatorException(S3ExecutionFactory.UTIL.gs("error_deleting", this.endpoint,
                        this.execution.getResponseCode(), getErrorDescription()));
            }
        } else if (command.getProcedureName().equalsIgnoreCase(S3ExecutionFactory.GETFILE)
                || command.getProcedureName().equalsIgnoreCase(S3ExecutionFactory.GETTEXTFILE)) {

            if (command.getProcedureName().equalsIgnoreCase(S3ExecutionFactory.GETTEXTFILE)) {
                this.isText = true;
            }
            this.execution = getFile(arguments);
            this.execution.execute();
            if (this.execution.getResponseCode() != 200) {
                throw new TranslatorException(S3ExecutionFactory.UTIL.gs("error_reading", this.endpoint,
                        this.execution.getResponseCode(), getErrorDescription()));
            }
        } else if (command.getProcedureName().equalsIgnoreCase(S3ExecutionFactory.LISTBUCKET)) {
            this.execution = listBucket(arguments);
            this.execution.execute();
            if (this.execution.getResponseCode() != 200) {
                throw new TranslatorException(S3ExecutionFactory.UTIL.gs("error_list", this.endpoint,
                        this.execution.getResponseCode(), getErrorDescription()));
            }
        } else if (command.getProcedureName().equalsIgnoreCase(S3ExecutionFactory.LISTBUCKETV1)) {
            this.execution = listBucketV1(arguments);
            this.execution.execute();
            if (this.execution.getResponseCode() != 200) {
                throw new TranslatorException(S3ExecutionFactory.UTIL.gs("error_list", this.endpoint,
                        this.execution.getResponseCode(), getErrorDescription()));
            }
        }
    }

    // should use chunking based save, but it is little complex
    // see http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-examples-using-sdks.html
    // for example.
    private BinaryWSProcedureExecution saveFile(List<Argument> arguments) throws TranslatorException {
        String name = (String)arguments.get(0).getArgumentValue().getValue();
        String bucket = (String)arguments.get(1).getArgumentValue().getValue();
        String region = (String)arguments.get(2).getArgumentValue().getValue();
        this.endpoint = (String)arguments.get(3).getArgumentValue().getValue();
        String accessKey = (String)arguments.get(4).getArgumentValue().getValue();
        String secretKey = (String)arguments.get(5).getArgumentValue().getValue();

        if (bucket == null) {
            bucket = this.ef.getBucket();
        }

        if (region == null) {
            region = this.ef.getRegion();
        }

        determineEndpoint(name, bucket, region);

        if (accessKey == null) {
            accessKey = this.ef.getAccesskey();
        }

        if (secretKey == null) {
            secretKey = this.ef.getSecretkey();
        }

        Object file = command.getArguments().get(6).getArgumentValue().getValue();
        if (file == null) {
            throw new TranslatorException(S3ExecutionFactory.UTIL.getString("non_null")); //$NON-NLS-1$
        }
        try {
            long length = 0;
            byte[] contents = null;
            if (file instanceof XMLType){
                length = ((XMLType)file).length();
                contents = ObjectConverterUtil.convertToByteArray(((XMLType)file).getBinaryStream());
            } else if (file instanceof Clob) {
                length = ((Clob)file).length();
                contents = ObjectConverterUtil.convertToByteArray(((Clob)file).getAsciiStream());
            } else if (file instanceof Blob) {
                length = ((Blob)file).length();
                contents = ObjectConverterUtil.convertToByteArray(((Blob)file).getBinaryStream());
            } else if (file instanceof String) {
                length = ((String)file).length();
                contents = ((String)file).getBytes();
            } else {
                throw new TranslatorException(S3ExecutionFactory.UTIL.getString("unknown_type")); //$NON-NLS-1$
            }

            byte[] contentHash = AWS4SignerBase.hash(contents);
            String contentHashString = BinaryUtils.toHex(contentHash);

            Map<String, String> headers = new HashMap<String, String>();
            headers.put("x-amz-content-sha256", contentHashString);
            headers.put("content-length", "" + length);
            headers.put("x-amz-storage-class", "STANDARD");

            if (accessKey != null) {
                AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(new URL(endpoint), "PUT",
                        "s3", region);
                String authorization = signer.computeSignature(headers, null, contentHashString, accessKey, secretKey);
                headers.put("Authorization", authorization);
            }
            headers.put("Content-Type", "application/octet-stream");

            LogManager.logDetail(LogConstants.CTX_WS, "Saving", endpoint); //$NON-NLS-1$
            return invokeHTTP("PUT", endpoint, new BlobType(contents), headers);
        } catch (SQLException | IOException e) {
            throw new TranslatorException(e);
        }
    }

    protected String determineEndpoint(String name, String bucket, String region) throws TranslatorException {
        if (endpoint != null) {
            return endpoint;
        }
        if (bucket == null) {
            throw new TranslatorException(S3ExecutionFactory.UTIL.gs("no_bucket")); //$NON-NLS-1$
        }
        if (baseEndpoint != null) {
            endpoint = baseEndpoint;
        } else if (region.equalsIgnoreCase(S3ExecutionFactory.US_EAST_1)) {
            endpoint = AWS_S3_URL_PREFIX + AWS_S3_HOSTHAME;
        } else {
            endpoint = AWS_S3_URL_PREFIX + region + "." + AWS_S3_HOSTHAME; //$NON-NLS-1$
        }
        //TODO: is url encoding needed here
        endpoint += ((!endpoint.endsWith("/")?"/":"") + bucket + "/" + name); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        return endpoint;
    }

    private BinaryWSProcedureExecution getFile(List<Argument> arguments) throws TranslatorException {
        String name = (String)arguments.get(0).getArgumentValue().getValue();
        String bucket = (String)arguments.get(1).getArgumentValue().getValue();
        String region = (String)arguments.get(2).getArgumentValue().getValue();
        this.endpoint = (String)arguments.get(3).getArgumentValue().getValue();
        String accessKey = (String)arguments.get(4).getArgumentValue().getValue();
        String secretKey = (String)arguments.get(5).getArgumentValue().getValue();

        String encryption = (String)arguments.get(6).getArgumentValue().getValue();
        String encryptionKey = (String)arguments.get(7).getArgumentValue().getValue();
        Boolean isStreaming = (Boolean)arguments.get(8).getArgumentValue().getValue();

        if (isStreaming != null) {
            this.streaming = isStreaming;
        }

        if (bucket == null) {
            bucket = this.ef.getBucket();
        }

        if (region == null) {
            region = this.ef.getRegion();
        }

        determineEndpoint(name, bucket, region);

        if (accessKey == null) {
            accessKey = this.ef.getAccesskey();
        }

        if (secretKey == null) {
            secretKey = this.ef.getSecretkey();
        }

        if (encryption == null) {
            encryption = this.ef.getEncryption();
        }

        if (encryptionKey == null) {
            encryptionKey = this.ef.getEncryptionkey();
        }

        Map<String, String> headers = new HashMap<String, String>();
        try {
            if (accessKey != null) {
                AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(new URL(endpoint), "GET",
                        "s3", region);
                headers.put("x-amz-content-sha256", AWS4SignerBase.EMPTY_BODY_SHA256);
                String authorization = signer.computeSignature(headers, null, AWS4SignerBase.EMPTY_BODY_SHA256, accessKey,
                        secretKey);
                headers.put("Authorization", authorization);
            }

            if (encryption != null) {
                assert encryptionKey != null;
                headers.put("x-amz-server-side​-encryption​-customer-algorithm", encryption);
                headers.put("x-amz-server-side​-encryption​-customer-key", encryptionKey);
                MessageDigest messageDigest = MessageDigest.getInstance("md5");
                headers.put("x-amz-server-side​-encryption​-customer-key-MD5",
                        Base64.getEncoder().encodeToString(messageDigest.digest(encryptionKey.getBytes())));
            }

            LogManager.logDetail(LogConstants.CTX_WS, "Getting", endpoint); //$NON-NLS-1$
            return invokeHTTP("GET", endpoint, null, headers);
        } catch (MalformedURLException | NoSuchAlgorithmException e) {
            throw new TranslatorException(e);
        }
    }

    private BinaryWSProcedureExecution deleteFile(List<Argument> arguments) throws TranslatorException {
        String name = (String)arguments.get(0).getArgumentValue().getValue();
        String bucket = (String)arguments.get(1).getArgumentValue().getValue();
        String region = (String)arguments.get(2).getArgumentValue().getValue();
        this.endpoint = (String)arguments.get(3).getArgumentValue().getValue();
        String accessKey = (String)arguments.get(4).getArgumentValue().getValue();
        String secretKey = (String)arguments.get(5).getArgumentValue().getValue();

        if (bucket == null) {
            bucket = this.ef.getBucket();
        }

        if (region == null) {
            region = this.ef.getRegion();
        }

        determineEndpoint(name, bucket, region);

        if (accessKey == null) {
            accessKey = this.ef.getAccesskey();
        }

        if (secretKey == null) {
            secretKey = this.ef.getSecretkey();
        }

        try {
            Map<String, String> headers = new HashMap<String, String>();
            if (accessKey != null) {
                AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                        new URL(endpoint), "DELETE", "s3", region);
                headers.put("x-amz-content-sha256", AWS4SignerBase.EMPTY_BODY_SHA256);
                String authorization = signer.computeSignature(headers, null, // no query parameters
                        AWS4SignerBase.EMPTY_BODY_SHA256, accessKey, secretKey);
                headers.put("Authorization", authorization);
            }
            headers.put("Content-Type", "text/plain");
            LogManager.logDetail(LogConstants.CTX_WS, "Deleting", endpoint); //$NON-NLS-1$
            return invokeHTTP("DELETE", endpoint, null, headers);
        } catch (MalformedURLException e) {
            throw new TranslatorException(e);
        }
    }

    private BinaryWSProcedureExecution listBucketV1(List<Argument> arguments) throws TranslatorException {
        String bucket = (String)arguments.get(0).getArgumentValue().getValue();
        String region = (String)arguments.get(1).getArgumentValue().getValue();
        String accessKey = (String)arguments.get(2).getArgumentValue().getValue();
        String secretKey = (String)arguments.get(3).getArgumentValue().getValue();

        if (bucket == null) {
            bucket = this.ef.getBucket();
        }

        if (region == null) {
            region = this.ef.getRegion();
        }

        determineEndpoint("", bucket, region);

        if (accessKey == null) {
            accessKey = this.ef.getAccesskey();
        }

        if (secretKey == null) {
            secretKey = this.ef.getSecretkey();
        }

        Map<String, String> headers = new HashMap<String, String>();
        try {
            if (accessKey != null) {
                URL url = new URL(endpoint);
                AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(url, "GET",
                        "s3", region);
                Map<String, String> query = new HashMap<String, String>();
                headers.put("x-amz-content-sha256", AWS4SignerBase.EMPTY_BODY_SHA256);
                String authorization = signer.computeSignature(headers, query, AWS4SignerBase.EMPTY_BODY_SHA256, accessKey,
                        secretKey);
                headers.put("Authorization", authorization);
            }
            this.isText = true;
            this.isList = true;
            LogManager.logDetail(LogConstants.CTX_WS, "Getting", endpoint); //$NON-NLS-1$
            return invokeHTTP("GET", endpoint, null, headers);
        } catch (MalformedURLException e) {
            throw new TranslatorException(e);
        }
    }

    private BinaryWSProcedureExecution listBucket(List<Argument> arguments) throws TranslatorException {
        String bucket = (String)arguments.get(0).getArgumentValue().getValue();
        String region = (String)arguments.get(1).getArgumentValue().getValue();
        String accessKey = (String)arguments.get(2).getArgumentValue().getValue();
        String secretKey = (String)arguments.get(3).getArgumentValue().getValue();
        String next = (String)arguments.get(4).getArgumentValue().getValue();

        if (bucket == null) {
            bucket = this.ef.getBucket();
        }

        if (region == null) {
            region = this.ef.getRegion();
        }

        determineEndpoint("?list-type=2", bucket, region);
        if (next != null) {
            endpoint += "&continuation-token="+next;
        }

        if (accessKey == null) {
            accessKey = this.ef.getAccesskey();
        }

        if (secretKey == null) {
            secretKey = this.ef.getSecretkey();
        }

        Map<String, String> headers = new HashMap<String, String>();
        try {
            if (accessKey != null) {
                URL url = new URL(endpoint);
                AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(url, "GET",
                        "s3", region);
                Map<String, String> query = new HashMap<String, String>();
                query.put("list-type", "2");
                if (next != null) {
                    query.put("continuation-token", next);
                }
                headers.put("x-amz-content-sha256", AWS4SignerBase.EMPTY_BODY_SHA256);
                String authorization = signer.computeSignature(headers, query, AWS4SignerBase.EMPTY_BODY_SHA256, accessKey,
                        secretKey);
                headers.put("Authorization", authorization);
            }
            this.isText = true;
            this.isList = true;
            LogManager.logDetail(LogConstants.CTX_WS, "Getting", endpoint); //$NON-NLS-1$
            return invokeHTTP("GET", endpoint, null, headers);
        } catch (MalformedURLException e) {
            throw new TranslatorException(e);
        }
    }

    protected BinaryWSProcedureExecution invokeHTTP(String method,
            String uri, Object payload, Map<String, String> headers)
            throws TranslatorException {

        Map<String, List<String>> targetHeaders = new HashMap<String, List<String>>();
        headers.forEach((k,v) -> targetHeaders.put(k, Arrays.asList(v)));

        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_WS, MessageLevel.DETAIL)) {
            try {
                LogManager.logDetail(LogConstants.CTX_WS,
                        "Source-URL=", URLDecoder.decode(uri, "UTF-8")); //$NON-NLS-1$ //$NON-NLS-2$
            } catch (UnsupportedEncodingException e) {
            }
        }

        List<Argument> parameters = new ArrayList<Argument>();
        parameters.add(new Argument(Direction.IN, new Literal(method, TypeFacility.RUNTIME_TYPES.STRING), null));
        parameters.add(new Argument(Direction.IN, new Literal(payload, TypeFacility.RUNTIME_TYPES.OBJECT), null));
        parameters.add(new Argument(Direction.IN, new Literal(uri, TypeFacility.RUNTIME_TYPES.STRING), null));
        parameters.add(new Argument(Direction.IN, new Literal(true, TypeFacility.RUNTIME_TYPES.BOOLEAN), null));
        //the engine currently always associates out params at resolve time even if the
        // values are not directly read by the call
        parameters.add(new Argument(Direction.OUT, TypeFacility.RUNTIME_TYPES.STRING, null));

        Call call = this.ef.getLanguageFactory().createCall("invokeHttp", parameters, null);

        BinaryWSProcedureExecution execution = new BinaryWSProcedureExecution(call, this.metadata, this.ec, null,
                this.conn);
        execution.setUseResponseContext(true);
        execution.setCustomHeaders(targetHeaders);
        return execution;
    }

    @Override
    public void close() {

    }

    @Override
    public void cancel() throws TranslatorException {

    }

    @SuppressWarnings("rawtypes")
    private String getHeader(String name) {
        List list = (List)execution.getResponseHeader(name);
        return (list == null || list.isEmpty()) ? null:(String)list.get(0);
    }

    private String getErrorDescription() throws TranslatorException {
        try {
            if (this.execution != null) {
                Blob contents = (Blob)execution.getOutputParameterValues().get(0);
                return ObjectConverterUtil.convertToString(contents.getBinaryStream());
            }
            return null;
        } catch (NumberFormatException |IOException | SQLException e) {
            throw new TranslatorException(e);
        }
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        if (this.command.getProcedureName().equalsIgnoreCase(S3ExecutionFactory.SAVEFILE)
                || this.command.getProcedureName().equalsIgnoreCase(S3ExecutionFactory.DELETEFILE)) {
            return null;
        }

        if (this.execution == null || this.execution.getResponseCode() < 200
                || this.execution.getResponseCode() > 300) {
            return null;
        }

        Blob contents = (Blob)execution.getOutputParameterValues().get(0);
        BlobInputStreamFactory isf = new BlobInputStreamFactory(contents);

        String length = getHeader("Content-Length");
        if (length != null) {
            isf.setLength(Long.parseLong(length));
        }

        Object value = null;
        if (isText) {
            ClobImpl clob = new ClobImpl(isf, -1);
            clob.setCharset(Charset.forName(this.ef.getEncoding()));
            value = new ClobType(clob);
            if (!streaming) {
                value = new InputStreamFactory.ClobInputStreamFactory(clob);
            }
        } else {
            if (streaming) {
                value = new BlobType(contents);
            } else {
                value = isf;
            }
        }

        String lastModified = getHeader("Last-Modified");

        ArrayList<Object> result = new ArrayList<Object>(2);
        result.add(value);
        if (!isList) {
            result.add(endpoint);
            try {
                SimpleDateFormat df = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss zzz");
                result.add(lastModified == null?null:new Timestamp(df.parse(lastModified).getTime()));
            } catch (ParseException e) {
                result.add(null);
            }
            result.add(getHeader("ETag"));
            result.add(length);
        }
        this.execution = null;
        return result;
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return Collections.emptyList();
    }
}