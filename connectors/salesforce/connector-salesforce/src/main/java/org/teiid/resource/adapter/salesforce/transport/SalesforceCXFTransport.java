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
package org.teiid.resource.adapter.salesforce.transport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;

import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.MessageHandler;
import com.sforce.ws.MessageHandlerWithHeaders;
import com.sforce.ws.tools.VersionInfo;
import com.sforce.ws.transport.JdkHttpTransport;
import com.sforce.ws.util.Base64;
import com.sforce.ws.util.FileUtil;

public class SalesforceCXFTransport extends JdkHttpTransport {
    private ByteArrayOutputStream payload = new ByteArrayOutputStream();
    private boolean successful;
    private SalesforceConnectorConfig config;
    private WebClient client;
    private URL url;

    public void setConfig(ConnectorConfig config) {
        this.config = (SalesforceConnectorConfig)config;
    }

    @Override
    public OutputStream connect(String uri, String soapAction) throws IOException {
        if (soapAction == null) {
            soapAction = "";
        }

        this.url = new URL(uri);
        HashMap<String, String> header = new HashMap<String, String>();

        header.put("SOAPAction", "\"" + soapAction + "\"");
        header.put("Content-Type", "text/xml; charset=UTF-8");
        header.put("Accept", "text/xml");

        return connectLocal(uri, header);
    }
    
    private OutputStream connectLocal(String uri, HashMap<String, String> httpHeaders) throws IOException {
        return connectLocal(uri, httpHeaders, true);
    }

    private OutputStream connectLocal(String uri, HashMap<String, String> httpHeaders, boolean enableCompression)
            throws IOException {
        return wrapOutput(connectRaw(uri, httpHeaders, enableCompression), enableCompression);
    }

    private OutputStream wrapOutput(OutputStream output, boolean enableCompression) throws IOException {
        if (config.getMaxRequestSize() > 0) {
            output = new LimitingOutputStream(config.getMaxRequestSize(), output);
        }

        // when we are writing a zip file we don't bother with compression
        if (enableCompression && config.isCompression()) {
            output = new GZIPOutputStream(output);
        }

        if (config.isTraceMessage()) {
            output = new TeeOutputStream(output);
        }

        if (config.hasMessageHandlers()) {
            output = new MessageHandlerOutputStream(output);
        }

        return output;
    }

    private OutputStream connectRaw(String uri, HashMap<String, String> httpHeaders, boolean enableCompression)
            throws IOException {
        
        if (config.isTraceMessage()) {
            config.getTraceStream().println( "WSC: Creating a new connection to " + uri + " Proxy = " +
                    config.getProxy() + " username " + config.getProxyUsername());
        }
        
        if (this.config.getCxfConfigFile() == null) {
            this.client = WebClient.create(uri);
        }
        else {
            this.client = WebClient.create(uri, this.config.getCxfConfigFile());
        }        
        
        this.client.header("User-Agent", VersionInfo.info());
        
        /*
         * Add all the client specific headers here
         */
        if (config.getHeaders() != null) {
            for (Entry<String, String> ent : config.getHeaders().entrySet()) {
                this.client.header(ent.getKey(), ent.getValue());
            }
        }
        
        if (enableCompression && config.isCompression()) {
            this.client.header("Content-Encoding", "gzip");
            this.client.header("Accept-Encoding", "gzip");
        }

        if (config.getProxyUsername() != null) {
            String token = config.getProxyUsername() + ":" + config.getProxyPassword();
            String auth = "Basic " + new String(Base64.encode(token.getBytes()));
            this.client.header("Proxy-Authorization", auth);
            this.client.header("Https-Proxy-Authorization", auth);
        }

        if (httpHeaders != null) {
            for (Map.Entry<String, String> entry : httpHeaders.entrySet()) {
                this.client.header(entry.getKey(), entry.getValue());
            }
        }

        HTTPClientPolicy clientPolicy = WebClient.getConfig(this.client).getHttpConduit().getClient();
        if (config.getReadTimeout() != 0) {
            clientPolicy.setReceiveTimeout(config.getReadTimeout());
        }

        if (config.getConnectionTimeout() != 0) {
            clientPolicy.setConnectionTimeout(config.getConnectionTimeout());
        }

        if (config.useChunkedPost()) {
            clientPolicy.setAllowChunking(true);
            clientPolicy.setChunkLength(4096);
        }
        
        if (config.getProxy() != Proxy.NO_PROXY) {
            InetSocketAddress addr = (InetSocketAddress)config.getProxy().address();
            clientPolicy.setProxyServer(addr.getHostName());
            clientPolicy.setProxyServerPort(addr.getPort());
        }
                
        return this.payload;
    }    

    @Override
    public InputStream getContent() throws IOException {
        javax.ws.rs.core.Response response = client.post(new ByteArrayInputStream(this.payload.toByteArray()));
        successful = true;
        InputStream in = (InputStream)response.getEntity();            
        if (response.getStatus() != 200) {
            successful = false;
        }

        if (!successful) {
            return in;
        }
        
        String encoding = response.getHeaderString("Content-Encoding");

        if (config.getMaxResponseSize() > 0) {
            in = new LimitingInputStream(config.getMaxResponseSize(), in);
        }

        if ("gzip".equals(encoding)) {
            in = new GZIPInputStream(in);
        }

        if (config.hasMessageHandlers() || config.isTraceMessage()) {
            byte[] bytes = FileUtil.toBytes(in);
            in = new ByteArrayInputStream(bytes);

            if (config.hasMessageHandlers()) {
                Iterator<MessageHandler> it = config.getMessagerHandlers();
                while(it.hasNext()) {
                    MessageHandler handler = it.next();
                    if (handler instanceof MessageHandlerWithHeaders) {
                        ((MessageHandlerWithHeaders) handler).handleResponse(url, bytes, modify(response.getHeaders()));
                    } else {
                        handler.handleResponse(url, bytes);
                    }
                }
            }

            if (config.isTraceMessage()) {
                MultivaluedMap<String, Object> headers = response.getHeaders();
                for (Map.Entry header : headers.entrySet()) {
                    config.getTraceStream().print(header.getKey());
                    config.getTraceStream().print("=");
                    config.getTraceStream().println(header.getValue());
                }
                
                new TeeInputStream(config, bytes);
            }
        }

        return in;
    }

    private Map<String, List<String>> modify(MultivaluedMap<String, Object> orig) {
        
        HashMap<String, List<String>> modified = new HashMap<String, List<String>>();
        for (String key:orig.keySet()) {
            Object anObj = orig.get(key);
            List<String> modifiedList = new ArrayList<String>();
            if (anObj instanceof List<?>) {
                List<?> list = (List<?>)anObj;
                for (Object o:list) {
                    modifiedList.add(o.toString());
                }
            } else {
                modifiedList.add(anObj.toString());                
            }
            modified.put(key, modifiedList);
        }
        return modified;
    }
    
    @Override
    public boolean isSuccessful() {
        return successful;
    }
}
