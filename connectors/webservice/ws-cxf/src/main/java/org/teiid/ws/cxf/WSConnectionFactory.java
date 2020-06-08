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

package org.teiid.ws.cxf;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import javax.xml.namespace.QName;

import org.apache.cxf.Bus;
import org.apache.cxf.bus.spring.SpringBusFactory;
import org.apache.cxf.configuration.Configurer;
import org.apache.cxf.interceptor.Interceptor;
import org.apache.cxf.jaxws.JaxWsClientFactoryBean;
import org.teiid.core.BundleUtil;
import org.teiid.translator.TranslatorException;

public class WSConnectionFactory implements Closeable {

    public static final BundleUtil UTIL = BundleUtil.getBundleUtil(WSConnectionFactory.class);

    private URL wsdlUrl;
    private List<? extends Interceptor> outInterceptors;
    private Bus bus;
    private QName portQName;
    private QName serviceQName;
    private WSConfiguration config;

    public WSConnectionFactory(WSConfiguration config) throws TranslatorException {
        this.config = config;
        String endPointName = config.getEndPointName();
        if (endPointName == null) {
            endPointName = WSConfiguration.DEFAULT_LOCAL_NAME;
        }
        String serviceName = config.getServiceName();
        if (serviceName == null) {
            serviceName = WSConfiguration.DEFAULT_LOCAL_NAME;
        }
        String namespaceUri = config.getNamespaceUri();
        if (namespaceUri == null) {
            namespaceUri = WSConfiguration.DEFAULT_NAMESPACE_URI;
        }
        this.portQName = new QName(namespaceUri, endPointName);
        this.serviceQName = new QName(namespaceUri, serviceName);
        if (config.getWsdl() != null) {
            try {
                this.wsdlUrl = new URL(config.getWsdl());
            } catch (MalformedURLException e) {
                File f = new File(config.getWsdl());
                try {
                    this.wsdlUrl = f.toURI().toURL();
                } catch (MalformedURLException e1) {
                    throw new TranslatorException(e1);
                }
            }
        }
        if (config.getConfigFile() != null) {
            this.bus = new SpringBusFactory().createBus(config.getConfigFile());
            JaxWsClientFactoryBean instance = new JaxWsClientFactoryBean();
            if (config.getWsdl() == null) {
                Configurer configurer = this.bus.getExtension(Configurer.class);
                if (null != configurer) {
                    configurer.configureBean(this.portQName.toString() + ".jaxws-client.proxyFactory", instance); //$NON-NLS-1$
                }
                this.outInterceptors = instance.getOutInterceptors();
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (bus != null) {
            bus.shutdown(false);
        }
    }

    public Bus getBus() {
        return bus;
    }

    public WSConfiguration getConfig() {
        return config;
    }

    public List<? extends Interceptor> getOutInterceptors() {
        return outInterceptors;
    }

    public QName getPortQName() {
        return portQName;
    }

    public QName getServiceQName() {
        return serviceQName;
    }

    public URL getWsdlUrl() {
        return wsdlUrl;
    }

}
