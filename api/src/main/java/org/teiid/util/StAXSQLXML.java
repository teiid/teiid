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

package org.teiid.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.sql.SQLException;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stax.StAXSource;

import org.teiid.connector.DataPlugin;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.StandardXMLTranslator;
import org.teiid.core.types.Streamable;

/**
 * NOTE that this representation of XML does become unreadable after a read operation.
 */
public class StAXSQLXML extends SQLXMLImpl {

    public interface StAXSourceProvider {
        StAXSource getStaxSource() throws SQLException;
    }

    private static final class SingleUseStAXSourceProvider implements
            StAXSourceProvider {
        private StAXSource source;

        public SingleUseStAXSourceProvider(StAXSource source) {
            this.source = source;
        }

        @Override
        public StAXSource getStaxSource() throws SQLException {
            if (source == null) {
                throw new SQLException(DataPlugin.Util.gs(DataPlugin.Event.TEIID60019));
            }
            StAXSource result = source;
            source = null;
            return result;
        }
    }

    private StAXSourceProvider sourceProvider;

    public StAXSQLXML(StAXSource source) {
        this(new SingleUseStAXSourceProvider(source), Streamable.CHARSET);
    }

    public StAXSQLXML(StAXSourceProvider provider, Charset charSet) {
        this.sourceProvider = provider;
        this.setCharset(charSet);
    }

    @SuppressWarnings("unchecked")
    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
        if (sourceClass == null || sourceClass == StAXSource.class) {
            return (T) sourceProvider.getStaxSource();
        }
        return super.getSource(sourceClass);
    }

    @Override
    public String getString() throws SQLException {
        StringWriter sw = new StringWriter();
        try {
            new StandardXMLTranslator(getSource(StAXSource.class)).translate(sw);
        } catch (TransformerException e) {
            throw new SQLException(e);
        } catch (IOException e) {
            throw new SQLException(e);
        }
        return sw.toString();
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        try {
            return new XMLInputStream(getSource(StAXSource.class), XMLOutputFactory.newFactory(), getCharset().name());
        } catch (XMLStreamException e) {
            throw new SQLException(e);
        } catch (FactoryConfigurationError e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        try {
            return new XMLReader(getSource(StAXSource.class), XMLOutputFactory.newFactory());
        } catch (XMLStreamException e) {
            throw new SQLException(e);
        } catch (FactoryConfigurationError e) {
            throw new SQLException(e);
        }
    }

}