package org.teiid.google.v4;

import org.teiid.core.TeiidRuntimeException;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;

public class ClientConstants {

    public static String ENCODING = "UTF-8"; //$NON-NLS-1$

    /** Global instance of the JSON factory. */
    public static final JsonFactory JSON_FACTORY =
        JacksonFactory.getDefaultInstance();

    /** Global instance of the HTTP transport. */
    public static HttpTransport HTTP_TRANSPORT;

    static {
        try {
            HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        } catch (Exception e) {
            throw new TeiidRuntimeException(e);
        }
    }

}
