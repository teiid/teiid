package org.teiid.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public interface MongoDBConfiguration {

    public static final String STANDARD_PREFIX = "mongodb://"; //$NON-NLS-1$
    public static final String SEEDLIST_PREFIX = "mongodb+srv://"; //$NON-NLS-1$

    public enum SecurityType {None, SCRAM_SHA_256, SCRAM_SHA_1, MONGODB_CR, Kerberos, X509};

    Boolean getSsl();

    /**
     * A list of servers to use.  If the string starts with mongodb:// or mongodb+srv://, then it will be used
     * as the full uri.  All other values will be assumed to be a ; separated list of servers.
     * @return
     */
    String getRemoteServerList();

    String getUsername();

    String getPassword();

    String getDatabase();

    /**
     * The {@link SecurityType}.  Can be one of SCRAM_SHA_256, SCRAM_SHA_1, MONGODB_CR, Kerberos, X509, None.
     * <br>Any other value will be treated as MONGODB_CR
     * @return
     */
    String getSecurityType();

    String getAuthDatabase();

    default MongoCredential getCredential() {

        MongoCredential credential = null;
        if (getSecurityType().equals(SecurityType.SCRAM_SHA_256.name())) {
            credential = MongoCredential.createScramSha256Credential(getUsername(),
                    (getAuthDatabase() == null) ? getDatabase(): getAuthDatabase(),
                    getPassword().toCharArray());
        }
        else if (getSecurityType().equals(SecurityType.SCRAM_SHA_1.name())) {
            credential = MongoCredential.createScramSha1Credential(getUsername(),
                    (getAuthDatabase() == null) ? getDatabase(): getAuthDatabase(),
                    getPassword().toCharArray());
        }
        else if (getSecurityType().equals(SecurityType.MONGODB_CR.name())) {
            credential = MongoCredential.createMongoCRCredential(getUsername(),
                    (getAuthDatabase() == null) ? getDatabase(): getAuthDatabase(),
                    getPassword().toCharArray());
        }
        else if (getSecurityType().equals(SecurityType.Kerberos.name())) {
            credential = MongoCredential.createGSSAPICredential(getUsername());
        }
        else if (getSecurityType().equals(SecurityType.X509.name())) {
            credential = MongoCredential.createMongoX509Credential(getUsername());
        } else if (getSecurityType().equals(SecurityType.None.name())) {
            // skip
        }
        else if (getUsername() != null && getPassword() != null) {
            // to support legacy pre-3.0 authentication
            credential = MongoCredential.createMongoCRCredential(
                    getUsername(),
                    (getAuthDatabase() == null) ? getDatabase(): getAuthDatabase(),
                    getPassword().toCharArray());
        }
        return credential;
    }

    default MongoClientOptions getOptions() {
        //if options needed then use URL format
        final MongoClientOptions.Builder builder = MongoClientOptions.builder();
        if (Boolean.TRUE.equals(getSsl())) {
            builder.sslEnabled(true);
        }
        return builder.build();
    }

    default List<ServerAddress> getServers() {
        String serverlist = getRemoteServerList();
        if (!serverlist.startsWith(STANDARD_PREFIX) && !serverlist.startsWith(SEEDLIST_PREFIX)) {
            List<ServerAddress> addresses = new ArrayList<ServerAddress>();
            StringTokenizer st = new StringTokenizer(serverlist, ";"); //$NON-NLS-1$
            while (st.hasMoreTokens()) {
                String token = st.nextToken();
                int idx = token.indexOf(':');
                if (idx < 0) {
                    addresses.add(new ServerAddress(token));
                } else {
                    addresses.add(new ServerAddress(token.substring(0, idx), Integer.valueOf(token.substring(idx+1))));
                }
            }
            return addresses;
        }
        return null;
    }

}
