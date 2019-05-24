package org.teiid.translator.mongodb;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.process.runtime.Network;

public class EmbeddedMongoDB {
    private static final MongodStarter starter = MongodStarter.getDefaultInstance();
    private MongodExecutable _mongodExe;
    private MongodProcess _mongod;

    public EmbeddedMongoDB() throws Exception {
        _mongodExe = starter.prepare(new MongodConfigBuilder()
        .version(de.flapdoodle.embed.mongo.distribution.Version.Main.PRODUCTION)
        .net(new Net(12345, Network.localhostIsIPv6()))
        .build());
        _mongod = _mongodExe.start();
    }

    public void stop() {
        _mongod.stop();
        _mongodExe.stop();
    }
}
