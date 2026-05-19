package company.vk.edu.distrib.compute.andeco.audit;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.andeco.ServerConfigConstants;
import company.vk.edu.distrib.compute.andeco.controller.StatusController;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class AuditableKVServiceImpl implements AuditableKVService {
    private final HttpServer server;
    private final KafkaAuditPublisher auditPublisher;

    public AuditableKVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        this.auditPublisher = new KafkaAuditPublisher();
        AuditableEntityController entityCtrl = new AuditableEntityController(dao, auditPublisher);
        StatusController statusCtrl = new StatusController();
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext(ServerConfigConstants.API_PATH + ServerConfigConstants.ENTITY_PATH,
                entityCtrl::processRequest);
        server.createContext(ServerConfigConstants.API_PATH + ServerConfigConstants.STATUS_PATH,
                statusCtrl::processRequest);
        server.setExecutor(Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors()));
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        auditPublisher.setBootstrapServers(bootstrapServers);
    }

    @Override
    public void setAsync(boolean enabled) {
        auditPublisher.setAsync(enabled);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        auditPublisher.close();
    }
}
