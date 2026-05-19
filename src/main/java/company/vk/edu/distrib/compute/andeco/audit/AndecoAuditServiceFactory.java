package company.vk.edu.distrib.compute.andeco.audit;

import company.vk.edu.distrib.compute.AuditService;
import company.vk.edu.distrib.compute.AuditServiceFactory;

import java.io.IOException;

public class AndecoAuditServiceFactory extends AuditServiceFactory {
    @Override
    protected AuditService doCreate(String bootstrapServers, String consumerGroupId) throws IOException {
        return new KafkaAuditService(bootstrapServers, consumerGroupId);
    }
}
