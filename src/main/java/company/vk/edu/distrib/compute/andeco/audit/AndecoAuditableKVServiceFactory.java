package company.vk.edu.distrib.compute.andeco.audit;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.andeco.FileDao;

import java.io.IOException;
import java.nio.file.Path;

public class AndecoAuditableKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        Path data = Path.of("..", "data");
        return new AuditableKVServiceImpl(port, new FileDao(data));
    }
}
