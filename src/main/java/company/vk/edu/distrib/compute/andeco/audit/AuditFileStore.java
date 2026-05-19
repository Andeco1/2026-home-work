package company.vk.edu.distrib.compute.andeco.audit;

import company.vk.edu.distrib.compute.AuditEvent;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

final class AuditFileStore {
    private final Path file;
    private final ReentrantLock lock = new ReentrantLock();
    private final List<AuditEvent> cache = new ArrayList<>();

    AuditFileStore(Path file) throws IOException {
        this.file = file;
        Files.createDirectories(file.getParent());
        if (Files.exists(file)) {
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) {
                    cache.add(AuditEventCoderUtils.decode(line));
                }
            }
        } else {
            Files.createFile(file);
        }
    }

    void append(AuditEvent event) throws IOException {
        lock.lock();
        try {
            Files.writeString(
                    file,
                    AuditEventCoderUtils.encode(event) + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
            cache.add(event);
        } finally {
            lock.unlock();
        }
    }

    List<AuditEvent> snapshot() {
        lock.lock();
        try {
            return List.copyOf(cache);
        } finally {
            lock.unlock();
        }
    }
}
