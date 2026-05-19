package company.vk.edu.distrib.compute.andeco.audit;

import company.vk.edu.distrib.compute.AuditEvent;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

final class AuditEventCoderUtils {
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    private AuditEventCoderUtils() {
    }

    static String encode(AuditEvent event) {
        String idEnc = (event.id() == null) ? "" :
                ENCODER.encodeToString(event.id().getBytes(StandardCharsets.UTF_8));
        return event.timestamp() + "\t" + event.method() + "\t" + idEnc;
    }

    static AuditEvent decode(String line) {
        String[] parts = line.split("\t", 3);
        int partsCount = 3;
        if (parts.length != partsCount) {
            throw new IllegalArgumentException("Invalid audit record: " + line);
        }
        long timestamp = Long.parseLong(parts[0]);
        String method = parts[1];
        String id = parts[2].isEmpty() ? null :
            new String(DECODER.decode(parts[2]), StandardCharsets.UTF_8);
        return new AuditEvent(method, id, timestamp);
    }
}
