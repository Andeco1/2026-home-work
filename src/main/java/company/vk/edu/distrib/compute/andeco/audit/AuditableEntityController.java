package company.vk.edu.distrib.compute.andeco.audit;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.andeco.Method;
import company.vk.edu.distrib.compute.andeco.QueryUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.NoSuchElementException;

import static company.vk.edu.distrib.compute.andeco.ServerConfigConstants.*;

public class AuditableEntityController {
    private final Dao<byte[]> dao;
    private final KafkaAuditPublisher auditPublisher;

    public AuditableEntityController(Dao<byte[]> dao, KafkaAuditPublisher auditPublisher) {
        this.dao = dao;
        this.auditPublisher = auditPublisher;
    }

    public void processRequest(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!API_PATH.equals("") && !ENTITY_PATH.equals("") && !(API_PATH + ENTITY_PATH)
                    .equals(exchange.getRequestURI().getPath())) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
                return;
            }
            String id = QueryUtil.extractId(exchange.getRequestURI().getQuery());
            auditPublisher.publish(new AuditEvent(exchange.getRequestMethod(), id, System.currentTimeMillis()));
            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }
            Method method;
            try {
                method = Method.valueOf(exchange.getRequestMethod());
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
                return;
            }
            switch (method) {
                case GET -> processGet(exchange, id);
                case PUT -> processPut(exchange, id);
                case DELETE -> processDelete(exchange, id);
                default -> exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
            }
        }
    }

    private void processGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] data = dao.get(id);
            exchange.getResponseHeaders().set(CONTENT_TYPE_HEADER, OCTET_STREAM);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, data.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
            }
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
        }
    }

    private void processPut(HttpExchange exchange, String id) throws IOException {
        byte[] value = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, value);
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_CREATED, -1);
    }

    private void processDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_ACCEPTED, -1);
    }
}
