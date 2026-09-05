package org.nubcraft.cvcommandgateway.velocity;

import com.google.gson.Gson;
import com.google.inject.Inject;
import com.velocitypowered.api.command.CommandSource;
import com.velocitypowered.api.command.SimpleCommand;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.connection.DisconnectEvent;
import com.velocitypowered.api.event.player.ServerPostConnectEvent;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ProxyServer;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.serializer.legacy.LegacyComponentSerializer;
import org.slf4j.Logger;
import org.yaml.snakeyaml.Yaml;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

@Plugin(id = "cvcommandgateway", name = "CVCommandGateway", version = "2.0.0-SNAPSHOT",
        authors = {"Greg Elsbernd"})
public final class CVCommandGatewayVelocity {
    private final ProxyServer proxy;
    private final Logger logger;
    private final Path dataDirectory;
    private final Gson gson = new Gson();
    private final AtomicBoolean running = new AtomicBoolean();
    private final List<Route> routes = new ArrayList<>();
    private RedisSettings redis;
    private Thread responseThread;
    private JedisPubSub responseSubscriber;

    @Inject
    public CVCommandGatewayVelocity(ProxyServer proxy, Logger logger, @DataDirectory Path dataDirectory) {
        this.proxy = proxy;
        this.logger = logger;
        this.dataDirectory = dataDirectory;
    }

    @Subscribe
    public void onInitialize(ProxyInitializeEvent event) {
        try {
            loadConfig();
            registerProxyRoutes();
            running.set(true);
            startResponseSubscriber();
            try (Jedis jedis = openRedis()) {
                jedis.del(playersKey());
            }
            proxy.getAllPlayers().forEach(this::publishPlayerLocation);
            logger.info("CVCommandGateway loaded {} proxy route(s).", routes.stream().filter(r -> r.trigger.equals("proxy")).count());
        } catch (Exception exception) {
            logger.error("CVCommandGateway could not start", exception);
        }
    }

    @Subscribe
    public void onServerPostConnect(ServerPostConnectEvent event) {
        publishPlayerLocation(event.getPlayer());
    }

    @Subscribe
    public void onDisconnect(DisconnectEvent event) {
        Player player = event.getPlayer();
        try (Jedis jedis = openRedis()) {
            jedis.hdel(playersKey(), player.getUsername().toLowerCase(Locale.ROOT), player.getUniqueId().toString());
        } catch (Exception exception) {
            logger.warn("Could not remove player location for {}: {}", player.getUsername(), exception.getMessage());
        }
    }

    @Subscribe
    public void onShutdown(ProxyShutdownEvent event) {
        running.set(false);
        if (responseSubscriber != null) responseSubscriber.unsubscribe();
        if (responseThread != null) responseThread.interrupt();
    }

    @SuppressWarnings("unchecked")
    private void loadConfig() throws IOException {
        Files.createDirectories(dataDirectory);
        Path configPath = dataDirectory.resolve("config.yml");
        if (!Files.exists(configPath)) {
            try (InputStream input = getClass().getResourceAsStream("/config.yml")) {
                if (input == null) throw new IOException("Bundled config.yml is missing");
                Files.copy(input, configPath);
            }
        }
        Map<String, Object> root;
        try (InputStream input = Files.newInputStream(configPath)) {
            root = new Yaml().load(input);
        }
        Map<String, Object> redisMap = map(root.get("redis"));
        redis = redisSettings(redisMap);
        routes.clear();
        for (Map.Entry<String, Object> entry : map(root.get("routes")).entrySet()) {
            Map<String, Object> value = map(entry.getValue());
            routes.add(new Route(entry.getKey(), strings(value.get("commands")), string(value, "trigger", "backend"),
                    string(value, "destination", ""), string(value, "permission", ""),
                    string(value, "local-execution", "player"), string(value, "remote-execution", "console")));
        }
    }

    private void registerProxyRoutes() {
        Set<String> registered = new HashSet<>();
        for (Route route : routes) {
            if (!route.trigger.equals("proxy") || route.commands.isEmpty()) continue;
            for (String alias : route.commands) {
                if (!registered.add(alias)) throw new IllegalStateException("Proxy command alias is configured more than once: " + alias);
            }
            String primary = route.commands.getFirst();
            String[] aliases = route.commands.stream().skip(1).toArray(String[]::new);
            var meta = proxy.getCommandManager().metaBuilder(primary).aliases(aliases).plugin(this).build();
            proxy.getCommandManager().register(meta, new RoutedCommand(route, primary));
        }
    }

    private final class RoutedCommand implements SimpleCommand {
        private final Route route;
        private final String command;

        private RoutedCommand(Route route, String command) {
            this.route = route;
            this.command = command;
        }

        @Override
        public void execute(Invocation invocation) {
            CommandSource source = invocation.source();
            if (!(source instanceof Player player)) {
                source.sendMessage(Component.text("This command can only be used by a player."));
                return;
            }
            if (!route.permission.isBlank() && !player.hasPermission(route.permission)) {
                player.sendMessage(Component.text("You do not have permission to use this command."));
                return;
            }
            String sourceServer = player.getCurrentServer().map(c -> c.getServerInfo().getName()).orElse("");
            if (sourceServer.isBlank()) {
                player.sendMessage(Component.text("Your current server could not be determined."));
                return;
            }
            String targetServer = route.destination;
            if (proxy.getServer(targetServer).isEmpty()) {
                player.sendMessage(Component.text("The destination server " + targetServer + " is not registered."));
                return;
            }
            String execution = sourceServer.equalsIgnoreCase(targetServer) ? route.localExecution : route.remoteExecution;
            String line = command + (invocation.arguments().length == 0 ? "" : " " + String.join(" ", invocation.arguments()));
            Request request = new Request(UUID.randomUUID().toString(), player.getUniqueId().toString(), player.getUsername(),
                    sourceServer, targetServer, execution, line, "", System.currentTimeMillis());
            try (Jedis jedis = openRedis()) {
                long receivers = jedis.publish(requestChannel(targetServer), gson.toJson(request));
                if (receivers == 0) player.sendMessage(Component.text("The destination server " + targetServer + " is not available."));
            } catch (Exception exception) {
                logger.error("Could not route /{} to {}", command, targetServer, exception);
                player.sendMessage(Component.text("The command gateway is temporarily unavailable."));
            }
        }

        @Override
        public boolean hasPermission(Invocation invocation) {
            return route.permission.isBlank() || invocation.source().hasPermission(route.permission);
        }

        @Override
        public List<String> suggest(Invocation invocation) {
            return Collections.emptyList();
        }
    }

    private void publishPlayerLocation(Player player) {
        Optional<String> server = player.getCurrentServer().map(c -> c.getServerInfo().getName());
        if (server.isEmpty()) return;
        PlayerLocation location = new PlayerLocation(player.getUniqueId().toString(), player.getUsername(), server.get());
        try (Jedis jedis = openRedis()) {
            String json = gson.toJson(location);
            jedis.hset(playersKey(), player.getUsername().toLowerCase(Locale.ROOT), json);
            jedis.hset(playersKey(), player.getUniqueId().toString(), json);
        } catch (Exception exception) {
            logger.warn("Could not publish player location for {}: {}", player.getUsername(), exception.getMessage());
        }
    }

    private void startResponseSubscriber() {
        responseThread = Thread.ofPlatform().name("CVCommandGateway-response-subscriber").daemon(true).start(() -> {
            while (running.get()) {
                try (Jedis jedis = openRedis()) {
                    responseSubscriber = new JedisPubSub() {
                        @Override public void onMessage(String channel, String message) {
                            Response response = gson.fromJson(message, Response.class);
                            if (response == null || response.actorUuid == null || response.message == null || response.message.isBlank()) return;
                            proxy.getScheduler().buildTask(CVCommandGatewayVelocity.this, () ->
                                    proxy.getPlayer(UUID.fromString(response.actorUuid)).ifPresent(p -> p.sendMessage(
                                            LegacyComponentSerializer.legacySection().deserialize(response.message)))).schedule();
                        }
                    };
                    jedis.subscribe(responseSubscriber, responseChannel());
                } catch (Exception exception) {
                    if (running.get()) {
                        logger.warn("Redis response subscriber disconnected: {}", exception.getMessage());
                        try { Thread.sleep(2000L); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                    }
                }
            }
        });
    }

    private Jedis openRedis() {
        Jedis jedis = new Jedis(redis.host, redis.port);
        if (!redis.password.isBlank()) {
            if (redis.username.isBlank()) jedis.auth(redis.password); else jedis.auth(redis.username, redis.password);
        }
        return jedis;
    }

    private RedisSettings redisSettings(Map<String, Object> redisMap) throws IOException {
        String host = string(redisMap, "host", "127.0.0.1");
        int port = integer(redisMap, "port", 6379);
        String username = string(redisMap, "username", "");
        String password = string(redisMap, "password", "");
        String credentialsFile = string(redisMap, "credentials-file", "");
        if (!credentialsFile.isBlank()) {
            Properties properties = new Properties();
            try (InputStream input = Files.newInputStream(Path.of(credentialsFile))) {
                properties.load(input);
            }
            host = properties.getProperty("redis.host", host).trim();
            port = Integer.parseInt(properties.getProperty("redis.port", String.valueOf(port)).trim());
            username = properties.getProperty("redis.username", username).trim();
            password = properties.getProperty("redis.password", password).trim();
        }
        return new RedisSettings(host, port, username, password,
                string(redisMap, "namespace", "nubcraft:commandgateway"));
    }

    private String requestChannel(String server) { return redis.namespace + ":requests:" + server.toLowerCase(Locale.ROOT); }
    private String responseChannel() { return redis.namespace + ":responses"; }
    private String playersKey() { return redis.namespace + ":players"; }

    @SuppressWarnings("unchecked") private static Map<String, Object> map(Object value) { return value instanceof Map<?, ?> m ? (Map<String, Object>) m : new LinkedHashMap<>(); }
    private static String string(Map<String, Object> map, String key, String fallback) { Object v = map.get(key); return v == null ? fallback : String.valueOf(v).trim(); }
    private static int integer(Map<String, Object> map, String key, int fallback) { Object v = map.get(key); return v instanceof Number n ? n.intValue() : fallback; }
    private static List<String> strings(Object value) { if (!(value instanceof List<?> list)) return List.of(); return list.stream().map(String::valueOf).map(s -> s.toLowerCase(Locale.ROOT)).toList(); }

    private record RedisSettings(String host, int port, String username, String password, String namespace) {}
    private record Route(String name, List<String> commands, String trigger, String destination, String permission, String localExecution, String remoteExecution) {}
    private record PlayerLocation(String uuid, String name, String server) {}
    private record Request(String id, String actorUuid, String actorName, String sourceServer, String targetServer, String execution, String command, String targetName, long createdAt) {}
    private record Response(String id, String actorUuid, String sourceServer, String delivery, boolean success, String message) {}
}
