package org.nubcraft.cvcommandgateway.paper;

import com.google.gson.Gson;
import org.bukkit.Bukkit;
import org.bukkit.attribute.Attribute;
import org.bukkit.attribute.AttributeInstance;
import org.bukkit.command.ConsoleCommandSender;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerCommandPreprocessEvent;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.java.JavaPlugin;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.lang.reflect.Method;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class CVCommandGatewayPaper extends JavaPlugin implements Listener {
    private final Gson gson = new Gson();
    private final AtomicBoolean running = new AtomicBoolean();
    private final Map<String, PlayerLocation> networkPlayers = new ConcurrentHashMap<>();
    private final Map<String, BackendRoute> commandRoutes = new HashMap<>();
    private final Map<String, Set<String>> inboundExecutions = new HashMap<>();
    private final Map<String, PendingDoctor> pendingDoctors = new ConcurrentHashMap<>();
    private RedisSettings redis;
    private String serverName;
    private Set<String> allowedExecutions;
    private Thread requestThread;
    private Thread responseThread;
    private JedisPubSub requestSubscriber;
    private JedisPubSub responseSubscriber;

    @Override
    public void onEnable() {
        saveDefaultConfig();
        reloadSettings();
        getServer().getPluginManager().registerEvents(this, this);
        running.set(true);
        startRequestSubscriber();
        startResponseSubscriber();
        getServer().getScheduler().runTaskTimerAsynchronously(this, this::refreshPlayerLocations, 1L, 20L);
        getLogger().info("Listening as backend '" + serverName + "' with " + commandRoutes.size() + " routed command alias(es).");
    }

    @Override
    public void onDisable() {
        running.set(false);
        if (requestSubscriber != null) requestSubscriber.unsubscribe();
        if (responseSubscriber != null) responseSubscriber.unsubscribe();
        if (requestThread != null) requestThread.interrupt();
        if (responseThread != null) responseThread.interrupt();
    }

    private void reloadSettings() {
        serverName = getConfig().getString("server-name", "").trim().toLowerCase(Locale.ROOT);
        if (serverName.isBlank() || serverName.equals("proxy")) throw new IllegalStateException("Set server-name to this Velocity backend name");
        redis = redisSettings();
        allowedExecutions = new HashSet<>();
        getConfig().getStringList("allowed-executions").forEach(v -> allowedExecutions.add(v.toLowerCase(Locale.ROOT)));
        commandRoutes.clear();
        inboundExecutions.clear();
        var routes = getConfig().getConfigurationSection("routes");
        if (routes == null) return;
        for (String name : routes.getKeys(false)) {
            String base = "routes." + name + ".";
            String trigger = getConfig().getString(base + "trigger", "backend").toLowerCase(Locale.ROOT);
            String destination = getConfig().getString(base + "destination", "");
            List<String> aliases = getConfig().getStringList(base + "commands");
            if (trigger.equals("proxy") && destination.equalsIgnoreCase(serverName)) {
                String localExecution = getConfig().getString(base + "local-execution", "player").toLowerCase(Locale.ROOT);
                String remoteExecution = getConfig().getString(base + "remote-execution", "console").toLowerCase(Locale.ROOT);
                aliases.forEach(alias -> inboundExecutions.computeIfAbsent(alias.toLowerCase(Locale.ROOT), ignored -> new HashSet<>())
                        .addAll(Set.of(localExecution, remoteExecution)));
                continue;
            }
            if (!trigger.equals("backend")) continue;
            BackendRoute route = new BackendRoute(name,
                    destination,
                    getConfig().getInt(base + "target-argument", 0),
                    lower(getConfig().getStringList(base + "local-values")),
                    getConfig().getString(base + "permission", ""),
                    getConfig().getString(base + "execution", "console").toLowerCase(Locale.ROOT));
            for (String alias : aliases) {
                String lowered = alias.toLowerCase(Locale.ROOT);
                if (commandRoutes.put(lowered, route) != null) throw new IllegalStateException("Backend command alias is configured more than once: " + alias);
                inboundExecutions.computeIfAbsent(lowered, ignored -> new HashSet<>()).add(route.execution);
            }
        }
    }

    @EventHandler(priority = EventPriority.LOWEST, ignoreCancelled = true)
    public void onPlayerCommand(PlayerCommandPreprocessEvent event) {
        String raw = event.getMessage().substring(1).trim();
        if (raw.isBlank()) return;
        String[] parts = raw.split("\\s+");
        BackendRoute route = commandRoutes.get(parts[0].toLowerCase(Locale.ROOT));
        if (route == null || !route.destination.equalsIgnoreCase("target-player")) return;
        int partIndex = route.targetArgument + 1;
        if (parts.length <= partIndex) return;
        String targetName = parts[partIndex];
        if (route.localValues.contains(targetName.toLowerCase(Locale.ROOT))) return;
        PlayerLocation target = networkPlayers.get(targetName.toLowerCase(Locale.ROOT));
        if (target == null || target.server.equalsIgnoreCase(serverName)) return;

        event.setCancelled(true);
        Player actor = event.getPlayer();
        if (!route.permission.isBlank() && !actor.hasPermission(route.permission)) {
            actor.sendMessage("§cYou do not have permission to use this command.");
            return;
        }
        if (route.execution.equals("ncranks-doctor")) {
            String error = beginNcranksDoctor(actor, target.name);
            if (error != null) {
                actor.sendMessage(error);
                return;
            }
        }
        Request request = new Request(UUID.randomUUID().toString(), actor.getUniqueId().toString(), actor.getName(),
                serverName, target.server, route.execution, raw, target.name, System.currentTimeMillis());
        if (route.execution.equals("ncranks-doctor")) {
            pendingDoctors.put(request.id, new PendingDoctor(actor.getUniqueId(), target.name));
            getServer().getScheduler().runTaskLater(this, () -> completeDoctor(request.id, false,
                    "§cThe cross-server doctor request timed out."), 200L);
        }
        getServer().getScheduler().runTaskAsynchronously(this, () -> {
            try (Jedis jedis = openRedis()) {
                long receivers = jedis.publish(requestChannel(target.server), gson.toJson(request));
                if (receivers == 0) getServer().getScheduler().runTask(this, () -> completeDoctor(request.id, false,
                        "§c" + target.server + " is not available."));
            } catch (Exception exception) {
                getLogger().warning("Could not publish remote command: " + exception.getMessage());
                getServer().getScheduler().runTask(this, () -> completeDoctor(request.id, false,
                        "§cThe command gateway is temporarily unavailable."));
            }
        });
    }

    private String beginNcranksDoctor(Player actor, String targetName) {
        try {
            Plugin plugin = getServer().getPluginManager().getPlugin("NCRanks");
            if (plugin == null || !plugin.isEnabled()) return "§cNCRanks is unavailable on this server.";
            Method method = plugin.getClass().getMethod("beginGatewayDoctor", Player.class, String.class);
            return (String) method.invoke(plugin, actor, targetName);
        } catch (Exception exception) {
            getLogger().warning("NCRanks gateway preflight failed: " + exception.getMessage());
            return "§cDoctor cross-server support is unavailable.";
        }
    }

    private void finishNcranksDoctor(UUID actorId, String targetName, boolean success, String message) {
        Plugin plugin = getServer().getPluginManager().getPlugin("NCRanks");
        if (plugin == null || !plugin.isEnabled()) return;
        try {
            Method method = plugin.getClass().getMethod("finishGatewayDoctor", UUID.class, String.class, boolean.class, String.class);
            method.invoke(plugin, actorId, targetName, success, message);
        } catch (Exception exception) {
            getLogger().warning("NCRanks gateway completion failed: " + exception.getMessage());
        }
    }

    private void completeDoctor(String requestId, boolean success, String message) {
        PendingDoctor pending = pendingDoctors.remove(requestId);
        if (pending != null) finishNcranksDoctor(pending.actorId, pending.targetName, success, message);
    }

    private void refreshPlayerLocations() {
        try (Jedis jedis = openRedis()) {
            Map<String, String> values = jedis.hgetAll(playersKey());
            Map<String, PlayerLocation> fresh = new HashMap<>();
            for (Map.Entry<String, String> entry : values.entrySet()) {
                PlayerLocation location = gson.fromJson(entry.getValue(), PlayerLocation.class);
                if (location != null && location.name != null) fresh.put(entry.getKey().toLowerCase(Locale.ROOT), location);
            }
            networkPlayers.clear();
            networkPlayers.putAll(fresh);
        } catch (Exception exception) {
            getLogger().warning("Could not refresh network player locations: " + exception.getMessage());
        }
    }

    private void startRequestSubscriber() {
        requestThread = Thread.ofPlatform().name("CVCommandGateway-request-subscriber").daemon(true).start(() -> subscriberLoop(true));
    }

    private void startResponseSubscriber() {
        responseThread = Thread.ofPlatform().name("CVCommandGateway-backend-response-subscriber").daemon(true).start(() -> subscriberLoop(false));
    }

    private void subscriberLoop(boolean requests) {
        while (running.get()) {
            try (Jedis jedis = openRedis()) {
                JedisPubSub subscriber = new JedisPubSub() {
                    @Override public void onMessage(String channel, String message) {
                        if (requests) {
                            Request request = gson.fromJson(message, Request.class);
                            if (request != null) getServer().getScheduler().runTask(CVCommandGatewayPaper.this, () -> processRequest(request));
                        } else {
                            Response response = gson.fromJson(message, Response.class);
                            if (response != null && response.delivery.equals("backend") && response.sourceServer.equalsIgnoreCase(serverName)) {
                                getServer().getScheduler().runTask(CVCommandGatewayPaper.this, () ->
                                        completeDoctor(response.id, response.success, response.message));
                            }
                        }
                    }
                };
                if (requests) {
                    requestSubscriber = subscriber;
                    jedis.subscribe(subscriber, requestChannel(serverName));
                } else {
                    responseSubscriber = subscriber;
                    jedis.subscribe(subscriber, responseChannel());
                }
            } catch (Exception exception) {
                if (running.get()) {
                    getLogger().warning("Redis subscriber disconnected: " + exception.getMessage());
                    try { Thread.sleep(2000L); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                }
            }
        }
    }

    private void processRequest(Request request) {
        if (!request.targetServer.equalsIgnoreCase(serverName)) return;
        if (Math.abs(System.currentTimeMillis() - request.createdAt) > 30_000L) return;
        String execution = request.execution.toLowerCase(Locale.ROOT);
        String command = request.command.strip().split("\\s+", 2)[0].toLowerCase(Locale.ROOT);
        Set<String> permittedForCommand = inboundExecutions.getOrDefault(command, Set.of());
        if (!allowedExecutions.contains(execution) || !permittedForCommand.contains(execution)) {
            respond(request, false, "Execution type is not allowed on " + serverName + ".", request.execution.equals("ncranks-doctor") ? "backend" : "proxy");
            return;
        }
        switch (execution) {
            case "player" -> executeAsPlayer(request);
            case "console" -> executeAsConsole(request);
            case "ncranks-doctor" -> executeDoctor(request);
            case "cvweathertimecontrol" -> executeWeather(request);
            default -> respond(request, false, "Unknown execution type.", "proxy");
        }
    }

    private void executeAsPlayer(Request request) {
        Player actor = Bukkit.getPlayer(UUID.fromString(request.actorUuid));
        if (actor == null || !actor.isOnline()) {
            respond(request, false, "You are no longer connected to " + serverName + ".", "proxy");
            return;
        }
        Bukkit.dispatchCommand(actor, request.command);
    }

    private void executeAsConsole(Request request) {
        ConsoleCommandSender console = Bukkit.getConsoleSender();
        boolean accepted = Bukkit.dispatchCommand(console, request.command);
        respond(request, accepted, accepted ? "Command sent to " + serverName + "." : "The command was rejected by " + serverName + ".", "proxy");
    }

    private void executeDoctor(Request request) {
        Player target = Bukkit.getPlayerExact(request.targetName);
        if (target == null || !target.isOnline()) {
            respond(request, false, "§cPlayer §6" + request.targetName + " §cis no longer online.", "backend");
            return;
        }
        if (target.isDead()) {
            respond(request, false, "§cToo late — " + target.getName() + " is already dead.", "backend");
            return;
        }
        if (!needsHealing(target)) {
            respond(request, false, "§6" + target.getName() + " §cdoes not need to be healed.", "backend");
            return;
        }
        AttributeInstance max = target.getAttribute(Attribute.MAX_HEALTH);
        if (max != null && target.getHealth() < max.getValue()) target.setHealth(max.getValue());
        if (target.getFoodLevel() < 20) { target.setFoodLevel(20); target.setSaturation(5.0F); target.setExhaustion(0.0F); }
        if (target.getFireTicks() > 0) target.setFireTicks(0);
        if (target.getRemainingAir() < target.getMaximumAir()) target.setRemainingAir(target.getMaximumAir());
        target.sendMessage("§aYou have been healed by §b" + request.actorName + "§a from another server.");
        respond(request, true, "§aYou have healed §6" + target.getName() + " §aon §b" + serverName + "§a.", "backend");
    }

    private boolean needsHealing(Player player) {
        AttributeInstance max = player.getAttribute(Attribute.MAX_HEALTH);
        return (max != null && player.getHealth() < max.getValue()) || player.getFoodLevel() < 20
                || player.getFireTicks() > 0 || player.getRemainingAir() < player.getMaximumAir();
    }

    private void executeWeather(Request request) {
        Plugin plugin = getServer().getPluginManager().getPlugin("CVWeatherTimeControl");
        if (plugin == null || !plugin.isEnabled()) {
            respond(request, false, "CVWeatherTimeControl is unavailable on " + serverName + ".", "proxy");
            return;
        }
        try {
            Method method = plugin.getClass().getMethod("executeGatewayPurchase", UUID.class, String.class, String.class);
            Object result = method.invoke(plugin, UUID.fromString(request.actorUuid), request.actorName, request.command.split("\\s+", 2)[0]);
            Method successMethod = result.getClass().getMethod("success");
            Method messageMethod = result.getClass().getMethod("message");
            respond(request, (boolean) successMethod.invoke(result), String.valueOf(messageMethod.invoke(result)), "proxy");
        } catch (Exception exception) {
            getLogger().warning("Weather gateway execution failed: " + exception.getMessage());
            respond(request, false, "The weather purchase could not be completed.", "proxy");
        }
    }

    private void respond(Request request, boolean success, String message, String delivery) {
        Response response = new Response(request.id, request.actorUuid, request.sourceServer, request.targetName, delivery, success, message);
        getServer().getScheduler().runTaskAsynchronously(this, () -> {
            try (Jedis jedis = openRedis()) { jedis.publish(responseChannel(), gson.toJson(response)); }
            catch (Exception exception) { getLogger().warning("Could not publish gateway response: " + exception.getMessage()); }
        });
    }

    private Jedis openRedis() {
        Jedis jedis = new Jedis(redis.host, redis.port);
        if (!redis.password.isBlank()) {
            if (redis.username.isBlank()) jedis.auth(redis.password); else jedis.auth(redis.username, redis.password);
        }
        return jedis;
    }

    private RedisSettings redisSettings() {
        String host = getConfig().getString("redis.host", "127.0.0.1");
        int port = getConfig().getInt("redis.port", 6379);
        String username = getConfig().getString("redis.username", "");
        String password = getConfig().getString("redis.password", "");
        String credentialsFile = getConfig().getString("redis.credentials-file", "").trim();
        if (!credentialsFile.isBlank()) {
            Properties properties = new Properties();
            try (FileInputStream input = new FileInputStream(new File(credentialsFile))) {
                properties.load(input);
            } catch (IOException exception) {
                throw new IllegalStateException("Could not read Redis credentials-file " + credentialsFile, exception);
            }
            host = properties.getProperty("redis.host", host).trim();
            port = Integer.parseInt(properties.getProperty("redis.port", String.valueOf(port)).trim());
            username = properties.getProperty("redis.username", username).trim();
            password = properties.getProperty("redis.password", password).trim();
        }
        return new RedisSettings(host, port, username, password,
                getConfig().getString("redis.namespace", "nubcraft:commandgateway"));
    }

    private String requestChannel(String server) { return redis.namespace + ":requests:" + server.toLowerCase(Locale.ROOT); }
    private String responseChannel() { return redis.namespace + ":responses"; }
    private String playersKey() { return redis.namespace + ":players"; }
    private static Set<String> lower(List<String> values) { Set<String> result = new HashSet<>(); values.forEach(v -> result.add(v.toLowerCase(Locale.ROOT))); return result; }

    private record RedisSettings(String host, int port, String username, String password, String namespace) {}
    private record BackendRoute(String name, String destination, int targetArgument, Set<String> localValues, String permission, String execution) {}
    private record PendingDoctor(UUID actorId, String targetName) {}
    private record PlayerLocation(String uuid, String name, String server) {}
    private record Request(String id, String actorUuid, String actorName, String sourceServer, String targetServer, String execution, String command, String targetName, long createdAt) {}
    private record Response(String id, String actorUuid, String sourceServer, String targetName, String delivery, boolean success, String message) {}
}
