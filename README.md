# CVCommandGateway for NubCraft

Modern Velocity/Paper replacement for Cubeville's original BungeeCord/CVIPC gateway.
It uses the authenticated local Redis service already used by NubCraft network plugins.

The gateway is deny-by-default: only commands explicitly listed under `routes` exist at
the proxy or may be intercepted by a backend. See `config.example.yml` for the complete
route format and examples.

The `redis.credentials-file` setting can point at NCNetworkStats' existing properties
file, so the Redis password does not need to be copied into another configuration file.
Every destination validates both the command name and its execution mode against its
own route list before doing anything.

Initial routes:

* `/doc <player>` follows the target player to their backend. NCRanks on the doctor's
  current backend owns permission checks and the normal cooldown.
* `/instaday`, `/instanight`, `/instasun`, and `/instastorm` always execute on Survival.
  A remote purchase is charged through Survival's Vault economy provider.

Build with `mvn clean package`. Deploy the Velocity jar to Velocity and the Paper jar to
every backend. Copy the same YAML to each plugin data folder and change `server-name` on
Paper backends (`survival`, `creative`, and so on).
