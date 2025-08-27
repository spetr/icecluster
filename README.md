# icecluster

Lehký clustered FS v Go: FUSE vrstvička nad lokálním adresářem, replikace přes HTTP mezi peery a distribuované (advisory) zámky. Součástí je jednoduché webové UI a Lua hooky pro reakci na události.

Stav: MVP pro lokální testování.

## Hlavní funkce
- FUSE FS (Linux): proxy nad „backing“ adresářem s atomickými lokálními zápisy a následnou replikací do peerů.
- HTTP peer protokol: PUT/DELETE fan‑out, index, zdraví, verze, statistiky.
- Distribuované zámky: jednoduchý koordinátor, klient s timeout/retry, uvolnění zámků při výpadku peeru (DOWN) podle jeho NodeID.
- Synchronizace: při join (latest/force) a volitelná periodická konsistence s auto‑heal.
- Web UI (SPA) na /ui: přehled peers/locks/stats, mini grafy, podpora API tokenu.
- Lua hooky: spouštění skriptů na události (soubor/adr./zámky/peery/atributy/čtení).
- Bezpečnost: Bearer token pro /v1/*, volitelně TLS.
- Logování: soubor nebo stdout, SIGHUP reopen; volitelný separátní log pro Lua skripty.

## Požadavky
- Go 1.22+ pro build.
- FUSE mount funguje pouze na Linuxu. Na macOS/Windows běží jen HTTP server bez mountu.

## Build
```
go build ./cmd/icecluster
go build ./cmd/icectl
```

## Konfigurace (YAML)
Aplikace čte jediný flag `-config` (výchozí: `/opt/icewarp/icecluster/config.yml`) a z něj načte nastavení.
Kompletní příklad je v `configs/icecluster.example.yaml`. Základní minimum:

```
node_id: "node-1"
bind: ":9000"
backing: "/srv/backing"
mount: "/mnt/ice"      # Linux only
join: "http://host2:9000"  # volitelné

# Bezpečnost
api_token: ""           # volitelné
tls_cert: ""            # volitelné
tls_key: ""             # volitelné

# Lua hooky
hooks_dir: ""           # cesta ke skriptům (pokud prázdné, hooky jsou vypnuté)
hooks_timeout: 2s
hooks_log_file: ""      # volitelný separátní log pro Lua
```

Klíčové volby:
- `node_id`: identita uzlu; používá se jako holder pro zámky a pro uvolnění zámků při výpadku peeru.
- `backing` a `mount`: backing je skutečné úložiště; mount je FUSE mountpoint.
- `join`: volitelný seznam peerů pro počáteční připojení a sync.
- `api_token`: když je nastaven, chrání všechny `/v1/*` endpointy (kromě health/ready). UI si token vyžádá a uloží lokálně.
- `hooks_dir`/`hooks_timeout`/`hooks_log_file`: zapnutí Lua hooků, timeout spuštění a separátní log.

## Spuštění
```
./icecluster -config /cesta/k/config.yml
```
Pro lokální cluster spusťte 2+ uzly s různými `bind`, `node_id`, `backing`, `mount`. Druhý a další uzly nastavte `join` na URL prvního.

## Web UI
- Otevřete `http://<host>:<port>/ui`.
- Pokud je nastaven `api_token`, UI si jej vyžádá a uloží do localStorage.

## CLI (icectl)
```
icectl version
icectl -base http://localhost:9000 peers|health|stats
icectl -base http://localhost:9000 get  -path /file
icectl -base http://localhost:9000 put  -path /file < data
icectl -base http://localhost:9000 del  -path /file
icectl -base http://localhost:9000 lock -path /file -holder <node_id>
icectl -base http://localhost:9000 unlock -path /file -holder <node_id>
```

## Lua hooky
Zapněte nastavením `hooks_dir`. Skripty se hledají jako `<dir>/<event>.lua`, všechny události zachytí `all.lua`. Příklady skriptů jsou v `configs/hooks-example/`.

Podporované události (název -> payload):
- peer_join { peer }
- peer_leave { peer }
- peer_down { peer, error }
- peer_recover { peer }
- file_put { path }
- file_delete { path }
- file_rename { old, new }
- dir_create { path }
- dir_delete { path }
- dir_rename { old, new }
- file_read { path, offset, size }
- dir_list { path }
- stat { path, kind, mode, size, mtime_ns }
- attr_change { path, kind, mode?, size?, atime_ns?, mtime_ns? }
- owner_change { path, kind, uid?, gid? }

Pozn.: `kind` je "file" nebo "dir". Hooky běží s timeoutem `hooks_timeout`. Pokud je `hooks_log_file` nastaven, logy z Lua jdou do tohoto souboru, jinak do hlavního logu.

## Zámky
- Advisory zámky po cestách; koordinátor je první peer (podle seřazeného seznamu).
- Držení zámků je vidět na `/v1/locks`.
- Při výpadku peeru (DOWN) se na ostatních uzlech automaticky uvolní všechny zámky držené jeho `NodeID`.

## Logování
- `log_file`: soubor (nebo prázdné = stdout). Na SIGHUP proběhne reopen.
- `log_verbose`: zapne podrobnější logy.
- `hooks_log_file`: (volitelné) oddělený log pro Lua hooky.

## Bezpečnost
- `api_token`: chrání `/v1/*` (Bearer token). `/health` a `/ready` zůstávají otevřené.
- `tls_cert` + `tls_key`: zapne HTTPS.

## API – tipy
- `/v1/version` vrací string verze.
- `/v1/node` vrací `{ id }` – identitu uzlu (používá se k uvolnění zámků při výpadku).
- `/v1/locks/release_by_holder?holder=<id>` administrativně uvolní všechny zámky daného holdera.

## Omezení
- FUSE mount jen Linux. Na jiných OS běží pouze HTTP část.
- Konflikty řešeny jednoduše (mtime/size); není deduplikace, žádné transakční multi‑file operace.
- MVP – vhodné pro PoC a nenáročné scénáře.

---
Build/release workflow v `.github/workflows/`; verze je dostupná i na `/v1/version` a v logu při startu.
