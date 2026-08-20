# lsmkv

`lsmkv` je durable, Dynamo-inspired distributed key-value store implementiran u Go-u.

Projekat ima dva sloja:

1. **LSM storage engine** — lokalni, crash-safe storage sa WAL-om, memtable-om, SSTable fajlovima, manifestom, background flush-om i compaction-om.
2. **Distribution layer** — višestruki node-ovi povezani preko gRPC-a, consistent hashing ring, replication i configurable quorum `N/R/W` operacije.

Trenutno je implementiran i testiran funkcionalni distributed MVP za 3-node cluster:

- `ReplicationFactor = 3`
- `WriteQuorum = 2`
- `ReadQuorum = 2`
- coordinator forwarding
- quorum `Put`, `Get` i `Delete`
- tolerancija jednog nedostupnog noda dok quorum ostaje dostupan

Projekat je inspirisan Dynamo-style arhitekturom, ali nije pokušaj potpune produkcijske implementacije Dynamo/Cassandra sistema. Napredne funkcionalnosti kao što su hinted handoff, vector clocks, anti-entropy repair i dynamic cluster membership su svesno odložene.

---

## Features

### Local LSM Storage Engine

- `Put`, `Get` i `Delete` operacije nad byte-string ključevima i vrednostima
- Overwrite semantika
- Tombstone zapisi za lokalni `Delete`
- Write-Ahead Log pre memtable upisa
- Konfigurisani WAL `fsync` durability režim
- WAL segmentacija, checksum validacija i replay pri restart-u
- Oporavak od truncated/torn WAL tail-a
- Memtable rotacija i immutable memtables
- Background scheduler za flush i compaction
- SSTable writer i reader
- Bloom filter za optimizaciju SSTable lookup-a
- Manifest kao authoritative source za live SSTable fajlove
- Immutable published `Version` view za SSTable state
- Size-tiered compaction
- Force flush, graceful close i fast close putanje
- Zaštita od orphan SSTable fajlova
- Crash zaštita između SSTable rename-a i manifest publish-a
- Hard-crash subprocess testovi za acknowledged `Put` i `Delete`
- Defensive corruption validation za WAL, manifest, SSTable index i Bloom filter metadata

### Distributed Quorum Layer

- gRPC API za client-facing i internal node-to-node zahteve
- Statička seed-list konfiguracija cluster-a
- Consistent hashing ring sa virtual nodes
- Deterministički izbor coordinator noda za svaki ključ
- Preference list za izbor replica seta
- Request forwarding: client može poslati zahtev bilo kom nodu
- Configurable `ReplicationFactor`, `ReadQuorum` i `WriteQuorum`
- Parallel replication prema preference list-i
- Quorum `Put`: uspeh čim `W` replika potvrdi write
- Quorum `Get`: uspeh čim `R` replika odgovori
- Quorum `Delete`: uspeh čim `W` replika potvrdi delete
- Early-return behavior: coordinator ne čeka nedostupnu/sporu repliku kada je quorum već postignut
- Integration testovi sa tri izolovana noda, odvojenim storage direktorijumima i stvarnim gRPC pozivima
- Testirani read/write/delete scenariji sa jednom nedostupnom replikom

---

## Current Distributed Model

Podrazumevana demonstraciona konfiguracija koristi:

```text
N = 3  -> ReplicationFactor
R = 2  -> ReadQuorum
W = 2  -> WriteQuorum
```

Sa `N = 3`, `R = 2` i `W = 2` važi:

```text
R + W > N
2 + 2 > 3
```

Read i write quorum se zato preklapaju u najmanje jednoj replici.

Praktično:

- `Put` uspeva kada dve replike potvrde upis.
- `Get` uspeva kada dve replike odgovore.
- `Delete` uspeva kada dve replike potvrde brisanje.
- Jedan node može biti nedostupan, a osnovne operacije i dalje mogu uspeti ako su preostale dve potrebne replike dostupne.

---

## Architecture

```text
                         Client
                           |
                           v
                 Any cluster node via gRPC
                           |
                           v
              Is this node the coordinator?
                    |                 |
                  yes                 no
                    |                 |
                    v                 v
          Execute quorum flow    Forward request
                    |
                    v
       Consistent hash ring / preference list
                    |
                    v
      Replica 1       Replica 2       Replica 3
          |               |               |
          v               v               v
    Local LSM Store  Local LSM Store  Local LSM Store
```

Svaki node sadrži jednu instancu lokalnog LSM storage engine-a i jednu instancu distribution layer-a.

Distribution layer koristi storage layer samo kroz osnovne operacije:

```text
Get(key)
Put(key, value)
Delete(key)
```

LSM engine nema znanje o cluster membership-u, consistent hashing-u, replikaciji ili quorum pravilima.

---

## Coordinator and Preference List

Za svaki ključ:

1. Ključ se hash-uje na consistent-hashing ring.
2. Prvi node u smeru kazaljke na satu postaje coordinator.
3. Coordinator i narednih `N - 1` nodova čine preference list.
4. Client request može doći na bilo koji node.
5. Ako primljeni node nije coordinator, zahtev se prosleđuje coordinator-u.
6. Coordinator kontaktira replike paralelno i vraća rezultat čim se dostigne potrebni quorum.

```text
key
  -> hash(key)
  -> coordinator
  -> preference list of N replicas
  -> parallel quorum requests
  -> return when R or W acknowledgements are collected
```

---

## Distributed Operation Flows

### Quorum Put

```text
Client Put(key, value)
  -> Any Node
  -> Forward to coordinator if necessary
  -> Coordinator resolves preference list
  -> Coordinator sends write to replicas in parallel
  -> Each replica writes to its local LSM store
  -> Return success as soon as W acknowledgements arrive
```

Za `N = 3, W = 2`, write ne čeka treću repliku ako su dve replike već uspešno potvrdile operaciju.

### Quorum Get

```text
Client Get(key)
  -> Any Node
  -> Forward to coordinator if necessary
  -> Coordinator resolves preference list
  -> Coordinator reads replicas in parallel
  -> Return after R replica responses
```

Za `N = 3, R = 2`, read može uspeti dok je jedna replika nedostupna, ako dve potrebne replike odgovore.

### Quorum Delete

```text
Client Delete(key)
  -> Any Node
  -> Forward to coordinator if necessary
  -> Coordinator resolves preference list
  -> Coordinator sends delete to replicas in parallel
  -> Each replica performs local LSM delete
  -> Return success as soon as W acknowledgements arrive
```

---

## Local Storage Write Path

Lokalni `Put` i `Delete` koriste durability-first redosled:

```text
Put/Delete
  -> WAL append
  -> WAL fsync according to configuration
  -> Active Memtable
  -> Memtable rotation
  -> Immutable Memtable
  -> Background Flush
  -> SSTable
  -> Manifest Publish
  -> New Version
  -> Background Compaction
```

Redosled je ključan: acknowledged mutacija se prvo zapisuje u WAL, a zatim postaje vidljiva kroz memtable i kasnije SSTable slojeve.

---

## Local Storage Read Path

```text
Get(key)
  -> Active Memtable
  -> Immutable Memtables, newest first
  -> Current Version / SSTables
```

Ako se tombstone pronađe u novijem lokalnom storage sloju, lookup se završava kao `not found`, čak i ako stariji SSTable sadrži prethodnu vrednost.

---

## WAL and Recovery

Write-Ahead Log je prvi durability layer lokalnog engine-a.

WAL podržava:

- Kreiranje i otvaranje WAL segmenata
- Segment rolling kada se dostigne `WALSegmentRollBytes`
- Header validation za postojeće segmente
- Binary encode/decode WAL record-a
- Checksum validation
- Replay validnih zapisa pri restart-u
- Recovery nakon incomplete/torn final record-a
- Bezbedno trunciranje nekompletnog tail-a

Sa `WALFsyncEveryN = 1`, acknowledged lokalne `Put` i `Delete` mutacije imaju subprocess testove koji potvrđuju WAL recovery nakon naglog prekida procesa bez `Close()`.

Ova tvrdnja pokriva process-level crash recovery prema WAL fsync politici. Ne predstavlja apsolutnu garanciju za fizički nestanak struje, bug u filesystem-u ili kvar storage uređaja izvan granica koje OS i uređaj pružaju za `fsync`.

---

## Manifest and SSTables

Manifest je authoritative source za live SSTable fajlove.

SSTable fajl postaje deo aktivnog on-disk stanja tek kada njegov metadata entry bude uspešno objavljen kroz novi manifest. Ako crash nastane između SSTable write/rename koraka i manifest publish-a:

- stari manifest ostaje authoritative;
- stari read state ostaje ispravan;
- novi SSTable može ostati orphan fajl bez uticaja na read correctness.

SSTable reader dodatno validira index blok i Bloom filter metadata kako korumpirani fajlovi ne bi izazvali pogrešan lookup ili runtime panic.

---

## Compaction

`lsmkv` trenutno koristi size-tiered compaction.

Compaction:

1. bira kompatibilne SSTable kandidate;
2. merge-uje podatke po redosledu svežine;
3. pravi novu SSTable;
4. objavljuje rezultat kroz manifest;
5. tek tada uklanja replaced tabele iz aktivnog version view-a.

Tombstone zapisi u lokalnom LSM engine-u sprečavaju da starija vrednost ponovo postane vidljiva nakon lokalnog `Delete`.

---

## Project Structure

```text
lsmkv-github/
├── cmd/
│   ├── lsmkv/
│   │   └── main.go                  # Lokalni CLI / application entrypoint
│   └── server/
│       └── main.go                  # gRPC node server entrypoint
├── config/
│   └── default.json                 # Podrazumevana storage konfiguracija
├── internal/
│   ├── coordinator/                 # Coordinator i quorum-related logika
│   ├── lsm/                         # Single-node durable LSM storage engine
│   ├── node/                        # gRPC node runtime, forwarding i replication
│   └── ring/                        # Consistent hashing ring i preference list
├── proto/                           # Protobuf / generated gRPC definicije
├── go.mod
└── README.md
```

Najvažniji paketi:

| Package | Odgovornost |
|---|---|
| `internal/lsm` | WAL, memtable, SSTables, manifest, flush, compaction, recovery |
| `internal/ring` | Hash ring, virtual nodes i preference-list izbor |
| `internal/coordinator` | Coordinator odluke i quorum konfiguracija |
| `internal/node` | gRPC server/client, request forwarding, quorum fan-out i local storage integracija |
| `proto` | RPC ugovori između client-a i node-ova |
| `cmd/server` | Pokretanje jednog cluster noda |
| `cmd/lsmkv` | Lokalni CLI / storage entrypoint |

> Runtime data direktorijumi, WAL segmenti i SSTable fajlovi ne treba da budu deo source-control istorije.

---

## Tests

Pokreni kompletnu test suite iz root direktorijuma:

```bash
go test -count=1 ./...
```

Za standardni brži run:

```bash
go test ./...
```

Test suite pokriva:

### Storage Engine

- Config defaults i validation
- WAL record encoding/decoding
- WAL segment lifecycle
- WAL replay i torn-tail recovery
- Basic Store behavior
- Read path behavior
- Flush behavior
- Force flush behavior
- Backpressure behavior
- Compaction picker i merge behavior
- SSTable writer i reader behavior
- Bloom filter validation
- Manifest structural validation
- Background scheduler behavior
- Crash injection scenarije
- Hard-crash durability za `Put` i `Delete`

### Distribution Layer

- Consistent hash ring ponašanje
- Preference list i coordinator izbor
- gRPC request forwarding
- Quorum replication u healthy 3-node cluster-u
- Quorum `Get` u healthy cluster-u
- Replicated `Delete` u healthy cluster-u
- Read quorum sa jednom ugašenom replikom
- Write quorum sa jednom ugašenom replikom
- Delete quorum sa jednom ugašenom replikom

---

## Tested Failure Behavior

Za `N = 3`, `R = 2`, `W = 2`, integration testovi potvrđuju:

| Scenario | Expected result |
|---|---|
| Sve tri replike su dostupne | `Put`, `Get` i `Delete` rade kroz quorum flow |
| Jedna replika je down tokom `Put` | Write uspeva sa dve dostupne quorum potvrde |
| Jedna replika je down tokom `Get` | Read uspeva sa odgovorima dve dostupne replike |
| Jedna replika je down tokom `Delete` | Delete uspeva sa dve dostupne quorum potvrde |
| Lokalni proces se prekine posle acknowledged mutacije | WAL replay obnavlja acknowledged lokalno stanje |

---

## Current Scope and Limitations

Ovo je funkcionalan Dynamo-inspired quorum MVP, ali nije kompletna produkcijska Dynamo/Cassandra implementacija.

Svesno nisu implementirani:

- Dynamic cluster membership
- Heartbeat failure detector i gossip membership
- Node join/leave data migration i automatic rebalance
- Sloppy quorum
- Hinted handoff
- Read repair
- Anti-entropy repair
- Merkle trees
- Vector clocks
- Sibling values za concurrent writes
- Conflict resolution između divergentnih verzija
- Distributed tombstones
- Replica catch-up nakon node recovery-ja
- TLS, authentication i authorization
- Prometheus metrics endpoint
- Distributed tracing i production observability
- Chaos test harness za kontinuirane kill/restart scenarije

### Important Delete Limitation

Lokalni storage engine koristi tombstone zapise i lokalni delete je crash-safe.

Međutim, distribucioni `Delete` trenutno nema **distributed tombstone**, hinted handoff ili repair mehanizam.

To znači:

1. Ako su sve replike dostupne, delete se propagira na sve replike.
2. Ako je jedna replika nedostupna, delete može uspešno završiti sa `W = 2`.
3. Nedostupna replika može propustiti delete i zadržati staru vrednost.
4. Kada se vrati, sistem trenutno nema automatski repair koji bi je uskladio.

Ovo ograničenje je poznato i namerno ostavljeno za naprednu narednu fazu. Produkcijski sistemi rešavaju ovaj slučaj kombinacijom distributed tombstone-a, hinted handoff-a, verzionisanja i anti-entropy repair-a.

---

## Current Status

### Completed

- Stabilan single-node LSM storage engine
- WAL durability i crash recovery
- Memtable, SSTables, manifest i size-tiered compaction
- Corruption validation za ključne on-disk strukture
- gRPC node komunikacija
- Static multi-node cluster konfiguracija
- Consistent hashing i virtual nodes
- Coordinator routing i request forwarding
- Replication factor `N`
- Read quorum `R`
- Write quorum `W`
- Quorum `Put`, `Get` i `Delete`
- Early-return quorum behavior
- Healthy i one-node-down integration testovi

### Deferred Advanced Work

- Hinted handoff i sloppy quorum
- Heartbeat/gossip membership
- Vector clocks, siblings i conflict resolution
- Anti-entropy repair preko Merkle trees
- Dynamic membership i data rebalance
- Distributed delete correctness kroz tombstones i repair
- Metrics, tracing, chaos testing i production hardening

---

## Design Goal

Cilj projekta je da jasno i testabilno demonstrira dva važna sistema:

- kako durable LSM storage engine čuva podatke kroz WAL, memtable, SSTable i compaction tok;
- kako Dynamo-style distribution layer koristi consistent hashing, replication i quorum pravila da održi dostupnost osnovnih operacija kada jedan node nije dostupan.

Trenutni rezultat je namerno ograničen, ali kompletan i objašnjiv distributed storage MVP: dovoljno realan da demonstrira glavne distributed-systems ideje, a dovoljno mali da ostane razumljiv, održiv i testabilan.