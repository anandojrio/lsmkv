# lsmkv

`lsmkv` je single-node, durable key-value storage engine implementiran u Go-u, zasnovan na LSM-tree arhitekturi.

Trenutna implementacija pokriva samo lokalni storage engine: WAL durability, memtable, SSTable fajlove, manifest/version publishing, background flush, compaction, crash recovery i integrity validation. Distribuirani deo projekta nije obuhvaćen ovim dokumentom.

## Features

- `Put`, `Get` i `Delete` operacije
- Overwrite semantika i tombstone zapisi za brisanje
- Write-Ahead Log (WAL) pre memtable upisa
- Konfigurisani WAL `fsync` durability režim
- WAL segmentacija, record checksum i replay pri restart-u
- Oporavak od truncated/torn WAL tail-a
- Memtable rotacija i immutable memtables
- Background scheduler za flush i compaction posao
- SSTable writer i reader
- Bloom filter za optimizaciju SSTable lookup-a
- Manifest kao authoritative source za live SSTable fajlove
- Version layer za immutable published SSTable view
- Force flush, graceful close i fast close putanje
- Size-tiered compaction
- Zaštita od orphan SSTable fajlova
- Crash zaštita između SSTable rename-a i manifest publish-a
- Hard-crash subprocess testovi za acknowledged `Put` i `Delete`
- Defensive corruption validation za WAL, manifest, SSTable index i Bloom filter metadata

## Project Structure

```text
lsmkv-github/
├── .vscode/                         # Lokalna editor konfiguracija
├── cmd/
│   └── lsmkv/
│       └── main.go                  # Application entrypoint
├── config/
│   └── default.json                 # Podrazumevana konfiguracija engine-a
├── data/                            # Lokalni runtime storage direktorijum
│   └── wal/
│       └── 000001.wal               # WAL segment generisan tokom rada
├── internal/
│   └── lsm/
│       ├── bloom.go                 # Bloom filter implementacija i decode guards
│       ├── bloom_test.go            # Bloom filter testovi i corruption guards
│       ├── compaction.go            # Compaction orchestration
│       ├── compaction_merge.go      # Merging SSTable podataka tokom compaction-a
│       ├── compaction_merge_test.go # Testovi merge logike
│       ├── compaction_picker.go     # Odabir SSTable fajlova za compaction
│       ├── compaction_picker_test.go# Testovi compaction picker-a
│       ├── compaction_test.go       # Integration testovi za compaction
│       ├── config.go                # Config defaults, load i validation
│       ├── config_test.go           # Config testovi
│       ├── crash_hooks.go           # Injectable crash hooks za fault injection
│       ├── crash_test.go            # Crash recovery i hard-crash durability testovi
│       ├── errors.go                # Sentinel greške engine-a
│       ├── manifest.go              # Manifest load/save i structural validation
│       ├── manifest_test.go         # Manifest validation testovi
│       ├── memtable.go              # In-memory sorted write structure
│       ├── metrics.go               # Engine metrics i counters
│       ├── scheduler.go             # Background flush/compaction scheduler
│       ├── scheduler_test.go        # Scheduler behavior testovi
│       ├── sstable_reader.go        # SSTable read path i integrity validation
│       ├── sstable_reader_all_entries_test.go
│       ├── sstable_reader_test.go   # SSTable reader testovi
│       ├── sstable_writer.go        # SSTable file writer
│       ├── sstable_writer_test.go   # SSTable writer testovi
│       ├── stats.go                 # Stats strukture i snapshot podaci
│       ├── store.go                 # Glavni Store API i lifecycle
│       ├── store_backpressure_test.go
│       ├── store_compact_test.go
│       ├── store_flush_test.go
│       ├── store_force_flush_test.go
│       ├── store_read_path_test.go
│       ├── store_test.go            # Core Store testovi
│       ├── version.go               # Published SSTable Version view
│       ├── wal.go                   # WAL segment lifecycle
│       ├── wal_record.go            # WAL binary record encoding/decoding
│       ├── wal_record_test.go       # WAL record testovi
│       ├── wal_recovery.go          # WAL replay i torn-tail recovery
│       └── wal_segment_test.go      # WAL segment testovi
├── go.mod                           # Go module definicija
├── HANDOFF.md                       # Development handoff beleške
├── PROJECT_STATUS.md                # Status projekta
└── README.md                        # Ovaj dokument
```

> `data/` je lokalni runtime direktorijum. WAL i SSTable fajlovi koji se generišu tokom rada ne treba da budu deo source-control istorije osim ako repo eksplicitno definiše drugačiji development workflow.

## Architecture

Engine koristi Log-Structured Merge Tree pristup.

```text
Put/Delete
  -> WAL append
  -> WAL fsync prema konfiguraciji
  -> Active Memtable
  -> Memtable rotation
  -> Immutable Memtable
  -> Background Flush
  -> SSTable
  -> Manifest Publish
  -> New Version
  -> Background Compaction
```

Read path traži podatke od najnovijeg ka najstarijem sloju:

```text
Get(key)
  -> Active Memtable
  -> Immutable Memtables (newest first)
  -> Current Version / SSTables
```

Ako se tombstone pronađe u novijem sloju, lookup se završava kao `not found`, čak i ako stariji SSTable sadrži prethodnu vrednost.

## Write Path

`Put` i `Delete` koriste durability-first redosled:

1. Validacija input-a i stanja store-a
2. Provera write-stall/backpressure uslova
3. Dodela sledećeg sequence broja
4. Upis mutacije u WAL
5. `fsync` prema `WALFsyncEveryN` konfiguraciji
6. Upis u aktivni memtable
7. Memtable rotacija kada se dostigne `MemtableMaxBytes`
8. Background flush immutable memtable-a u SSTable

Ovaj redosled je ključan: acknowledged mutacija je prvo zapisana u WAL, pa tek onda postaje vidljiva u memtable-u.

## WAL

Write-Ahead Log predstavlja prvi durability layer engine-a.

WAL podržava:

- Kreiranje i otvaranje WAL segmenata
- Segment rolling kada se dostigne `WALSegmentRollBytes`
- Header validation za postojeće segmente
- Binary encode/decode WAL record-a
- Checksum validation
- Replay svih validnih zapisa pri restart-u
- Recovery nakon incomplete/torn final record-a
- Bezbedno trunciranje nekompletnog tail-a

WAL konfiguracija i postojeći segment header-i se validiraju pri otvaranju, tako da nevalidna konfiguracija ili korumpiran segment ne nastavljaju tiho kroz recovery path.

## Manifest

Manifest je authoritative source za live SSTable fajlove.

SSTable nije deo aktivnog on-disk stanja samo zato što fajl postoji na disku. Tabela postaje live tek kada novi manifest bude uspešno objavljen.

`loadManifest()` odbija korumpiran ili logički nevalidan manifest, uključujući:

- Nepodržanu manifest verziju
- Table entry sa `ID == 0`
- Prazan `File`
- Putanju izvan data direktorijuma, na primer `../some-file.sst`
- Nevalidan sequence range, gde je `MinSeqNo > MaxSeqNo`
- Negativan `FileSize`
- Dupli table ID
- Dupli table file path

Takva stanja vraćaju `ErrCorruptionDetected`. `manifest_test.go` proverava da validan manifest i dalje normalno prolazi, a nevalidni manifest-i budu odbijeni.

## SSTables

SSTable je immutable on-disk struktura nastala flush-ovanjem memtable-a.

Relevantne komponente su:

- `sstable_writer.go` — zapis podataka, blokova, index-a i Bloom metadata
- `sstable_reader.go` — lookup i čitanje SSTable sadržaja
- `sstable_reader_all_entries_test.go` — provera čitanja kompletnog sadržaja
- `sstable_writer_test.go` i `sstable_reader_test.go` — writer/reader correctness testovi

Reader dodatno validira SSTable index blok. Index se prihvata samo ako konzistentno deli data region fajla na uređene i validne blokove.

Ova provera sprečava da korumpiran index navede reader da čita pogrešan deo fajla, vrati pogrešan `ErrNotFound`, prijavi nejasan I/O problem ili pogrešno interpretira podatke. Umesto toga, engine fail-fast vraća `ErrCorruptionDetected`.

## Bloom Filter Validation

Bloom filter se koristi kao read optimization pre detaljnog SSTable lookup-a.

Bloom payload se prihvata samo ako važe sledeća pravila:

- `m` nije nula
- `m` je deljiv sa 8
- `k` nije nula
- Veličina bit-array-a je tačno `m / 8`

Bez tih guard-a, korumpiran Bloom header može kasnije izazvati runtime panic, na primer modulo operaciju nad nulom. Cilj je da korupcija postane kontrolisani `ErrCorruptionDetected`, a ne pad servera.

`bloom_test.go` pokriva ove validation scenarije.

## Memtable, Flush and Scheduler

Aktivni memtable prima nove mutacije dok ne dostigne `MemtableMaxBytes`.

Kada se prag dostigne:

1. Aktivni memtable postaje immutable.
2. Novi aktivni memtable nastavlja da prima write-ove.
3. Background scheduler flush-uje najstariji immutable memtable u SSTable.
4. Novi SSTable se objavljuje kroz manifest.
5. Novi `Version` postaje vidljiv read path-u.

`scheduler_test.go` potvrđuje sledeće:

- Više malih write-ova izazove memtable rotacije
- Background flush worker zaista odradi posao
- `ImmutablesCount` na kraju padne na `0`
- Objavi se najmanje jedan SSTable
- Svi upisani ključevi ostaju čitljivi

Test pokazuje da scheduler ne zahteva dodatne izmene za trenutnu Phase A funkcionalnost.

## Compaction

Compaction smanjuje broj SSTable fajlova tako što bira kompatibilne tabele i merge-uje ih u novu SSTable strukturu.

Kod je podeljen na:

- `compaction_picker.go` — bira kandidatske SSTable fajlove
- `compaction_merge.go` — merge logika
- `compaction.go` — orchestration i publish koraci

Compaction rezultat postaje live tek nakon uspešnog manifest publish-a. Ako crash nastane između SSTable write/rename koraka i manifest publish-a, stari manifest ostaje authoritative, a novonastali SSTable može ostati orphan fajl bez uticaja na read correctness.

## Crash Recovery

Testovi pokrivaju više kategorija crash scenarija:

- Crash između SSTable rename-a i manifest publish-a
- Crash tokom compaction-a pre manifest publish-a
- Torn/truncated WAL tail
- Restart u prisustvu orphan SSTable fajla
- Hard process crash nakon acknowledged `Put`
- Hard process crash nakon acknowledged `Delete`

### Hard Crash After Put

`TestHardCrashAfterAcknowledgedPutsRecoverFromWAL` koristi child process koji:

1. Otvara store sa `WALFsyncEveryN = 1`
2. Koristi veliki `MemtableMaxBytes`, pa se flush ne dešava
3. Uspešno izvršava `Put` operacije
4. Završava preko `os.Exit(0)` bez `store.Close()`

Parent process zatim ponovo otvara isti data direktorijum i potvrđuje da su acknowledged vrednosti obnovljene WAL replay-om.

Ovaj test direktno dokazuje da fsync-ovani acknowledged `Put` zapisi prežive nagli prekid procesa.

### Hard Crash After Delete

`TestHardCrashAfterAcknowledgedDeleteRecoversTombstone` koristi child process koji:

1. Otvara store sa `WALFsyncEveryN = 1`
2. Izvršava `Put("gone", "value")`
3. Izvršava `Delete("gone")`
4. Završava preko `os.Exit(0)` bez `store.Close()`

Nakon restart-a, parent process replay-uje WAL i proverava da `Get("gone")` vraća `found == false`.

Ovaj test dokazuje durability tombstone-a, odnosno da potvrđeno brisanje ne može vratiti staru vrednost nakon process crash-a.

## Store Lifecycle

`Store` podržava sledeće lifecycle putanje:

- `Open()` — učitava manifest, otvara SSTable reader-e, otvara WAL i replay-uje WAL zapise
- `ForceFlush()` — sinhrono flush-uje aktivne/pending podatke kada je potreban eksplicitan durability korak u SSTable sloj
- `Close()` / graceful close — zaustavlja background rad i završava pending flush putanju
- `CloseFast()` — zatvara engine bez čekanja da se svi pending immutable memtable-i flush-uju

Nakon reopen-a, engine kombinuje manifest state i WAL replay da bi obnovio najnovije validno stanje.

## Error Handling

Engine koristi sentinel greške za jasnije razlikovanje failure kategorija:

- `ErrInvalidArgument` — nevalidan input ili config
- `ErrStoreClosed` — operacija nad zatvorenim store-om
- `ErrNotFound` — ključ ne postoji u storage slojevima
- `ErrCorruptionDetected` — korumpiran WAL, manifest, SSTable metadata ili Bloom metadata
- `ErrIOFailure` — I/O failure tokom storage operacije
- `ErrTooManyImmutables` — previše pending immutable memtable-a
- `ErrWriteStall` — write je odbijen zbog L0/backpressure limita

## Tests

Pokreni kompletnu test suite iz root direktorijuma:

```bash
go test -count=1 ./...
```

Test suite pokriva:

- Config defaults i validation
- WAL record encoding/decoding
- WAL segment lifecycle
- WAL recovery
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
- Crash injection scenarios
- Hard-crash durability za `Put` i `Delete`

## Durability Scope

Sa `WALFsyncEveryN = 1`, acknowledged `Put` i `Delete` mutacije imaju konkretne subprocess testove koji potvrđuju WAL recovery nakon naglog prekida procesa bez `Close()`.

Ta tvrdnja pokriva process-level crash recovery prema WAL fsync politici. Ne predstavlja apsolutnu garanciju za fizički nestanak struje, bug u filesystem-u ili kvar storage uređaja izvan granica koje OS i uređaj pružaju za `fsync`.

## Current Status

Phase A, single-node LSM key-value engine, ima implementirane osnovne write/read/storage/recovery tokove i ciljane testove za durability i corruption handling.

Jedina eksplicitno odložena stavka je potencijalna concurrency provera u `version.go`: standardni build i testovi prolaze, dok je race-oriented verifikacija trenutno ograničena Windows toolchain okruženjem.