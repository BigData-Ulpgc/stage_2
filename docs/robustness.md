# Robustness of indexing and publication (DR-11)

What can fail while a book goes from `index-queue` to Search, what must happen then, and which
test proves it. The tests are in `indexing-service/src/test/java/es/ulpgc/bigdata/indexing/`.
Those marked *Mongo* run against a real MongoDB (`MONGO_URI`), and those marked *Hazelcast*
against embedded members. The README explains how to run them.

The three rules that all the rows depend on:
- **Indexing is idempotent.** Metadata are replaced by `_id`, posting lists use `$addToSet`, and
  `index_log` has one entry per book, so repeating a book or a batch is always safe.
- **`index_log` is written last** (DR-07): metadata, then posting lists, then the log. A book
  in the log is complete, and `publish` and `INDEXED` only trust the log.
- **Visibility changes in one step:** a book becomes visible when `CURRENT` moves to its
  version, through a compare-and-set (coordination contract §5).

## Indexing a book or a batch

| Failure | What must happen | Test |
|---|---|---|
| The same book is indexed twice | Same result, nothing duplicated | `IndexingServiceTest.indexarDosVecesDaElMismoResultado`, `IndexingServiceMongoTest.indexarDosVecesNoDuplicaNadaEnMongo` (*Mongo*) |
| **The same books indexed at once by two replicas, 4 threads each** | The index of a single run, and no `E11000 duplicate key` | `ConcurrentIndexingMongoTest.dosReplicasIndexandoLosMismosLibrosALaVezDejanElIndiceComoUnaSola` (*Mongo*) |
| Several threads of one replica at once | Only one writes to the inverted index at a time | `IndexingServiceTest.variosHilosALaVezNuncaEscribenEnElIndiceALaVez` |
| The metadata write fails | The index is not touched | `IndexingServiceTest.siFallanLosMetadatosNoSeTocaElIndice` |
| The flush of the posting lists fails | The book is not in the log, and the lock is released | `IndexingServiceTest.siFallaElFlushElLibroNoEntraEnElLog`, `siFallaElFlushElLockSeSueltaYOtroHiloPuedeIndexar` |
| The log write fails | The call fails, and the retry completes the book | `IndexingServiceTest.siFallaElLogElUpdateFallaYElReintentoLoCompleta` |
| A write of a batch fails | No book of the batch is in the log, and the retry completes them | `IndexingServiceTest.siFallaLaEscrituraNingunLibroDelLoteEntraEnElLogYElReintentoLoCompleta` |
| A bad book inside a batch (not ready, no terms, unreadable) | Only that book fails, the others are indexed | `IndexingServiceTest.losLibrosQueNoSePuedenPrepararSeQuedanFueraYLosDemasSeIndexan`, `IndexingWriterTest.enUnLoteCadaLibroMaloFallaSoloYLosBuenosQuedanIndexed` |
| The thread is interrupted while a batch is prepared | Nothing is written | `IndexingServiceTest.siInterrumpenAlHiloMientrasSePreparaNoSeEscribeNada` |

## The `index-queue` writer

| Failure | What must happen | Test |
|---|---|---|
| MongoDB is down | The whole batch goes back to the queue without spending attempts, and is indexed when MongoDB returns | `IndexingWriterTest.conMongoCaidoTodoElLoteVuelveALaColaSinGastarIntentosYLuegoSeIndexa` (*Hazelcast*) |
| A book is not complete in the datalake yet | It goes back to the queue until it runs out of attempts | `IndexingWriterTest.unLibroSinReadyVuelveALaColaHastaAgotarLosIntentos` (*Hazelcast*) |
| The writer loses the lease of a book | It drops the book, and another attempt indexes it | `IndexingWriterTest.siPierdeElLeaseDescartaElLibroYOtroIntentoLoIndexa` (*Hazelcast*) |
| Two replicas | Only one writes, and the other takes over when it stops | `IndexingWriterTest.conDosEscritoresSoloUnoIndexaYElOtroTomaElRelevo` (*Hazelcast*), `AppTest.conDosIndexingSoloUnoEscribeEnLotesYAlPararloElOtroTomaElRelevo` (*Hazelcast*, *Mongo*) |
| The cluster is lost | The writer stops claiming books, releases `index-writer` and goes on when the cluster returns | `IndexingWriterTest.sinClusterDejaDeReclamarSueltaElLockYAlVolverSigue` (*Hazelcast*) |
| The cluster is lost and returns during a book | Never two writers at once | `IndexingWriterTest.unaReconexionDuranteUnLibroNoDejaDosEscritores`, `unaReconexionCortaConElLockAunSuyoLoSueltaYLoVuelveATomar` (*Hazelcast*) |
| The service starts without MongoDB or without the cluster | It starts anyway, and only what needs them fails | `AppTest.arrancaAunqueMongoEsteCaidoYSoloFallaLoQueLoNecesita`, `sinMongoElEscritorArrancaIgualYSinClusterPasaAEspera` |

## Publication

| Failure | What must happen | Test |
|---|---|---|
| **A publication while a batch is half written** | No book of that batch is published, and the next publication shows each of them complete | `ConcurrentIndexingMongoTest.publicarMientrasSeEscribeUnLoteNoHaceVisibleNingunLibroAMedias` (*Mongo*) |
| Two publications at once in one replica | One after the other | `PublisherTest.dosPublicacionesALaVezEnElMismoProcesoVanUnaTrasOtra` |
| Another replica publishes first | The publication is repeated on top of its version | `PublisherTest.siOtraReplicaPublicaAntesRepiteLaPublicacionSobreSuVersion`, `siOtraReplicaPublicaLaMismaVersionCuentaTambienLosLibrosDeEstaReplica` |
| The process dies before switching `CURRENT` | The next publication, or the next start, completes it | `PublisherTest.completaUnaPublicacionQueMurioAntesDeCambiarCurrent`, `alArrancarCompletaSoloLosInterrumpidos`, `AppTest.alArrancarCompletaUnaPublicacionInterrumpida` |
| The history entry cannot be written | The call still answers the published version, and it is repaired later | `PublisherTest.siFallaElHistorialRespondeLaVersionPublicadaYLaSiguienteLlamadaLoRepara`, `alArrancarReparaElHistorialQueFalta` |
| Nothing new to publish | No new version (idempotent) | `IndexingApiTest.publishDosVecesSeguidasNoCreaOtraVersion` |

## After a publication: `PUBLISHED` (T10) and the `IndexPublished` event

| Failure | What must happen | Test |
|---|---|---|
| T10 is interrupted | It is repaired when the service connects again | `HazelcastPublicationNotifierTest.alConectarReparaUnT10Interrumpido` (*Hazelcast*) |
| T10 fails | The event is sent anyway, and the next publication marks the states | `HazelcastPublicationNotifierTest.siT10FallaElEventoSaleIgualYLaSiguientePublicacionMarcaLosEstados` (*Hazelcast*) |
| The cluster is lost with an event queued | It is not sent then; it is announced when the cluster returns | `HazelcastPublicationNotifierTest.siSePierdeElClusterConUnEventoEnColaNoSeEnviaYSeAnunciaAlVolver`, `desconectadoSeSaltaYAlVolverSeAnunciaLaVersionYSeMarcanLosEstados` (*Hazelcast*) |

## Real runs (DR-09, two replicas, `INDEX_WRITE_BATCH=20`)

- **SIGTERM to the active replica** after 100 books: the standby takes over **2.8 s** later,
  and all 300 books end up `INDEXED`, with nothing requeued.
- **`kill -9` to the active replica** after 100 books: the standby takes over **4.0 s** later.
  The 17 books the dead replica had claimed stayed `INDEXING` until their lease expired. Then a
  stand-in for the Control's lease check (`requeueExpiredLeases`) put them back in the queue,
  and the survivor indexed them.

## How the new tests were checked (mutation testing)

Each change was made in the code, one at a time, and the test had to fail. All of them were
caught every time they were run.

| Change in the code | Caught by |
|---|---|
| The filter of a term is not only an equality (MongoDB no longer retries the upsert) | `E11000` in the two-replica test |
| The same for `_id` in `books` or in `index_log` | `E11000` in the two-replica test |
| The index on `term` is not unique | The two-replica test: two upserts that meet both insert, about 4,700 documents for 3,100 terms |
| `$push` instead of `$addToSet` | The two-replica test: a repeated id |
| The flush outside the `writeLock` | The two-replica test: a thread fails, because the threads of a replica share the pending map of its index |
| `index_log` written before the flush | The publication test: two books published without their posting lists |
| The metadata written after the log | The publication test, at its first check: the metadata are not written yet at the pause |
