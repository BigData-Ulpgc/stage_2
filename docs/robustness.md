# Robustness of indexing and publication (DR-11, DR-10)

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
| A batch takes longer than the lease (a large index, DR-13) | The writer renews the leases of its batch every third of the lease, so the Control does not requeue books that are still being written | `IndexingWriterTest.unLoteMasLargoQueElLeaseLoRenuevaYElControlNoLoDevuelveALaCola` (*Hazelcast*) |
| A book of the batch is requeued anyway while the batch is written | The writer does not complete it, and another attempt indexes it | `IndexingWriterTest.siElLeaseSePierdeDuranteElLoteNoSeCompletaYOtroIntentoLoIndexa` (*Hazelcast*) |
| The writer thread dies (an `Error`, such as an `OutOfMemoryError` writing a large batch) | The service exits with code 1 and Docker restarts it, instead of answering `/status` as healthy without indexing; the books of the batch wait for their lease to expire and the Control requeues them | `IndexingWriterTest.unErrorAlEscribirMataAlEscritorYLosLeasesDelLoteDejanDeRenovarse` (*Hazelcast*), `AppTest.siElEscritorMuereElServicioSaleConCodigo1DesdeOtroHilo`, `siElServicioYaSeEstaParandoNoLlamaAExit`, `elHiloDelEscritorParaElServicioSiMuereSalvoSiYaSeEstaParando` (*Hazelcast*) |
| The heap fills up while a batch is written (`OutOfMemoryError` in any thread: the MongoDB driver's, Hazelcast's, an HTTP one) | The JVM ends (`-XX:+ExitOnOutOfMemoryError`, Indexing in `docker-compose.yml`) and Docker restarts the service | Real run: see the PR of this fix (batches of 1000 books with 768 MB of heap) |
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

## The rebuild of the index (DR-10)

`POST /index/rebuild` fills a new collection `inverted_index_rNN` and switches `CURRENT` to it
with one publication (coordination contract §5). Until the switch, Search reads the old
collection, so every failure before it leaves the index as it was.

| Failure | What must happen | Test |
|---|---|---|
| Two rebuilds at once, from any replicas | Only one runs, the other gets 409 | `RebuilderTest.siHayOtraEnMarchaRespondeQueYaHayUna`, `IndexingApiRebuildTest.mientrasReconstruyeRebuildUpdateYPublishResponden409` (*Hazelcast*), `CoordinationTest.soloUnaReconstruccionALaVez` (*Hazelcast*) |
| The writer of `index-queue` is writing a batch | The rebuild waits for `index-writer`; the writer finishes its batch, releases it and claims nothing more | `RebuilderTest.esperaAQueElEscritorSuelteElLockAntesDeTocarNada`, `IndexingWriterTest.elEscritorActivoSueltaElLockEnCuantoVeLaMarca`, `conUnaReconstruccionEnMarchaNoReclamaNiCogeElLock` (*Hazelcast*) |
| An update writes into the old collection during the rebuild | The book is indexed into the new one too, before the switch | `RebuilderTest.unLibroEscritoEnLaColeccionViejaMientrasEsperaTambienEntra` (*Hazelcast*) |
| MongoDB fails during the rebuild | The new collection is dropped, `CURRENT` is untouched, the state is `failed` | `RebuilderTest.siFallaAntesDelCambioBorraSuColeccionYCurrentNoCambia` (*Hazelcast*) |
| The marker expires (the replica stalled longer than its TTL) | The rebuild stops without switching | `RebuilderTest.siLaMarcaCaducaSeDetieneSinCambiarCurrent`, `CoordinationTest.laMarcaCaducaSiNadieLaRenuevaYEntoncesYaNoSePuedeRenovar`, `unaReconstruccionCaducadaNoQuitaNiRenuevaLaMarcaDeLaSiguiente` (*Hazelcast*) |
| The service stops during the rebuild | The rebuild fails, drops its collection and removes its marker at once | `RebuilderTest.alCerrarElServicioLaReconstruccionFallaYBorraSuColeccion` (*Hazelcast*), `AppTest.pararElServicioEnMitadDeUnaReconstruccionNoDejaNadaColgado` (*Hazelcast*, *Mongo*) |
| The replica dies during the rebuild | The marker expires, indexing goes on, and the status shows it as `failed` | `CoordinationTest.laMarcaCaducaSiNadieLaRenueva…`, `RebuilderTest.unaQueDiceRunningSinMarcaEsDeUnaReplicaQueMurio` (*Hazelcast*) |
| A collection left by a rebuild that died | The next rebuild drops it, and never reuses its name | `RebuilderTest.borraLasColeccionesHuerfanasYNuncaReutilizaSuNombre`, `trasUnFalloLaSiguienteEmpiezaDeCeroYNoReutilizaElNombre` (*Hazelcast*) |
| Search still reads the old collection after the switch | It is dropped only after `INDEX_REBUILD_DROP_SECONDS`, also by another rebuild | `RebuilderTest.laColeccionViejaSeBorraTrasElMargenYNoAntes`, `noBorraLaColeccionQueOtraReconstruccionDejoEsperandoSuMargen` (*Hazelcast*), `AppTest.reconstruyeDeExtremoAExtremoYBorraLaColeccionVieja` (*Hazelcast*, *Mongo*) |
| After the switch | Every write goes to the new collection, in every replica, without restarting | `IndexingServiceCollectionTest.currentSeLeeAntesDeCadaEscrituraYCadaColeccionSeAbreUnaVez`, `AppTest.trasUnaReconstruccionIndexaEnLaColeccionDeCurrent` (*Mongo*) |
| The switch has no new book | It is a new version anyway, and it is announced, so Search loads the new collection | `PublisherTest.publishIntoCambiaLaColeccionAunqueNoHayaLibrosNuevos`, `HazelcastPublicationNotifierTest.elCambioDeUnaReconstruccionSeAnunciaAunqueNoHayaLibrosNuevos` (*Hazelcast*) |

## Real runs (DR-09, two replicas, `INDEX_WRITE_BATCH=20`)

- **SIGTERM to the active replica** after 100 books: the standby takes over **2.8 s** later,
  and all 300 books end up `INDEXED`, with nothing requeued.
- **`kill -9` to the active replica** after 100 books: the standby takes over **4.0 s** later.
  The 17 books the dead replica had claimed stayed `INDEXING` until their lease expired. Then a
  stand-in for the Control's lease check (`requeueExpiredLeases`) put them back in the queue,
  and the survivor indexed them.

## Real runs (DR-10, two Indexing replicas and Search, 2,000 synthetic books)

The jars of the branch on the host, against a cluster of two Hazelcast members and a MongoDB of
their own, with `INDEX_WRITE_BATCH=100` and `INDEX_REBUILD_DROP_SECONDS=20`. Replica A held
`index-writer`, B was the standby, and Search answered `q=house` (1,876 results) all along.

- **A rebuild started on B**, the standby. A released `index-writer` before its next claim,
  and both writers said `paused`. A second rebuild, an update and a publication got 409 in
  both replicas, and both showed the same progress, 100 books at a time. 2,000 books took
  **145 s** (about 73 ms per book, the cost of indexing measured in DR-09). Search answered
  from version 1 until the switch and then served version 2 on `inverted_index_r01`, with the
  same 1,876 results. `inverted_index` was dropped 20 s later, and an update afterwards
  answered 200.
- **`kill -9` to B after its first batch.** A answered 409 until the marker expired, **29 s**
  later. Then its status said `failed` ("the replica that ran it stopped"), its writer was
  `active` again, and updates answered 200. Search stayed on version 2 the whole time.
  `inverted_index_r02` was left behind.
- **B restarted, and a rebuild from A:** it dropped `inverted_index_r02`, built
  `inverted_index_r03` in 140 s and switched to version 3. `inverted_index_r01` was dropped
  20 s later.
- **SIGTERM to B during a rebuild:** it stopped in **1.4 s**, dropped `inverted_index_r04` and
  removed the marker, so A's writer was `active` again 1.7 s after the signal.

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
