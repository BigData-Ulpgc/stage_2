package es.ulpgc.bigdata.datalake;

import es.ulpgc.bigdata.model.BookLocation;
import es.ulpgc.bigdata.model.RawBook;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * What the file-based datalakes (book, range, time) have in common.
 * Each subclass only decides WHERE each book goes; HOW it is written and WHAT
 * counts as a saved book is defined here, only once.
 *
 * Definition of "saved book" (contract §6.2, DR-06): its ready marker, its header and its
 * body exist as regular files with their final name. The marker is written LAST, so a book
 * whose save was interrupted (header without body, or both without marker) does not exist:
 * Indexing never reads it and Ingestion writes it again. A .tmp never counts.
 *
 * The marker holds one line, the time of the save in ISO-8601 UTC to the second
 * ("2026-11-04T10:15:00Z"), read by the recovery of the Control and by the metrics.
 */
public abstract class AbstractFileDatalake implements Datalake {

    /** Suffix of half-written files. */
    static final String TMP_SUFFIX = ".tmp";

    /** Id that save() could have written: "0" or no leading zeros, max. 9 digits. */
    private static final Pattern CANONICAL_ID = Pattern.compile("0|[1-9][0-9]{0,8}");

    protected final Path root;

    /** Time written in the ready marker; fixed in the tests. */
    private final Clock clock;

    protected AbstractFileDatalake(Path root, Clock clock) {
        this.root = Objects.requireNonNull(root, "root");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    // ------------------------------------------------------------------
    // Writing
    // ------------------------------------------------------------------

    /**
     * Saves a book in the paths given by the subclass: header, body and, LAST, the ready
     * marker. A book only "exists" when the three are in place, so a crash halfway leaves a
     * NOT saved book, which the next save writes again.
     *
     * A book that is already saved is never modified (§6.2): save writes nothing and returns
     * its location, so Indexing never reads a book while it is being rewritten. "Saved" means
     * complete: a book whose marker exists but whose body was lost is written again.
     *
     * Two saves of the same book at the same time are safe: each file is replaced atomically
     * with a complete version (writeAtomically), and both write the same book.
     */
    protected BookLocation writeBook(RawBook book, Path header, Path body, Path ready) {
        Objects.requireNonNull(book, "book");
        Objects.requireNonNull(book.header(), "header");
        Objects.requireNonNull(book.body(), "body");
        if (book.id() < 0) {
            throw new IllegalArgumentException("book_id negativo: " + book.id());
        }
        if (isCompleteBook(header, body, ready)) {
            return new BookLocation(book.id(), header, body);
        }
        try {
            writeAtomically(header, book.header());
            writeAtomically(body, book.body());
            writeAtomically(ready, clock.instant().truncatedTo(ChronoUnit.SECONDS) + "\n");
        } catch (IOException e) {
            throw new UncheckedIOException("No se pudo guardar el libro " + book.id(), e);
        }
        return new BookLocation(book.id(), header, body);
    }

    /**
     * Writes content to target without anyone ever being able to see a half-written target:
     * create directory -> write a temporary file -> move it to target.
     *
     * The temporary name is UNIQUE per call (target.&lt;uuid&gt;.tmp). With one fixed name, two
     * threads or processes saving the same book at once (a manual POST /ingest and a worker,
     * or a worker that lost its lease) would write into the same temporary file: one could
     * move the other's half-written file to target, and the other would then fail. With one
     * name each, every move is a complete file, and the last one wins.
     *
     * Not Files.createTempFile: it creates the file readable only by its owner (rw-------), and
     * Indexing reads the books Ingestion writes, from another container.
     */
    static void writeAtomically(Path target, String content) throws IOException {
        writeAtomically(target, content, Files::move);
    }

    /** writeAtomically with the given Mover (the tests simulate the AccessDeniedException of Windows). */
    static void writeAtomically(Path target, String content, Mover mover) throws IOException {
        Files.createDirectories(target.getParent());
        Path tmp = target.resolveSibling(target.getFileName() + "." + UUID.randomUUID() + TMP_SUFFIX);
        try {
            Files.writeString(tmp, content, StandardCharsets.UTF_8);
            moveReplacing(tmp, target, mover);
        } catch (IOException e) {
            Files.deleteIfExists(tmp);   // do not leave garbage if something fails
            throw e;
        }
    }

    /** Most attempts of moveReplacing; with a pause of 1, 2, 3… ms, about 1.2 s in all. */
    static final int MAX_MOVE_ATTEMPTS = 50;

    /** Moves a file: Files::move in production, a fake in the tests. */
    @FunctionalInterface
    interface Mover {
        void move(Path source, Path target, CopyOption... options) throws IOException;
    }

    /**
     * Moves tmp onto target, replacing it, atomically if the file system can.
     *
     * On Windows the move fails with AccessDeniedException while another thread is replacing or
     * reading the same target (two saves of the same book at once). That is momentary, so the
     * move is tried again after a short, growing pause, up to MAX_MOVE_ATTEMPTS times. On Linux
     * a rename never fails for that reason, so there an AccessDeniedException is a real lack of
     * permissions and only arrives a little later. A target that is a folder fails at once:
     * waiting will not change it. Any other IOException is not retried.
     */
    static void moveReplacing(Path tmp, Path target, Mover mover) throws IOException {
        for (int attempt = 1; ; attempt++) {
            try {
                try {
                    mover.move(tmp, target, ATOMIC_MOVE, REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException e) {
                    // The file system does not guarantee atomicity: we carry on,
                    // but the window in which target can be seen half-written is no longer zero.
                    mover.move(tmp, target, REPLACE_EXISTING);
                }
                return;
            } catch (AccessDeniedException e) {
                if (attempt >= MAX_MOVE_ATTEMPTS || Files.isDirectory(target)) {
                    throw e;
                }
                try {
                    Thread.sleep(attempt);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Reading
    // ------------------------------------------------------------------

    /**
     * The only definition of "saved book", used by save, locate and listBookIds. The marker
     * is checked first: it is the last file written, so without it the book is not there.
     */
    protected static boolean isCompleteBook(Path header, Path body, Path ready) {
        return Files.isRegularFile(ready) && Files.isRegularFile(header) && Files.isRegularFile(body);
    }

    protected static Optional<BookLocation> completeBook(int id, Path header, Path body, Path ready) {
        return isCompleteBook(header, body, ready)
                ? Optional.of(new BookLocation(id, header, body))
                : Optional.empty();
    }

    /** "1342" -> 1342 ; "0084", "abc", "-5", "99999999999" -> null. */
    protected static Integer parseCanonicalId(String text) {
        return CANONICAL_ID.matcher(text).matches() ? Integer.parseInt(text) : null;
    }

    /** ("1342._READY", "._READY") -> 1342 ; "1342._READY.tmp" -> null. */
    protected static Integer parseIdWithSuffix(String fileName, String suffix) {
        if (!fileName.endsWith(suffix)) {
            return null;
        }
        return parseCanonicalId(fileName.substring(0, fileName.length() - suffix.length()));
    }
}