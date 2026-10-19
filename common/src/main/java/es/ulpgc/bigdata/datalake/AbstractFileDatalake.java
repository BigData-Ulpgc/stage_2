package es.ulpgc.bigdata.datalake;

import es.ulpgc.bigdata.model.BookLocation;
import es.ulpgc.bigdata.model.RawBook;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * What the file-based datalakes (book, range, time) have in common.
 * Each subclass only decides WHERE each book goes; HOW it is written and WHAT
 * counts as a saved book is defined here, only once.
 *
 * Definition of "saved book": header and body exist as regular
 * files with their final name. A .tmp never counts.
 */
public abstract class AbstractFileDatalake implements Datalake {

    /** Suffix of half-written files. */
    static final String TMP_SUFFIX = ".tmp";

    /** Id that save() could have written: "0" or no leading zeros, max. 9 digits. */
    private static final Pattern CANONICAL_ID = Pattern.compile("0|[1-9][0-9]{0,8}");

    protected final Path root;

    protected AbstractFileDatalake(Path root) {
        this.root = Objects.requireNonNull(root, "root");
    }

    // ------------------------------------------------------------------
    // Writing
    // ------------------------------------------------------------------

    /**
     * Saves a book in the paths given by the subclass.
     * Header first and body afterwards: a book only "exists" when both
     * are in place, so a crash halfway leaves a NOT saved book.
     */
    protected BookLocation writeBook(RawBook book, Path header, Path body) {
        Objects.requireNonNull(book, "book");
        Objects.requireNonNull(book.header(), "header");
        Objects.requireNonNull(book.body(), "body");
        if (book.id() < 0) {
            throw new IllegalArgumentException("book_id negativo: " + book.id());
        }
        try {
            writeAtomically(header, book.header());
            writeAtomically(body, book.body());
        } catch (IOException e) {
            throw new UncheckedIOException("No se pudo guardar el libro " + book.id(), e);
        }
        return new BookLocation(book.id(), header, body);
    }

    /**
     * Writes content to target without anyone ever being able to see a half-written target:
     * create directory -> write target.tmp -> move target.tmp to target.
     */
    static void writeAtomically(Path target, String content) throws IOException {
        Files.createDirectories(target.getParent());
        Path tmp = target.resolveSibling(target.getFileName() + TMP_SUFFIX);
        try {
            Files.writeString(tmp, content, StandardCharsets.UTF_8);
            try {
                Files.move(tmp, target, ATOMIC_MOVE, REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                // The file system does not guarantee atomicity: we carry on,
                // but the window in which target can be seen half-written is no longer zero.
                Files.move(tmp, target, REPLACE_EXISTING);
            }
        } catch (IOException e) {
            Files.deleteIfExists(tmp);   // do not leave garbage if something fails
            throw e;
        }
    }

    // ------------------------------------------------------------------
    // Reading
    // ------------------------------------------------------------------

    /** The only definition of "saved book", used by locate and listBookIds. */
    protected static boolean isCompleteBook(Path header, Path body) {
        return Files.isRegularFile(header) && Files.isRegularFile(body);
    }

    protected static Optional<BookLocation> completeBook(int id, Path header, Path body) {
        return isCompleteBook(header, body)
                ? Optional.of(new BookLocation(id, header, body))
                : Optional.empty();
    }

    /** "1342" -> 1342 ; "0084", "abc", "-5", "99999999999" -> null. */
    protected static Integer parseCanonicalId(String text) {
        return CANONICAL_ID.matcher(text).matches() ? Integer.parseInt(text) : null;
    }

    /** ("1342.body.txt", ".body.txt") -> 1342 ; "1342.body.txt.tmp" -> null. */
    protected static Integer parseIdWithSuffix(String fileName, String suffix) {
        if (!fileName.endsWith(suffix)) {
            return null;
        }
        return parseCanonicalId(fileName.substring(0, fileName.length() - suffix.length()));
    }
}