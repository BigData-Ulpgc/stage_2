package es.ulpgc.bigdata.ingestion.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Objects;

/**
 * The offline counterpart of GutenbergClient: reads the raw files from a local folder, named
 * like the last part of the download URL, {@code <dir>/pg<id>.txt} (stage_1/sample_dataset/raw,
 * and the synthetic dataset of DR-16, which has the same format).
 *
 * It returns the same text the URL would serve, untouched (\r\n included), so everything after
 * the fetch runs exactly as it does online. That makes ingestion benchmarks independent of the
 * network and repeatable, and spares Gutenberg 100,000 downloads.
 *
 * A missing file is BookNotFoundException (like a 404); any other read error is
 * SourceUnavailableException (like a network failure). Migrated from Stage 1, where both were
 * an empty Optional / an unchecked exception.
 */
public final class LocalFileSource implements BookSource {

    private final Path dir;

    /**
     * @throws IllegalArgumentException if {@code dir} is not a directory, or has no
     *         {@code pg<id>.txt} file at all. Checked here, at startup: a wrong folder would
     *         otherwise make every book "not found", a permanent error, and send them all to
     *         FAILED. The empty case matters in Docker: a bind mount of a host folder that does
     *         not exist creates it, empty, instead of failing.
     */
    public LocalFileSource(Path dir) {
        this.dir = Objects.requireNonNull(dir, "dir");
        if (!Files.isDirectory(dir)) {
            throw new IllegalArgumentException("local book folder " + dir.toAbsolutePath() + " is not a directory");
        }
        if (!hasAnyBook(dir)) {
            throw new IllegalArgumentException("local book folder " + dir.toAbsolutePath()
                    + " has no pg<id>.txt file (empty, or the wrong folder)");
        }
    }

    /** Stops at the first pg<id>.txt: cheap also with 100,000 books. */
    private static boolean hasAnyBook(Path dir) {
        try (DirectoryStream<Path> books = Files.newDirectoryStream(dir, "pg*.txt")) {
            for (Path book : books) {
                if (book.getFileName().toString().matches("pg\\d+\\.txt")) {
                    return true;
                }
            }
            return false;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot list the local book folder " + dir.toAbsolutePath(), e);
        }
    }

    @Override
    public String fetch(int bookId) throws BookNotFoundException, SourceUnavailableException {
        Path file = dir.resolve("pg" + bookId + ".txt");
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (NoSuchFileException e) {
            throw new BookNotFoundException(bookId, "no file " + file);
        } catch (IOException e) {
            throw new SourceUnavailableException("cannot read " + file + ": " + e, e);
        }
    }

    @Override
    public String describe() {
        return "local " + dir.toAbsolutePath();
    }
}
