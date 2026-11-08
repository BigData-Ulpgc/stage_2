package es.ulpgc.bigdata.control;

import es.ulpgc.bigdata.model.BookIds;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

/**
 * Reads a list of book ids to seed: shared/book_ids.txt and the book_ids.txt of the sample
 * dataset. One id per line in canonical form (BookIds); blank lines and lines starting with #
 * are skipped (both files start with a # comment).
 */
public final class BookIdsFile {

    private BookIdsFile() {
    }

    /**
     * @return the ids in file order (duplicates kept: seeding ignores them)
     * @throws IllegalArgumentException naming the line, if a line is not a book id: seeding a
     *         half-read file would hide the problem
     */
    public static List<Integer> read(Path file) throws IOException {
        List<Integer> ids = new ArrayList<>();
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (int n = 0; n < lines.size(); n++) {
            String line = lines.get(n).strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            OptionalInt id = BookIds.parse(line);
            if (id.isEmpty()) {
                throw new IllegalArgumentException(file + ", line " + (n + 1) + ": '" + line + "' is not a book id");
            }
            ids.add(id.getAsInt());
        }
        return ids;
    }
}
