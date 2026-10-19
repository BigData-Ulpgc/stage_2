package es.ulpgc.bigdata.datamart.index;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Tokenizer of the common contract (shared/SPEC.md, section 5), identical in Java, Python and C:
 *
 *  1. The text is traversed character by character.
 *  2. A-Z becomes a-z; a-z and 0-9 are part of the token.
 *  3. Any other character (space, punctuation, apostrophe, non-ASCII) is a separator.
 *  4. Tokens of length < 2 are discarded.
 *  5. Stopwords are discarded.
 *
 * It does not use toLowerCase() or Character.isLetter(): both follow Unicode rules
 * that C cannot reproduce byte by byte.
 *
 * Stage 2: it lives in common because Indexing and Search must tokenize EXACTLY the same;
 * otherwise a query term would never match the term stored in the index. Immutable, so one
 * instance can be shared by every thread.
 */
public class Tokenizer {

    static final int MIN_LENGTH = 2;

    private final Set<String> stopwords;

    public Tokenizer(Set<String> stopwords) {
        this.stopwords = Set.copyOf(Objects.requireNonNull(stopwords, "stopwords"));
    }

    /**
     * Loads shared/stopwords.txt: one per line; empty lines and those
     * starting with '#' are ignored.
     */
    public static Tokenizer fromStopwordsFile(Path file) {
        try {
            return new Tokenizer(parseStopwords(Files.readAllLines(file, StandardCharsets.UTF_8)));
        } catch (IOException e) {
            throw new UncheckedIOException("No se pudieron leer las stopwords de " + file, e);
        }
    }

    /** Copy of stage_1/shared/stopwords.txt packaged inside the common jar. */
    static final String STANDARD_STOPWORDS = "/stopwords.txt";

    /**
     * The tokenizer every Stage 2 service uses (Indexing to build the index, Search to parse
     * the query). The stopwords travel inside the common jar instead of being a file each
     * service reads from its own path: two services cannot end up with different lists.
     */
    public static Tokenizer standard() {
        try (InputStream in = Tokenizer.class.getResourceAsStream(STANDARD_STOPWORDS)) {
            if (in == null) {
                throw new IllegalStateException(STANDARD_STOPWORDS + " no está en el classpath");
            }
            String content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return new Tokenizer(parseStopwords(content.lines().toList()));
        } catch (IOException e) {
            throw new UncheckedIOException("No se pudieron leer las stopwords " + STANDARD_STOPWORDS, e);
        }
    }

    private static Set<String> parseStopwords(List<String> lines) {
        Set<String> words = new LinkedHashSet<>();
        for (String line : lines) {
            String word = line.strip();
            if (!word.isEmpty() && !word.startsWith("#")) {
                words.add(word);
            }
        }
        return words;
    }

    /** All the tokens, in order and with repetitions: "Boat, BOAT!" -> [boat, boat]. */
    public List<String> tokenize(String text) {
        List<String> tokens = new ArrayList<>();
        forEachToken(text, tokens::add);
        return Collections.unmodifiableList(tokens);
    }

    /**
     * Each term only once, in the order in which it first appears:
     * "Boat, BOAT!" -> [boat]. It is what each book contributes to the index.
     */
    public Set<String> uniqueTerms(String text) {
        Set<String> terms = new LinkedHashSet<>();
        forEachToken(text, terms::add);
        return Collections.unmodifiableSet(terms);   // Set.copyOf would lose the order
    }

    /** The tokenizer's only loop; tokenize and uniqueTerms only change where it is stored. */
    private void forEachToken(String text, Consumer<String> sink) {
        Objects.requireNonNull(text, "text");
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c >= 'A' && c <= 'Z') {
                current.append((char) (c + ('a' - 'A')));     // ASCII uppercase -> lowercase
            } else if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
                current.append(c);
            } else {
                emit(current, sink);                          // separator: closes the token
            }
        }
        emit(current, sink);                                  // the text may end in the middle of a token
    }

    /** Emits the accumulated token if it passes the filters, and empties the accumulator. */
    private void emit(StringBuilder current, Consumer<String> sink) {
        if (current.length() >= MIN_LENGTH) {
            String token = current.toString();
            if (!stopwords.contains(token)) {
                sink.accept(token);
            }
        }
        current.setLength(0);
    }
}