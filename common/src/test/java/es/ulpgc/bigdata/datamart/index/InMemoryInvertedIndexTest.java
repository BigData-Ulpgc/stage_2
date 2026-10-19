package es.ulpgc.bigdata.datamart.index;

import es.ulpgc.bigdata.model.RawBook;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryInvertedIndexTest {

    /** The three books of the paper exercise (section 4 of the guide). */
    private static final List<RawBook> PAPER_BOOKS = List.of(
            new RawBook(10, "", "red boat sails"),
            new RawBook(20, "", "blue boat sails fast"),
            new RawBook(30, "", "red island"));

    private final Tokenizer tokenizer = new Tokenizer(Set.of());   // no stopwords: they do not matter here
    private InMemoryInvertedIndex index;

    @BeforeEach
    void setUp() {
        index = new InMemoryInvertedIndex();
    }

    private void indexBook(RawBook book) {
        index.addDocument(book.id(), tokenizer.uniqueTerms(book.body()));
    }

    private void indexPaperBooks() {
        PAPER_BOOKS.forEach(this::indexBook);
    }

    // --- Challenge criteria ------------------------------------------------

    @Test
    void coincideConElIndiceManualDelApendiceA() {
        indexPaperBooks();

        Map<String, List<Integer>> expected = new TreeMap<>(Map.of(
                "blue", List.of(20),
                "boat", List.of(10, 20),
                "fast", List.of(20),
                "island", List.of(30),
                "red", List.of(10, 30),
                "sails", List.of(10, 20)));

        Map<String, List<Integer>> actual = new TreeMap<>();
        for (String term : index.terms()) {
            actual.put(term, index.postings(term));
        }
        assertEquals(expected, actual);                  // not one term too many or too few
    }

    @Test
    void indexarDosVecesElMismoLibroNoDuplicaSuId() {
        indexPaperBooks();
        indexBook(PAPER_BOOKS.get(0));                   // 10 again

        assertEquals(List.of(10, 20), index.postings("boat"));
        assertEquals(List.of(10, 30), index.postings("red"));
    }

    @Test
    void terminoInexistenteDevuelveListaVacia() {
        indexPaperBooks();

        assertEquals(List.of(), index.postings("whale"));
    }

    // --- Details -----------------------------------------------------------

    @Test
    void lasPostingsSalenOrdenadasAunqueLosLibrosLleguenDesordenados() {
        indexBook(PAPER_BOOKS.get(2));                   // 30
        indexBook(PAPER_BOOKS.get(1));                   // 20
        indexBook(PAPER_BOOKS.get(0));                   // 10

        assertEquals(List.of(10, 20), index.postings("boat"));
        assertEquals(List.of(10, 30), index.postings("red"));
    }

    @Test
    void unaPalabraRepetidaEnElMismoLibroAportaElIdUnaSolaVez() {
        index.addDocument(10, tokenizer.uniqueTerms("Boat, BOAT! boat"));

        assertEquals(List.of(10), index.postings("boat"));
    }

    @Test
    void indiceVacio() {
        assertEquals(List.of(), index.postings("boat"));
        assertEquals(0, index.termCount());
        assertEquals(Set.of(), index.terms());
    }

    @Test
    void elTerminoDebeVenirTokenizado() {
        indexPaperBooks();

        assertEquals(List.of(), index.postings("Boat"));            // uppercase: not a term
        assertEquals(List.of(10, 20), index.postings("boat"));
    }

    @Test
    void loQueDevuelvePostingsNoModificaElIndice() {
        indexPaperBooks();
        List<Integer> boat = index.postings("boat");

        assertThrows(UnsupportedOperationException.class, () -> boat.add(99));
        assertEquals(List.of(10, 20), index.postings("boat"));
    }

    @Test
    void cuentaLosTerminosDistintos() {
        indexPaperBooks();

        assertEquals(6, index.termCount());
    }

    @Test
    void idNegativoSeRechaza() {
        assertThrows(IllegalArgumentException.class, () -> index.addDocument(-1, Set.of("boat")));
    }

    // --- The three queries of the paper exercise ---------------------------
    // There is no search yet (it arrives in challenge 20); here it is only checked that the
    // posting lists contain what is needed to solve them by hand.

    private List<Integer> andByHand(String a, String b) {
        return index.postings(a).stream().filter(index.postings(b)::contains).toList();
    }

    @Test
    void lasConsultasDelEjercicioSeResuelvenConLasPostings() {
        indexPaperBooks();

        assertEquals(List.of(10, 20), andByHand("boat", "sails"));
        assertEquals(List.of(30), andByHand("red", "island"));
        assertEquals(List.of(10), andByHand("red", "boat"));
    }
}