package es.ulpgc.bigdata.datamart.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TokenizerTest {

    /** A subset of shared/stopwords.txt, enough for the tests. */
    private final Tokenizer tokenizer = new Tokenizer(Set.of("the", "and", "of", "in", "is"));

    private List<String> tokens(String text) {
        return tokenizer.tokenize(text);
    }

    // --- Challenge criteria --------------------------------------------------

    @Test
    void boatBoatDaDosTokensPeroUnSoloTermino() {
        assertEquals(List.of("boat", "boat"), tokenizer.tokenize("Boat, BOAT!"));
        assertEquals(Set.of("boat"), tokenizer.uniqueTerms("Boat, BOAT!"));
    }

    @Test
    void losAcentosYLaEnieSonSeparadores() {
        // é, ï, ñ are not A-Z/a-z/0-9: they cut the word in two.
        assertEquals(List.of("caf", "na", "ve", "se", "or"), tokens("café naïve señor"));
    }

    // --- The cases the mission asks for -------------------------------------

    @Test
    void mayusculasPasanAMinusculas() {
        assertEquals(List.of("hello", "world", "mixed"), tokens("Hello WORLD mIxEd"));
    }

    @Test
    void laPuntuacionSepara() {
        assertEquals(List.of("end", "start", "middle", "fin"), tokens("end.Start;middle,,,(fin)"));
    }

    @Test
    void losApostrofesSeparanYLoQueQuedaCortoSeDescarta() {
        // "don't" -> "don" + "t"; "Mary's" -> "mary" + "s"; "t" and "s" have length 1.
        assertEquals(List.of("don", "mary", "cat"), tokens("don't Mary's cat"));
    }

    @Test
    void elApostrofeTipograficoDeGutenbergTambienSepara() {
        assertEquals(List.of("don", "wait"), tokens("don\u2019t\u2014wait"));   // ’ and —
    }

    @Test
    void losNumerosSonTokensYSeMezclanConLetras() {
        // "2" has length 1 and is discarded; "3rd" and "abc123def" are a single token each.
        assertEquals(List.of("1813", "3rd", "abc123def"), tokens("1813, 2... 3rd abc123def"));
    }

    @Test
    void tokensDeUnCaracterSeDescartanYDeDosSeConservan() {
        assertEquals(List.of("ox", "yz"), tokens("a I x ox 7 yz"));
    }

    @Test
    void lasStopwordsSeDescartanTrasPasarAMinusculas() {
        assertEquals(List.of("cat", "hat"), tokens("The cat AND THE hat"));
    }

    // --- Edge cases ---------------------------------------------------------

    @Test
    void textoVacioOSoloSeparadoresNoDaTokens() {
        assertEquals(List.of(), tokens(""));
        assertEquals(List.of(), tokens("  ,.;!? \n\t—"));
    }

    @Test
    void elUltimoTokenSeEmiteAunqueNoHayaSeparadorFinal() {
        assertEquals(List.of("red", "boat"), tokens("red boat"));
    }

    @Test
    void losDigitosNoAsciiSonSeparadores() {
        // Character.isDigit('٣') is true; the contract only accepts 0-9.
        assertEquals(List.of(), tokens("\u0663\u0664"));
    }

    @Test
    void noDependeDelIdiomaDelOrdenador() {
        // In Turkish, "TITLE".toLowerCase() gives "tıtle" (dotless i).
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr"));
            assertEquals(List.of("title"), tokens("TITLE"));
        } finally {
            Locale.setDefault(original);
        }
    }

    // --- uniqueTerms --------------------------------------------------------

    @Test
    void uniqueTermsConservaElOrdenDePrimeraAparicion() {
        assertEquals(List.of("red", "boat", "sails"),
                List.copyOf(tokenizer.uniqueTerms("red boat RED sails boat")));
    }

    @Test
    void elEjercicioEnPapelDeLaGuia() {
        assertEquals(Set.of("red", "boat", "sails"), tokenizer.uniqueTerms("red boat sails"));
        assertEquals(Set.of("blue", "boat", "sails", "fast"), tokenizer.uniqueTerms("blue boat sails fast"));
        assertEquals(Set.of("red", "island"), tokenizer.uniqueTerms("red island"));
    }

    @Test
    void losResultadosNoSePuedenModificar() {
        assertThrows(UnsupportedOperationException.class, () -> tokenizer.tokenize("red boat").add("x"));
        assertThrows(UnsupportedOperationException.class, () -> tokenizer.uniqueTerms("red boat").add("x"));
    }

    // --- Stopwords file -----------------------------------------------------

    @TempDir Path tmp;

    @Test
    void cargaStopwordsIgnorandoComentariosLineasVaciasYEspacios() throws IOException {
        Path file = tmp.resolve("stopwords.txt");
        Files.writeString(file, "# Stopwords en inglés\nthe\n\n  and  \n# comentario\nof\n");

        Tokenizer t = Tokenizer.fromStopwordsFile(file);

        assertEquals(List.of("cat", "hat"), t.tokenize("The cat and the hat of"));
        assertEquals(List.of("comentario"), t.tokenize("# comentario"));   // it is only a comment in the file
    }

    // --- Stage 2: stopwords packaged in common ------------------------------

    @Test
    void standardUsaLasStopwordsEmpaquetadasEnCommon() {
        Tokenizer standard = Tokenizer.standard();

        // "the", "and", "of" are in shared/stopwords.txt; "do" is its last line.
        assertEquals(List.of("pride", "prejudice", "jane", "austen"),
                standard.tokenize("The Pride and Prejudice of Jane Austen, do"));
    }

    @Test
    void standardEsIgualQueCargarElFicheroCompartido() throws IOException {
        // Same list as stage_1/shared/stopwords.txt, line by line.
        Path file = tmp.resolve("stopwords.txt");
        try (var in = Tokenizer.class.getResourceAsStream(Tokenizer.STANDARD_STOPWORDS)) {
            Files.write(file, in.readAllBytes());
        }
        String text = "it is a truth universally acknowledged that a single man in possession";

        assertEquals(Tokenizer.fromStopwordsFile(file).tokenize(text), Tokenizer.standard().tokenize(text));
    }
}