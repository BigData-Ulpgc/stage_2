"""
Tests of the synthetic dataset (DR-16). From the repository root:

    python3 -m unittest discover -s scripts/dataset -v

They check that a book depends only on (seed, id), that the files have the format the services
read (the regular expressions of BookSplitter and MetadataParser), and that the dataset keeps
the measured properties of the real books: the Zipf vocabulary, the stopwords, the lengths and
the mix of headers.
"""

import argparse
import contextlib
import hashlib
import io
import json
import pathlib
import random
import re
import statistics
import tempfile
import unittest

import build_vocabulary
import generate_dataset as g

REPO = pathlib.Path(__file__).resolve().parents[2]
STOPWORDS = {line.strip() for line in (REPO / "common/src/main/resources/stopwords.txt").read_text().splitlines()
             if line.strip() and not line.startswith("#")}

# The digest of the first 20 books with the defaults. If it changes, the books are no longer the
# ones the group generated: change GENERATOR (v2...) and this digest on purpose, in the same commit.
REFERENCE_DIGEST_20 = "3408332390cae4d9241d04514674308eb8d2bb40270dfd479568c46c90fee011"

# The same expressions as the Java code (BookSplitter, MetadataParser).
START = re.compile(r"^\*\*\* START OF (THE|THIS) PROJECT GUTENBERG EBOOK.*$", re.MULTILINE)
END = re.compile(r"^\*\*\* END OF (THE|THIS) PROJECT GUTENBERG EBOOK.*$", re.MULTILINE)
TITLE = re.compile(r"^Title:\s*(.+)$", re.MULTILINE)
AUTHOR = re.compile(r"^Author:\s*(.+)$", re.MULTILINE)
CREATOR = re.compile(r"^Creator:\s*(.+)$", re.MULTILINE)
RELEASE = re.compile(r"^Release date:\s*(.+?)(\s*\[.*)?$", re.MULTILINE)
LANGUAGE = re.compile(r"^Language:\s*(.+)$", re.MULTILINE)
ORIGINAL = re.compile(r"^Original publication:\s*(.+)$", re.MULTILINE)

REAL_WORDS_15_BOOKS = 30_442          # distinct words of the 15 real books (vocabulary.txt)
REAL_TOKENS_15_BOOKS = 1_137_230
REAL_STOPWORD_SHARE = 0.42


def options(out, count, **overrides):
    values = dict(out=pathlib.Path(out), count=count, seed=g.DEFAULT_SEED, first_id=g.DEFAULT_FIRST_ID,
                  median_words=g.DEFAULT_MEDIAN_WORDS, sigma=g.DEFAULT_SIGMA,
                  head_ranks=g.DEFAULT_HEAD_RANKS, zipf_tail=g.DEFAULT_ZIPF_TAIL, vocabulary_size=g.DEFAULT_VOCABULARY_SIZE, workers=2)
    values.update(overrides)
    return argparse.Namespace(**values)


def quietly(function, *args):
    with contextlib.redirect_stderr(io.StringIO()):
        return function(*args)


def text_of(path):
    # newline="" keeps the CRLF, as LocalFileSource reads the file
    return path.read_text(encoding="utf-8", newline="") if hasattr(path, "read_text") else path


class LibroTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sampler = g.Sampler(g.DEFAULT_VOCABULARY_SIZE, g.DEFAULT_HEAD_RANKS, g.DEFAULT_ZIPF_TAIL)

    def book(self, book_id=g.DEFAULT_FIRST_ID, seed=g.DEFAULT_SEED, median=g.DEFAULT_MEDIAN_WORDS):
        return g.make_book(self.sampler, seed, book_id, median, g.DEFAULT_SIGMA)

    def test_un_libro_depende_solo_de_la_semilla_y_del_id(self):
        self.assertEqual(self.book(), self.book())
        self.assertNotEqual(self.book(), self.book(seed=7))
        self.assertNotEqual(self.book(), self.book(book_id=g.DEFAULT_FIRST_ID + 1))

    def test_tiene_el_formato_de_un_fichero_crudo_de_gutenberg(self):
        for book_id in range(g.DEFAULT_FIRST_ID, g.DEFAULT_FIRST_ID + 30):
            text = self.book(book_id)
            self.assertNotIn("\n", text.replace("\r\n", ""), "solo finales CRLF")
            start, end = START.search(text), END.search(text.replace("\r", ""))
            self.assertIsNotNone(start)
            self.assertIsNotNone(end)
            header = text[:start.start()].replace("\r", "")
            body = text.replace("\r", "")[start.end():end.start()].strip()
            self.assertTrue(TITLE.search(header))
            release = RELEASE.search(header)
            self.assertIn(f"[eBook #{book_id}]", release.group(0))
            self.assertRegex(release.group(1), r"^[A-Z][a-z]+ \d{1,2}, \d{4}$")
            self.assertTrue(LANGUAGE.search(header))
            self.assertIn("synthetic", header)
            self.assertTrue(all(len(line) <= g.LINE_WIDTH for line in body.splitlines()))
            self.assertGreater(len(body), 1000)

    def test_las_cabeceras_siguen_la_mezcla_de_las_200_reales(self):
        n = 4000
        headers = ["\n".join(g.header_lines(g.book_rng(1, i), self.sampler, i, 1)[1]) for i in range(n)]
        original = sum(bool(ORIGINAL.search(h)) for h in headers) / n
        no_author = sum(not AUTHOR.search(h) for h in headers) / n
        creator = sum(bool(CREATOR.search(h)) for h in headers) / n
        other = sum(LANGUAGE.search(h).group(1) != "English" for h in headers) / n
        self.assertAlmostEqual(original, g.P_ORIGINAL_PUBLICATION, delta=0.012)
        self.assertAlmostEqual(no_author, g.P_NO_AUTHOR, delta=0.007)
        self.assertLess(creator, no_author)
        self.assertAlmostEqual(other, g.P_OTHER_LANGUAGE, delta=0.02)
        for h in headers:
            if ORIGINAL.search(h):       # as the real ones: released 2022-2025
                self.assertRegex(RELEASE.search(h).group(1), r"202[2-5]$")

    def test_las_longitudes_tienen_la_mediana_y_los_limites_pedidos(self):
        rng = random.Random(3)
        lengths = [g.length_of(rng, 10_000, g.DEFAULT_SIGMA) for _ in range(20_000)]
        self.assertAlmostEqual(statistics.median(lengths), 10_000, delta=400)
        self.assertEqual(500, min(lengths))
        self.assertEqual(200_000, max(lengths))

    def test_el_vocabulario_crece_como_el_de_los_libros_reales(self):
        rng = random.Random(5)
        words = self.sampler.draw(rng, REAL_TOKENS_15_BOOKS)
        distinct = len(set(words))
        self.assertAlmostEqual(distinct, REAL_WORDS_15_BOOKS, delta=REAL_WORDS_15_BOOKS * 0.08)
        more = len(set(words + self.sampler.draw(rng, 3 * REAL_TOKENS_15_BOOKS)))
        self.assertAlmostEqual(more / distinct, 2.0, delta=0.25, msg="Heaps: 4 veces el texto, 2 veces el vocabulario")
        share = sum(w in STOPWORDS for w in words) / len(words)
        self.assertAlmostEqual(share, REAL_STOPWORD_SHARE, delta=0.05)

    def test_las_pseudopalabras_son_tokens_nuevos_y_distintos(self):
        real = set(g.real_words())
        tail = self.sampler.words[len(real):len(real) + 50_000]
        self.assertEqual(len(tail), len(set(tail)))
        self.assertFalse(real & set(tail))
        self.assertTrue(all(re.fullmatch(r"[a-z]{4,}", w) for w in tail))
        self.assertEqual(g.DEFAULT_VOCABULARY_SIZE, len(self.sampler.words))

    def test_el_nucleo_usa_la_frecuencia_real_y_la_cola_empalma_con_pendiente_2(self):
        counts = [count for _, count in g.real_entries()]
        cumulative = g.cumulative_weights(20_000, counts, 3_000, 2.0)
        weight = lambda r: cumulative[0] if r == 1 else cumulative[r - 1] - cumulative[r - 2]
        self.assertEqual([counts[0], counts[1], counts[2999]], [weight(1), weight(2), weight(3_000)])
        self.assertAlmostEqual(weight(3_001) / weight(3_000), (3_000 / 3_001) ** 2, delta=1e-9)
        self.assertAlmostEqual(weight(20_000) / weight(10_000), (10_000 / 20_000) ** 2, delta=1e-9)


class DatasetTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = pathlib.Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_la_huella_de_los_20_primeros_libros_no_cambia(self):
        manifest = quietly(g.generate, options(self.dir / "a", 20))
        self.assertEqual(REFERENCE_DIGEST_20, manifest["digest"])

    def test_el_dataset_pequeno_es_el_principio_del_grande_con_cualquier_numero_de_procesos(self):
        quietly(g.generate, options(self.dir / "small", 6, workers=1))
        quietly(g.generate, options(self.dir / "big", 15, workers=3))
        small = (self.dir / "small/SHA256SUMS").read_text().splitlines()
        big = (self.dir / "big/SHA256SUMS").read_text().splitlines()
        self.assertEqual(small, big[:6])
        self.assertEqual(15, len(big))

    def test_escribe_los_ficheros_que_leen_ingestion_y_el_control(self):
        out = self.dir / "a"
        quietly(g.generate, options(out, 5, first_id=2_000_000))
        names = sorted(p.name for p in out.iterdir())
        self.assertEqual(["SHA256SUMS", "book_ids.txt", "manifest.json"]
                         + [f"pg{i}.txt" for i in range(2_000_000, 2_000_005)], names)
        ids = [line for line in (out / "book_ids.txt").read_text().splitlines() if not line.startswith("#")]
        self.assertEqual([str(i) for i in range(2_000_000, 2_000_005)], ids)
        manifest = json.loads((out / "manifest.json").read_text())
        self.assertEqual(sum((out / f"pg{i}.txt").stat().st_size for i in range(2_000_000, 2_000_005)),
                         manifest["bytes"])

    def test_verify_detecta_un_libro_cambiado_o_que_falta(self):
        out = self.dir / "a"
        quietly(g.generate, options(out, 4))
        self.assertTrue(quietly(g.verify, out))
        book = out / f"pg{g.DEFAULT_FIRST_ID + 1}.txt"
        book.write_bytes(book.read_bytes().replace(b"Title:", b"Title: x", 1))
        self.assertFalse(quietly(g.verify, out))
        book.unlink()
        self.assertFalse(quietly(g.verify, out))

    def test_verify_detecta_un_libro_cambiado_aunque_tambien_cambien_su_suma(self):
        out = self.dir / "a"
        quietly(g.generate, options(out, 4))
        name = f"pg{g.DEFAULT_FIRST_ID + 2}.txt"
        book = out / name
        old = hashlib.sha256(book.read_bytes()).hexdigest()
        book.write_bytes(book.read_bytes() + b"x")
        sums = out / "SHA256SUMS"
        sums.write_text(sums.read_text().replace(old, hashlib.sha256(book.read_bytes()).hexdigest()))
        self.assertFalse(quietly(g.verify, out), "solo la huella de manifest.json lo delata")

    def test_no_mezcla_dos_datasets_en_la_misma_carpeta(self):
        out = self.dir / "a"
        quietly(g.generate, options(out, 3))
        quietly(g.generate, options(out, 5))            # same parameters: more books, fine
        with self.assertRaises(SystemExit):
            quietly(g.generate, options(out, 3, seed=7))
        foreign = self.dir / "b"
        foreign.mkdir()
        (foreign / "pg1342.txt").write_text("Pride and Prejudice")
        with self.assertRaises(SystemExit):
            quietly(g.generate, options(foreign, 3))

    def test_ids_fuera_de_bookids_no_se_generan(self):
        with self.assertRaises(SystemExit):
            quietly(g.main, ["--count", "2", "--first-id", "999999999", "--out", str(self.dir / "a")])


class VocabularioTest(unittest.TestCase):

    def test_tokeniza_como_el_tokenizer_de_common(self):
        self.assertEqual(["don", "stop", "me", "1814", "caf", "s1"],
                         build_vocabulary.tokens("Don't STOP-me, 1814! café a s1"))

    def test_ordena_por_frecuencia_y_los_empates_alfabeticamente(self):
        with tempfile.TemporaryDirectory() as tmp:
            sample = pathlib.Path(tmp)
            # First seen: gamma, beta, alpha; the ties must still come out alphabetically
            for book_id, body in [(1, "gamma beta beta"), (2, "alpha gamma alpha delta")]:
                (sample / "book" / str(book_id)).mkdir(parents=True)
                (sample / "book" / str(book_id) / "body.txt").write_text(body)
            out = sample / "vocabulary.txt"
            quietly(build_vocabulary.main, [str(sample), "--out", str(out)])
            lines = [line for line in out.read_text().splitlines() if not line.startswith("#")]
            self.assertEqual(["alpha\t2", "beta\t2", "gamma\t2", "delta\t1"], lines)
            self.assertEqual(["alpha", "beta", "gamma", "delta"], g.real_words(out))


if __name__ == "__main__":
    unittest.main()
