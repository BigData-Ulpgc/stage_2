"""
Tests of experiment_rows.py (DR-13). From the repository root:

    python3 -m unittest discover -s benchmarks/indexing -v
"""
import contextlib
import csv
import io
import pathlib
import tempfile
import unittest

import experiment_rows as e


def rows(argv):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        e.main(argv)
    return list(csv.reader(out.getvalue().splitlines()))


def by_metric(table):
    return {r[9]: (float(r[10]), r[11]) for r in table if r[0] != "experiment"}


class ExperimentRowsTest(unittest.TestCase):

    def setUp(self):
        self.dir = pathlib.Path(tempfile.mkdtemp())
        # 5 s of start-up, then 10 books/s for the first half and 5 books/s for the second
        lines = ["seconds,books_indexed", "0.0,0", "5.0,0"]
        lines += [f"{5 + s}.0,{10 * s}" for s in range(1, 6)]
        lines += [f"{10 + 2 * s}.0,{50 + 10 * s}" for s in range(1, 6)]
        self.timeline = self.dir / "timeline.csv"
        self.timeline.write_text("\n".join(lines) + "\n")
        self.base = ["--experiment", "index_scale", "--threads", "4", "--size", "100", "--repetition", "2",
                     "--summary", "100,0,6.000,20.000,5.000,6.400", "--timeline", str(self.timeline)]

    def test_cada_fila_tiene_las_columnas_del_formato_comun(self):
        table = rows(self.base + ["--header"])
        self.assertEqual(e.HEADER, table[0])
        for r in table[1:]:
            self.assertEqual(len(e.HEADER), len(r))
            self.assertEqual(["index_scale", "indexing", "integration", "docker", "1", "1", "4", "100", "2"], r[:9])

    def test_el_ritmo_estable_el_total_y_los_segundos_por_libro(self):
        m = by_metric(rows(self.base))
        self.assertEqual((6.4, "books/s"), m["throughput"])
        self.assertEqual((5.0, "books/s"), m["throughput_total"])
        self.assertAlmostEqual(1 / 6.4, m["seconds_per_book"][0], places=5)
        self.assertEqual((6.0, "s"), m["startup"])
        self.assertEqual((20.0, "s"), m["duration"])
        self.assertEqual((0.0, "books"), m["failed"])

    def test_por_decimas_se_ve_que_el_ritmo_cae(self):
        m = by_metric(rows(self.base))
        self.assertAlmostEqual(10 / 6, m["throughput_pct_00_010"][0], places=5, msg="la primera décima lleva el arranque")
        self.assertAlmostEqual(10.0, m["throughput_pct_20_030"][0], places=5)
        self.assertAlmostEqual(5.0, m["throughput_pct_70_080"][0], places=5)
        self.assertEqual(10, len([k for k in m if k.startswith("throughput_pct_")]))

    def test_cpu_media_y_memoria_maxima_mientras_indexa(self):
        monitor = self.dir / "monitor" / "run_20261107T000000Z"
        monitor.mkdir(parents=True)
        header = "timestamp,container,cpu_pct,mem_used_mib,mem_limit_mib,mem_pct,net_rx_mb,net_tx_mb,block_read_mb,block_write_mb,pids"
        lines = [header]
        # started at 00:00:00, first book at 6 s, last at 20 s: only 00:00:06 .. 00:00:20 count
        for second, indexing, mongo in [(2, 300.0, 1.0), (8, 15.0, 90.0), (12, 13.0, 95.0), (25, 0.5, 2.0)]:
            t = f"2026-11-07T00:00:{second:02d}Z"
            lines.append(f"{t},stage_2-indexing-1,{indexing},400,1024,39,0,0,0,0,40")
            lines.append(f"{t},mongo,{mongo},{300 + second},1024,30,0,0,0,0,30")
            lines.append(f"{t},hz-1,3.0,500,768,65,0,0,0,0,50")
            lines.append(f"{t},hz-2,4.0,500,768,65,0,0,0,0,50")
        (monitor / "containers.csv").write_text("\n".join(lines) + "\n")
        started = e.parse_time("2026-11-07T00:00:00Z")
        m = by_metric(rows(self.base + ["--monitor", str(self.dir / "monitor"), "--started", str(started)]))
        self.assertEqual((14.0, "% of 1 CPU"), m["cpu_indexing"], "sin el arranque ni lo de después")
        self.assertEqual((92.5, "% of 1 CPU"), m["cpu_mongo"])
        self.assertEqual((7.0, "% of 1 CPU"), m["cpu_hazelcast"], "los dos miembros suman")
        self.assertEqual((312.0, "MiB"), m["mem_max_mongo"])

    def test_las_cifras_de_mongodb(self):
        stats = self.dir / "stats.csv"
        stats.write_text("\n".join([
            "metric,collection,term,value,unit",
            "documents,inverted_index,,6,documents",
            "data_size,inverted_index,,600,bytes",
            "storage_size,inverted_index,,4096,bytes",
            "index_size,inverted_index,,8192,bytes",
            "storage_size,books,,4096,bytes",
            "index_size,books,,4096,bytes",
            "terms,inverted_index,,6,terms",
            "postings,inverted_index,,17,ids",
            "mean_postings,inverted_index,,2.833,ids/term",
            "posting_length,inverted_index,the,6,ids",
            "posting_bytes,inverted_index,the,90,bytes",
            "read_median,inverted_index,the,0.400,ms",
            "read_max,inverted_index,the,1.200,ms",
            "posting_length,inverted_index,whale,4,ids",
        ]) + "\n")
        m = by_metric(rows(self.base + ["--stats", str(stats)]))
        self.assertEqual((6.0, "terms"), m["index_terms"])
        self.assertEqual((12288.0, "bytes"), m["index_disk"], "datos más índices")
        self.assertEqual((8192.0, "bytes"), m["books_disk"])
        self.assertEqual((0.0, "bytes"), m["index_log_disk"], "si no está, 0")
        self.assertEqual((6.0, "ids"), m["posting_longest_length"], "la primera es la más larga")
        self.assertEqual((0.4, "ms"), m["posting_longest_read_median"])

    def test_una_publicacion_da_su_latencia(self):
        m = by_metric(rows(["--experiment", "index_publish_10", "--threads", "4", "--size", "10000",
                            "--repetition", "3", "--publish-seconds", "0.084"]))
        self.assertEqual({"publish_latency": (84.0, "ms")}, m)

    def test_la_prueba_de_actualizacion_da_una_fila_por_operacion_y_longitud(self):
        probe = self.dir / "probe.csv"
        probe.write_text("operation,list_length,median_ms,max_ms\naddToSet,1000,4.950,6.100\n"
                         "push,1000,1.100,1.900\naddToSet,100000,226.450,240.000\n")
        table = rows(["--experiment", "index_posting_update", "--threads", "1", "--size", "0",
                      "--repetition", "1", "--probe", str(probe)])
        self.assertEqual([["1000", "addtoset_latency", "4.95", "ms"], ["1000", "push_latency", "1.1", "ms"],
                          ["100000", "addtoset_latency", "226.45", "ms"]],
                         [[r[7], r[9], r[10], r[11]] for r in table])

    def test_un_resumen_con_columnas_de_menos_se_rechaza(self):
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            rows(self.base[:-4] + ["--summary", "100,0,6.0", "--timeline", str(self.timeline)])

    def test_sin_ritmo_estable_no_hay_segundos_por_libro(self):
        argv = self.base[:]
        argv[argv.index("--summary") + 1] = "40,0,6.000,20.000,2.000,"
        m = by_metric(rows(argv))
        self.assertNotIn("throughput", m)
        self.assertNotIn("seconds_per_book", m)
        self.assertIn("throughput_total", m)


if __name__ == "__main__":
    unittest.main()
