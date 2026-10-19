package es.ulpgc.bigdata.datamart.metadata;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Schema of the metadata database (shared/SPEC.md): books table + two indexes.
 * Everything uses IF NOT EXISTS, so it can be run every time the database is opened.
 */
public final class SqliteSchema {

    static final String CREATE_BOOKS_TABLE = """
            CREATE TABLE IF NOT EXISTS books (
                book_id      INTEGER PRIMARY KEY,
                title        TEXT,
                author       TEXT,
                language     TEXT,
                release_date TEXT,
                body_path    TEXT,
                header_path  TEXT
            )""";

    static final String CREATE_AUTHOR_INDEX =
            "CREATE INDEX IF NOT EXISTS idx_books_author ON books(author)";

    static final String CREATE_TITLE_INDEX =
            "CREATE INDEX IF NOT EXISTS idx_books_title ON books(title)";

    private SqliteSchema() {
    }

    /** JDBC URL for a database file. */
    public static String jdbcUrl(Path dbFile) {
        return "jdbc:sqlite:" + dbFile.toAbsolutePath();
    }

    /** Creates the table and indexes if they do not exist yet. */
    public static void create(Connection connection) throws SQLException {
        try (Statement st = connection.createStatement()) {
            st.execute(CREATE_BOOKS_TABLE);
            st.execute(CREATE_AUTHOR_INDEX);
            st.execute(CREATE_TITLE_INDEX);
        }
    }
}