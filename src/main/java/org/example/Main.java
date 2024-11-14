package org.example;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import java.io.FileReader;
import java.sql.*;

public class Main {

    private static final String URL = "jdbc:postgresql://localhost:5432/study";
    private static final String USER = "postgres";
    private static final String PASSWORD = "";

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            Statement stmt = conn.createStatement();

            createTables(stmt);

            addDataFromJson(conn, "books.json");

            listMusicCompositions(stmt);
            filterMusicCompositions(stmt);
            addFavoriteMusic(stmt);
            listBooksOrderedByYear(stmt);
            listBooksBefore2000(stmt);
            addPersonalInfoAndFavorites(stmt);

            deleteTables(stmt);

            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createTables(Statement stmt) throws SQLException {
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS study.visitors (" +
                "visitor_id SERIAL PRIMARY KEY," +
                "name TEXT NOT NULL," +
                "surname TEXT," +
                "phone TEXT," +
                "subscribed BOOLEAN" +
                ");");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS study.books (" +
                "book_id SERIAL PRIMARY KEY," +
                "title TEXT NOT NULL," +
                "author TEXT," +
                "publishing_year INT," +
                "isbn TEXT UNIQUE," +
                "publisher TEXT" +
                ");");
    }

    private static void addDataFromJson(Connection conn, String jsonFilePath) {
        try (JsonReader reader = new JsonReader(new FileReader(jsonFilePath))) {
            JsonArray visitorsArray = JsonParser.parseReader(reader).getAsJsonArray();
            String insertVisitorSQL = "INSERT INTO study.visitors (name, surname, phone, subscribed) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING";
            String insertBookSQL = "INSERT INTO study.books (title, author, publishing_year, isbn, publisher) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING";

            for (int i = 0; i < visitorsArray.size(); i++) {
                JsonObject visitor = visitorsArray.get(i).getAsJsonObject();
                try (PreparedStatement visitorStmt = conn.prepareStatement(insertVisitorSQL, Statement.RETURN_GENERATED_KEYS);
                     PreparedStatement bookStmt = conn.prepareStatement(insertBookSQL)) {

                    visitorStmt.setString(1, visitor.get("name").getAsString());
                    visitorStmt.setString(2, visitor.get("surname").getAsString());
                    visitorStmt.setString(3, visitor.get("phone").getAsString());
                    visitorStmt.setBoolean(4, visitor.get("subscribed").getAsBoolean());
                    visitorStmt.executeUpdate();

                    JsonArray favoriteBooks = visitor.getAsJsonArray("favoriteBooks");
                    for (int j = 0; j < favoriteBooks.size(); j++) {
                        JsonObject book = favoriteBooks.get(j).getAsJsonObject();
                        bookStmt.setString(1, book.get("name").getAsString());
                        bookStmt.setString(2, book.get("author").getAsString());
                        bookStmt.setInt(3, book.get("publishingYear").getAsInt());
                        bookStmt.setString(4, book.get("isbn").getAsString());
                        bookStmt.setString(5, book.get("publisher").getAsString());
                        bookStmt.executeUpdate();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void listMusicCompositions(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM study.music");
        while (rs.next()) {
            System.out.println("ID: " + rs.getInt("id") + ", Name: " + rs.getString("name"));
        }
    }

    private static void filterMusicCompositions(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM study.music WHERE LOWER(name) NOT LIKE '%m%' AND LOWER(name) NOT LIKE '%t%'");
        while (rs.next()) {
            System.out.println("Filtered Song: " + rs.getString("name"));
        }
    }

    private static void addFavoriteMusic(Statement stmt) throws SQLException {
        stmt.executeUpdate("INSERT INTO study.music (id, name) VALUES (21, 'My Favorite Song') ON CONFLICT (id) DO NOTHING");
    }

    private static void listBooksOrderedByYear(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM study.books ORDER BY publishing_year");
        while (rs.next()) {
            System.out.println("Book: " + rs.getString("title") + ", Year: " + rs.getInt("publishing_year"));
        }
    }

    private static void listBooksBefore2000(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT * FROM study.books WHERE publishing_year < 2000");
        while (rs.next()) {
            System.out.println("Book before 2000: " + rs.getString("title"));
        }
    }

    private static void addPersonalInfoAndFavorites(Statement stmt) throws SQLException {
        stmt.executeUpdate("INSERT INTO study.visitors (name, surname, phone, subscribed) VALUES ('Egor', 'Ahhhhh', '000111000', true) ON CONFLICT DO NOTHING");
        stmt.executeUpdate("INSERT INTO study.books (title, author, publishing_year, isbn, publisher) VALUES ('Fav book', 'Fav author', 2022, '111111111', 'Fav publisher') ON CONFLICT (isbn) DO NOTHING");

        ResultSet rsVisitor = stmt.executeQuery("SELECT * FROM study.visitors WHERE name = 'Egor'");
        while (rsVisitor.next()) {
            System.out.println("Visitor: " + rsVisitor.getString("name") + " " + rsVisitor.getString("surname"));
        }

        ResultSet rsBook = stmt.executeQuery("SELECT * FROM study.books WHERE title = 'Fav book'");
        while (rsBook.next()) {
            System.out.println("Favorite Book: " + rsBook.getString("title"));
        }
    }

    private static void deleteTables(Statement stmt) throws SQLException {
        stmt.executeUpdate("DROP TABLE IF EXISTS study.visitors");
        stmt.executeUpdate("DROP TABLE IF EXISTS study.books");
    }
}