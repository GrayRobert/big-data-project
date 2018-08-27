package com.dbs.utils;

import com.dbs.Constants;

import java.sql.*;

public class SqlLiteHelper {

    private static String url = "jdbc:sqlite:" + Constants.SQL_LITE_DATABASE;

    public static void createSummaryTable() throws SQLException {

        String createSummaryTable = "CREATE TABLE IF NOT EXISTS summary_word_counts (\n"
                + "	id integer PRIMARY KEY,\n"
                + "	genre text,\n"
                + "	year text,\n"
                + "	word text NOT NULL,\n"
                + "	count integer NOT NULL\n"
                + ");";

        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database has been created.");
                Statement stmt = conn.createStatement();
                stmt.execute(createSummaryTable);
                System.out.println("Created summary_word_counts_table");

                conn.close();
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }


    }

    public static void createProfanityTable() throws SQLException {

        String createProfanityTable = "CREATE TABLE IF NOT EXISTS profanity_summary (\n"
                + "	id integer PRIMARY KEY,\n"
                + "	genre text,\n"
                + "	year text,\n"
                + "	count integer NOT NULL\n"
                + ");";

        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database has been created.");
                Statement stmt = conn.createStatement();
                stmt.execute(createProfanityTable);
                System.out.println("Created profanity_summary_table");

                conn.close();
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }


    }

    public static void dropSummaryTable() throws SQLException {

        String dropSummaryTable = "DROP TABLE IF EXISTS summary_word_counts;";;

        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                Statement stmt = conn.createStatement();
                stmt.execute(dropSummaryTable);
                System.out.println("Dropped summary_word_counts and profanity_summary tables");
                conn.close();
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }


    }

    public static void dropProfantiySummaryTable() throws SQLException {

        String dropProfanityTable = "DROP TABLE IF EXISTS profanity_summary;";

        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                Statement stmt = conn.createStatement();
                stmt.execute(dropProfanityTable);
                System.out.println("Dropped profanity_summary tables");
                conn.close();
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }


    }

    public static void insertData(Integer id, String genre, String year, String word, Integer count) throws SQLException {

        String sql = "INSERT INTO summary_word_counts(id,genre,year,word,count) VALUES(?,?,?,?,?)";

        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                PreparedStatement pstmt = conn.prepareStatement(sql);
                pstmt.setInt(1, id);
                pstmt.setString(2, genre);
                pstmt.setString(3, year);
                pstmt.setString(4, word);
                pstmt.setInt(5, count);
                pstmt.executeUpdate();
                conn.close();
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }

    public static void insertProfanityData(Integer id, String genre, String year, Integer count) throws SQLException {

        String sql = "INSERT INTO profanity_summary(id,genre,year,count) VALUES(?,?,?,?)";

        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                PreparedStatement pstmt = conn.prepareStatement(sql);
                pstmt.setInt(1, id);
                pstmt.setString(2, genre);
                pstmt.setString(3, year);
                pstmt.setInt(4, count);
                pstmt.executeUpdate();
                conn.close();
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }
}
