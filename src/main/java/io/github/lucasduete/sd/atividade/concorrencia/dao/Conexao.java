package io.github.lucasduete.sd.atividade.concorrencia.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Conexao {

    private static final String url = "jdbc:postgresql://localhost:5432/sd";
    private static final String user = "postgres";
    private static final String password = "postgres";

    public static Connection getConnection  () throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}
