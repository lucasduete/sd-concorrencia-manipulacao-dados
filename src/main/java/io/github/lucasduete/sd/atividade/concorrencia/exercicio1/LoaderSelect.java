package io.github.lucasduete.sd.atividade.concorrencia.exercicio1;

import io.github.lucasduete.sd.atividade.concorrencia.dao.Conexao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LoaderSelect {

    public static void main(String[] args) throws SQLException {
        Connection conn = Conexao.getConnection();

        PreparedStatement statement = conn.prepareStatement("SELECT * FROM TABLE_1");
//        statement.setFetchSize(200);

        long started = System.currentTimeMillis();
        ResultSet resultSet = statement.executeQuery();

//        while (resultSet.next());

        long finished = System.currentTimeMillis();

        System.out.println(finished - started);

    }
}
