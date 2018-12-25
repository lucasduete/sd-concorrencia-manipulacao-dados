package io.github.lucasduete.sd.atividade.concorrencia.exercicio1;

import io.github.lucasduete.sd.atividade.concorrencia.dao.Conexao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class LoaderSimple {

    private static final AtomicInteger id = new AtomicInteger(0);
    private static final Integer LIMIT = 100;

    private static final String sqlInsert = "INSERT INTO Table_1(Id, Nome) VALUES (?,?);";
    private static final String sqlUpdate = "UPDATE Table_1 SET updated = TRUE";
    private static final String sqlDelete = "UPDATE Table_1 SET deleted = TRUE";

    public static void main(String[] args) throws SQLException {

        Connection conn = Conexao.getConnection();

        PreparedStatement stmtInsert = conn.prepareStatement(sqlInsert);
        PreparedStatement stmtUpdate = conn.prepareStatement(sqlUpdate);
        PreparedStatement stmtDelete = conn.prepareStatement(sqlDelete);

        Long startedMilli = System.currentTimeMillis();

        while (LIMIT > id.get()) {

            stmtInsert.setInt(1, id.get());
            stmtInsert.setString(2, String.format("Nome%d", id.getAndIncrement()));
            stmtInsert.executeUpdate();

            stmtUpdate.executeUpdate();

            stmtDelete.executeUpdate();
        }

        Long finishedMilli = System.currentTimeMillis();

        System.out.println(finishedMilli - startedMilli + "ms");
    }
}
