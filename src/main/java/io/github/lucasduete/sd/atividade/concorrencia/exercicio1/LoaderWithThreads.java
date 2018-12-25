package io.github.lucasduete.sd.atividade.concorrencia.exercicio1;

import io.github.lucasduete.sd.atividade.concorrencia.dao.Conexao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class LoaderWithThreads {

    private static final AtomicInteger id = new AtomicInteger(0);
    private static final Integer LIMIT = 100;

    private static final String sqlInsert = "INSERT INTO Table_1(Id, Nome) VALUES (?,?);";
    private static final String sqlUpdate = "UPDATE Table_1 SET updated = TRUE WHERE id = ?";
    private static final String sqlDelete = "UPDATE Table_1 SET deleted = TRUE WHERE id = ?";

    public static void main(String[] args) throws SQLException {

        Connection conn = Conexao.getConnection();

        PreparedStatement stmtInsert = conn.prepareStatement(sqlInsert);
        PreparedStatement stmtUpdate = conn.prepareStatement(sqlUpdate);
        PreparedStatement stmtDelete = conn.prepareStatement(sqlDelete);

        Runnable insertion = () -> {
            try {
                stmtInsert.setInt(1, id.get());
                stmtInsert.setString(2, String.format("Nome%d", id.getAndIncrement()));
                stmtInsert.executeUpdate();
            } catch (SQLException ex) {
                ex.printStackTrace();
                return;
            }
        };

        Runnable updation = () -> {
            try {
                stmtUpdate.setInt(1, id.get());
                stmtUpdate.executeUpdate();
            } catch (SQLException ex) {
                ex.printStackTrace();
                return;
            }
        };

        Runnable deletation = () -> {
            try {
                stmtDelete.setInt(1, id.get());
                stmtDelete.executeUpdate();
            } catch (SQLException ex) {
                ex.printStackTrace();
                return;
            }
        };

        Long startedMilli = System.currentTimeMillis();

        int count = 0;
        while (LIMIT > count) {
            new Thread(insertion).start();
            new Thread(updation).start();
            new Thread(deletation).start();
            count++;
        }

        Long finishedMilli = System.currentTimeMillis();


        System.out.println(finishedMilli - startedMilli + "ms");
    }
}
