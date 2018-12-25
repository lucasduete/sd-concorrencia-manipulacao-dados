package io.github.lucasduete.sd.atividade.concorrencia.exercicio2;

import io.github.lucasduete.sd.atividade.concorrencia.dao.Conexao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class LoaderUsingDB {

    private static Long startedMilli;

    private static final Integer LIMIT = 1000;

    private static final String sqlInsert = "INSERT INTO Table_1(Id, Nome) VALUES (?,?);";
    private static final String sqlUpdate = "UPDATE Table_1 SET updated = TRUE WHERE id = ?";
    private static final String sqlDelete = "UPDATE Table_1 SET deleted = TRUE WHERE id = ?";
    private static final String sqlConfigId = "INSERT INTO control_index(index_name, index_value) VALUES ('default', 0);";
    private static final String sqlRetriveId = "UPDATE control_index SET index_value = (index_value + 1) WHERE index_name = 'default' RETURNING index_value;";

    private static final BlockingQueue<Integer> queueUpdate = new ArrayBlockingQueue<>(3);
    private static final BlockingQueue<Integer> queueDelete = new ArrayBlockingQueue<>( 3);

    private static Thread restoreUpdate = null;
    private static Thread restoreDelete = null;

    private static Boolean doContinue = true;

    public static void main(String[] args) throws Exception {

        Connection conn = Conexao.getConnection();

        try {
            conn.prepareStatement(sqlConfigId).executeUpdate();
        } catch (Exception ex) {
            // do nothing
        }

        Runnable insertion = () -> {
            try {

                ResultSet rs = conn.prepareStatement(sqlRetriveId).executeQuery();

                int localId = 0;
                if (rs.next()) localId = rs.getInt("index_value");
                rs.close();

                if (localId > LIMIT) stop();
                else {

                    PreparedStatement stmtInsert = conn.prepareStatement(sqlInsert);

                    stmtInsert.setInt(1, localId);
                    stmtInsert.setString(2, String.format("Nome%d", localId));
                    stmtInsert.executeUpdate();

                    queueUpdate.put(localId);
                }

            } catch (Exception ex) {
                ex.printStackTrace();
                return;
            }
        };

        Runnable updation = () -> {
            try {
                Integer localId = new Integer(queueUpdate.take());
                PreparedStatement stmtUpdate = conn.prepareStatement(sqlUpdate);

                stmtUpdate.setInt(1, localId);
                stmtUpdate.executeUpdate();

                queueDelete.put(localId);
            } catch (Exception ex) {
                ex.printStackTrace();
                return;
            }
        };

        Runnable deletation = () -> {
            try {
                Integer localId = new Integer(queueDelete.take());
                PreparedStatement stmtDelete = conn.prepareStatement(sqlDelete);

                stmtDelete.setInt(1, localId);
                stmtDelete.executeUpdate();

                if (localId.equals(LIMIT)) stop();
            } catch (Exception ex) {
                ex.printStackTrace();
                return;
            }
        };

        restoreUpdate = new Thread(() -> {
            try {
                Connection localConn = Conexao.getConnection();

                String sqlRestoreUpdate = "SELECT id FROM table_1 WHERE updated = FALSE;";

                ResultSet rs = localConn.prepareStatement(sqlRestoreUpdate).executeQuery();

                while (rs.next()) queueUpdate.put(rs.getInt("id"));

            } catch (SQLException | InterruptedException ex) {
                ex.printStackTrace();
            }
        });

        restoreDelete = new Thread(() -> {
            try {
                Connection localConn = Conexao.getConnection();

                String sqlRestoreUpdate = "SELECT id FROM table_1 WHERE deleted = FALSE;";

                ResultSet rs = localConn.prepareStatement(sqlRestoreUpdate).executeQuery();

                while (rs.next()) queueDelete.put(rs.getInt("id"));

            } catch (SQLException | InterruptedException ex) {
                ex.printStackTrace();
            }
        });

        startedMilli = System.currentTimeMillis();

        restoreUpdate.start();
        restoreDelete.start();

        while (getDoContinue()) {

            new Thread(insertion).start();
            new Thread(updation).start();
            new Thread(deletation).start();
        }


    }

    public static void stop() {
        if(restoreUpdate.isAlive() || restoreDelete.isAlive()) return;
        else {
            Long finishedMilli = System.currentTimeMillis();
            System.out.printf("\n\n" + (finishedMilli - startedMilli) + "ms\n\n");
            setDoContinue(false);
        }
    }

    public static Boolean getDoContinue() {
        return doContinue;
    }

    public static void setDoContinue(Boolean doContinue) {
        LoaderUsingDB.doContinue = doContinue;
    }
}
