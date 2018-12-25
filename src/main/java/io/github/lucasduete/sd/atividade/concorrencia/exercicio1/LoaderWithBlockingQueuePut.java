package io.github.lucasduete.sd.atividade.concorrencia.exercicio1;

import io.github.lucasduete.sd.atividade.concorrencia.dao.Conexao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class LoaderWithBlockingQueuePut {

    private static Long startedMilli;

    private static Integer id = new Integer(1);
    private static final Integer LIMIT = 1000;

    private static final String sqlInsert = "INSERT INTO Table_1(Id, Nome) VALUES (?,?);";
    private static final String sqlUpdate = "UPDATE Table_1 SET updated = TRUE WHERE id = ?";
    private static final String sqlDelete = "UPDATE Table_1 SET deleted = TRUE WHERE id = ?";

    public static void main(String[] args) throws Exception {

        Connection conn = Conexao.getConnection();

        BlockingQueue<Integer> queueInsert = new ArrayBlockingQueue<>(10);
        BlockingQueue<Integer> queueUpdate = new ArrayBlockingQueue<>(10);
        BlockingQueue<Integer> queueDelete = new ArrayBlockingQueue<>( 10);

        Runnable insertion = () -> {
            try {
                int localId = Integer.valueOf(queueInsert.take());
                PreparedStatement stmtInsert = conn.prepareStatement(sqlInsert);

                stmtInsert.setInt(1, localId);
                stmtInsert.setString(2, String.format("Nome%d", localId));
                stmtInsert.executeUpdate();

                queueUpdate.put(localId);

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

                if (localId.equals(LIMIT)) {
                    Long finishedMilli = System.currentTimeMillis();
                    System.out.printf("\n\n" + (finishedMilli - startedMilli) + "ms\n\n");
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                return;
            }
        };

        startedMilli = System.currentTimeMillis();

        while (LIMIT >= id) {

            queueInsert.put(id++);

            new Thread(insertion).start();
            new Thread(updation).start();
            new Thread(deletation).start();

        }

    }
}
