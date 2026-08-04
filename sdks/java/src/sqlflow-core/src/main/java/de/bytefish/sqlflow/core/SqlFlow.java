package de.bytefish.sqlflow.core;

import de.bytefish.sqlflow.core.functional.TaskHandler;
import de.bytefish.sqlflow.core.models.*;
import de.bytefish.sqlflow.core.workers.WorkerOptions;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public interface SqlFlow {

    /**
     * Ported Worker. Uses a Semaphore to control concurrency and a ConcurrentHashMap
     * to track active tasks. Virtual Threads make this code very efficient.
     */
    public class SqlFlowWorker {
        private final SqlFlow client;
        private final WorkerOptions options;
        private final ConcurrentHashMap<String, Boolean> executing = new ConcurrentHashMap<>();

        public SqlFlowWorker(WorkerOptions options, SqlFlow client) {
            this.client = client;
            this.options = options;
        }

        public void execute() {
            Semaphore semaphore = new Semaphore(options.concurrency());
            int batchSize = options.batchSize() != null ? options.batchSize() : options.concurrency();

            // The infinite loop of a background worker
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (semaphore.availablePermits() == 0) {
                        Thread.sleep((long) (options.pollInterval() * 1000));
                        continue;
                    }

                    int toClaim = Math.min(batchSize, semaphore.availablePermits());
                    List<ClaimedTask> messages = client.claimTasks(options.queue(), options.workerId(), options.claimTimeout(), toClaim);

                    if (messages.isEmpty()) {
                        Thread.sleep((long) (options.pollInterval() * 1000));
                        continue;
                    }

                    for (ClaimedTask task : messages) {
                        semaphore.acquire();
                        // Launch a Virtual Thread for the task
                        Thread.ofVirtual().start(() -> executeTaskWrapper(task, semaphore));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception ex) {
                    if (options.onError() != null) options.onError().accept(ex);
                    try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
                }
            }
        }

        private void executeTaskWrapper(ClaimedTask task, Semaphore semaphore) {
            try {
                executing.put(task.taskId(), true);
                client.executeTask(task, options.queue(), options.claimTimeout(), options.fatalOnLeaseTimeout());
            } catch (Exception ex) {
                if (options.onError() != null) options.onError().accept(ex);
            } finally {
                executing.remove(task.taskId());
                semaphore.release();
            }
        }
    }
    void registerTask(TaskRegistrationOptions options, TaskHandler handler);

    void createQueue(String queueName);

    void dropQueue(String queueName);

    List<String> listQueues();

    <TRequest> SpawnResult spawn(SpawnOptions options, String jobName, TRequest request);

    void emitEvent(EmitEventOptions options, String eventName, Object payload);

    void cancelTask(CancelTaskOptions options, String taskId);

    List<ClaimedTask> claimTasks(String queue, String workerId, int claimTimeout, int batchSize);

    void workBatch(String queue, String workerId, int claimTimeout, int batchSize);

    void executeTask(ClaimedTask task, String queue, int claimTimeout, boolean fatalOnLeaseTimeout);
}