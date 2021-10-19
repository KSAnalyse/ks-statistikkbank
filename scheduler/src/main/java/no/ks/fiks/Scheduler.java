package no.ks.fiks;

public class Scheduler implements Runnable{

    private String message;

    public Scheduler(String scheduler_executed_using_thread) {
        this.message = scheduler_executed_using_thread;
    }

    @Override
    public void run() {
        System.out.println(message);
    }
}
