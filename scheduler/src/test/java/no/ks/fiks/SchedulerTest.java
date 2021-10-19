package no.ks.fiks;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SchedulerTest {

    @Test
    public void givenRunnable_whenRunIt_thenResult() throws Exception {
        Thread thread = new Thread(new Scheduler("Scheduler executed using Thread"));
        thread.start();
        thread.join();
    }

}