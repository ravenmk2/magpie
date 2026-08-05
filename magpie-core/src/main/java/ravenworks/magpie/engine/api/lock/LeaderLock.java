package ravenworks.magpie.engine.api.lock;


/**
 * @author Raven
 */
public interface LeaderLock {

    void init();

    PulseResult pulse();

    void release();


    enum PulseResult {
        ACQUIRED,
        RENEWED,
        LOST,
        FAILED
    }

}
