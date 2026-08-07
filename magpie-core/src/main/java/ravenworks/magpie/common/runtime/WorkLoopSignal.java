package ravenworks.magpie.common.runtime;


/**
 * WorkLoop 派发的生命周期信号。
 *
 * @author Raven
 */
public enum WorkLoopSignal {

    IDLE,
    STARTED,
    PRE_SHUTDOWN,
    TERMINATED

}
