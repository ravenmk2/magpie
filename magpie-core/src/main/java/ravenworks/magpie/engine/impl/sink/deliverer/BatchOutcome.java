package ravenworks.magpie.engine.impl.sink.deliverer;


/**
 * 一批消息的处置结果。
 *
 * @param watermark 已处置（投递成功或已落库）前缀的水位；本批无进展为 -1。
 *                  批次按 offset 有序，所有 offset ≤ watermark 的记录必须都已处置。
 * @param completed false 为中断信号（停机或 INTERRUPTED）：Worker 提交已处置前缀后
 *                  不再拉取，未处置后缀等重启从未提交 offset 重投。
 * @author Raven
 */
public record BatchOutcome(long watermark, boolean completed) {

}
