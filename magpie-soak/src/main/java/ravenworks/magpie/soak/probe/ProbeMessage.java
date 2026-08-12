package ravenworks.magpie.soak.probe;

/**
 * 探针消息：loadgen 写入、verifier 校验的载荷格式。
 *
 * @param key    业务键（同 key 顺序与完整性校验的最小维度），含 runId 前缀，
 *               loadgen 每次启动生成新 runId，重启天然隔离历史序列
 * @param seq    每 key 从 1 开始严格递增的序号，发送方保证同 key 前一条确认后才发下一条
 * @param sentAt 发送时刻（epoch millis），verifier 据此计算端到端延迟
 * @param pad    填充字段，把载荷撑到配置的 payloadSize
 */
public record ProbeMessage(String key, long seq, long sentAt, String pad) {

}
