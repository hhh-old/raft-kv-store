package com.raftkv.controller;

import com.raftkv.entity.WatchRequest;
import com.raftkv.service.RaftKVService;
import com.raftkv.service.WatchManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.HashMap;
import java.util.Map;

/**
 * Watch 控制器 - 提供 etcd 风格的 Watch API
 * 
 * 与 etcd 不同，本实现将"创建 Watch"和"订阅事件流"分成两步：
 * 1. POST /watch          → 创建订阅，返回 watchId（同时在服务端创建好 SseEmitter）
 * 2. GET /watch/{watchId} → 客户端连接 SSE 流，获取事件
 * 
 * 这种设计好处：
 * - 客户端可以保存 watchId，断线后用原 watchId 重连
 * - 不会在 POST 响应期间阻塞
 * 
 * 事件流格式（SSE）：
 * event: init\n
 * data: {"watchId":"xxx","currentRevision":42}\n\n
 * 
 * event: put\n
 * data: {"type":"PUT","key":"/config/app","value":"v1","revision":43,...}\n\n
 * 
 * event: delete\n
 * data: {"type":"DELETE","key":"/config/app","revision":44,...}\n\n
 */
@Slf4j
@RestController
@RequestMapping("/watch")
public class WatchController {

    @Autowired
    private WatchManager watchManager;

    @Autowired
    private RaftKVService raftKVService;

    /**
     * 创建 Watch 订阅
     * 
     * 请求体（JSON）：
     * {
     *   "key": "/config/app",      // 监听的 key 或前缀
     *   "prefix": false,            // 是否为前缀匹配，默认 false
     *   "startRevision": 0,         // 起始版本号，0 = 从当前开始
     *   "watchId": "optional-id"    // 可选，自定义 watchId
     * }
     * 
     * 响应：
     * {
     *   "watchId": "uuid",
     *   "currentRevision": 42,
     *   "streamUrl": "/watch/uuid"
     * }
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> createWatch(@RequestBody WatchRequest request) {
        log.info("Creating watch: key={}, prefix={}, startRevision={}",
                request.getKey(), request.isPrefix(), request.getStartRevision());

        try {
            // createWatch 返回 watchId，内部已创建好 SseEmitter
            String watchId = watchManager.createWatch(request);
            long currentRevision = raftKVService.getCurrentRevision();

            Map<String, Object> response = new HashMap<>();
            response.put("watchId", watchId);
            response.put("currentRevision", currentRevision);
            response.put("streamUrl", "/watch/" + watchId);

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to create watch", e);
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Failed to create watch: " + e.getMessage());
            return ResponseEntity.status(500).body(error);
        }
    }

    /**
     * 获取 Watch 事件流（SSE 长连接）
     * 
     * 客户端调用此接口后进入长连接状态，服务端实时推送 key 变化事件。
     * 
     * 连接结束条件：
     * 1. 客户端主动断开
     * 2. 调用 DELETE /watch/{watchId} 取消
     * 3. 服务端异常
     * 
     * @param watchId 已创建的 Watch ID（由 POST /watch 返回）
     * @return SSE 事件流
     */
    @GetMapping("/{watchId}")
    public SseEmitter watchStream(@PathVariable String watchId) {
        log.debug("Watch stream requested: {}", watchId);

        // 直接从 WatchManager 拿到对应的 SseEmitter
        SseEmitter emitter = watchManager.getEmitter(watchId);

        if (emitter == null) {
            // watch 不存在，返回一个携带错误信息的 emitter
            SseEmitter errorEmitter = new SseEmitter(0L);
            try {
                errorEmitter.send(SseEmitter.event()
                        .name("error")
                        .data("{\"error\":\"Watch not found: " + watchId + "\"}"));
                errorEmitter.complete();
            } catch (Exception e) {
                errorEmitter.completeWithError(e);
            }
            return errorEmitter;
        }

        // 直接返回已注册在 WatchManager 中的 emitter
        // 后续 publishEvent 调用都会推送到这个 emitter
        return emitter;
    }

    /**
     * 取消 Watch 订阅
     * 
     * @param watchId Watch ID
     */
    @DeleteMapping("/{watchId}")
    public ResponseEntity<Map<String, Object>> cancelWatch(@PathVariable String watchId) {
        log.info("Canceling watch: {}", watchId);

        if (!watchManager.hasWatch(watchId)) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Watch not found: " + watchId);
            return ResponseEntity.status(404).body(error);
        }

        watchManager.closeWatch(watchId);

        Map<String, Object> response = new HashMap<>();
        response.put("watchId", watchId);
        response.put("message", "Watch canceled successfully");
        return ResponseEntity.ok(response);
    }

    /**
     * 获取当前全局版本号
     * 
     * 客户端断线重连时，可先调用此接口获取 currentRevision，
     * 然后用 startRevision = currentRevision + 1 重新创建 Watch，
     * 确保不漏掉断线期间发生的事件。
     */
    @GetMapping("/revision")
    public ResponseEntity<Map<String, Object>> getRevision() {
        Map<String, Object> response = new HashMap<>();
        response.put("revision", raftKVService.getCurrentRevision());
        response.put("activeWatches", watchManager.getActiveWatchCount());
        return ResponseEntity.ok(response);
    }

    /**
     * Watch 统计信息（管理接口）
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getWatchStats() {
        Map<String, Object> response = new HashMap<>();
        response.put("activeWatches", watchManager.getActiveWatchCount());
        return ResponseEntity.ok(response);
    }
}
