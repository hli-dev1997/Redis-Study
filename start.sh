#!/bin/bash
# =============================================================
# redis-project 启动脚本
# 部署路径: /hli/project/RedisStudy/
# 日志路径: /hli/project/RedisStudy/logs/
# =============================================================

# ── 基础路径配置 ──────────────────────────────────────────────
BASE_DIR="/hli/project/RedisStudy"
JAR_NAME="redis-study-0.0.1-SNAPSHOT.jar"
JAR_PATH="${BASE_DIR}/${JAR_NAME}"
LOG_DIR="${BASE_DIR}/logs"
CONSOLE_LOG="${LOG_DIR}/console.log"
PID_FILE="${BASE_DIR}/app.pid"

# ── Java 21 路径（明确指定，不依赖系统 JAVA_HOME）────────────
JAVA_CMD="/hli/software/jdk21/bin/java"

# ── JVM 参数 ──────────────────────────────────────────────────
# -XX:+ZGenerational 是 JDK 21 新增的分代 ZGC，JDK 17 不支持，不要加
JVM_OPTS="-Xms3g -Xmx3g \
  -XX:+UseG1GC \
  -Djava.security.egd=file:/dev/./urandom"

# ── Spring 参数 ───────────────────────────────────────────────
SPRING_OPTS="--spring.application.name=redis-study"

# =============================================================

# 创建日志目录（不存在时自动建）
mkdir -p "${LOG_DIR}"

# ── 函数：打印带时间戳的消息 ──────────────────────────────────
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# ── 函数：启动 ───────────────────────────────────────────────
start() {
    if [ -f "${PID_FILE}" ]; then
        PID=$(cat "${PID_FILE}")
        if kill -0 "${PID}" 2>/dev/null; then
            log "服务已在运行中, PID=${PID}, 请勿重复启动"
            exit 1
        else
            log "发现残留 PID 文件但进程不存在, 清理后重新启动"
            rm -f "${PID_FILE}"
        fi
    fi

    if [ ! -f "${JAR_PATH}" ]; then
        log "错误: JAR 包不存在 -> ${JAR_PATH}"
        exit 1
    fi

    log "启动 redis-study ..."
    log "JAR  : ${JAR_PATH}"
    log "日志 : ${CONSOLE_LOG}"

    # nohup 后台启动，控制台输出重定向到 console.log
    nohup ${JAVA_CMD} ${JVM_OPTS} -jar "${JAR_PATH}" ${SPRING_OPTS} \
        >> "${CONSOLE_LOG}" 2>&1 &

    PID=$!
    echo ${PID} > "${PID_FILE}"
    log "启动成功, PID=${PID}"
    log "查看日志: tail -f ${CONSOLE_LOG}"
}

# ── 函数：停止 ───────────────────────────────────────────────
stop() {
    if [ ! -f "${PID_FILE}" ]; then
        log "PID 文件不存在, 服务可能未启动"
        return
    fi

    PID=$(cat "${PID_FILE}")
    if kill -0 "${PID}" 2>/dev/null; then
        log "正在停止服务, PID=${PID} ..."
        kill "${PID}"
        # 等待进程退出（最多 30 秒）
        for i in $(seq 1 30); do
            if ! kill -0 "${PID}" 2>/dev/null; then
                log "服务已停止"
                rm -f "${PID_FILE}"
                return
            fi
            sleep 1
        done
        log "进程 30s 未退出, 强制 kill -9"
        kill -9 "${PID}"
        rm -f "${PID_FILE}"
    else
        log "进程 PID=${PID} 不存在, 清理 PID 文件"
        rm -f "${PID_FILE}"
    fi
}

# ── 函数：查看状态 ───────────────────────────────────────────
status() {
    if [ -f "${PID_FILE}" ]; then
        PID=$(cat "${PID_FILE}")
        if kill -0 "${PID}" 2>/dev/null; then
            log "服务运行中, PID=${PID}"
        else
            log "PID 文件存在但进程已退出 (PID=${PID}), 服务异常停止"
        fi
    else
        log "服务未启动"
    fi
}

# ── 函数：查看日志 ───────────────────────────────────────────
log_tail() {
    tail -f "${CONSOLE_LOG}"
}

# ── 入口 ─────────────────────────────────────────────────────
case "$1" in
    start)   start    ;;
    stop)    stop     ;;
    restart) stop; sleep 2; start ;;
    status)  status   ;;
    log)     log_tail ;;
    *)
        echo "用法: $0 {start|stop|restart|status|log}"
        echo ""
        echo "  start    启动服务"
        echo "  stop     停止服务"
        echo "  restart  重启服务"
        echo "  status   查看运行状态"
        echo "  log      实时查看控制台日志 (tail -f)"
        exit 1
        ;;
esac
