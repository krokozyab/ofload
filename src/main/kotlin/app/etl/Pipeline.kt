package app.etl

import app.config.PipelineConfig
import app.db.OracleDs
import app.db.SourceDs

class Pipeline(private val cfg: PipelineConfig) {

    fun run() {
        for (target in cfg.targets) {
            println("[${target.name}] Starting...")
            println("[${target.name}] Connecting to Oracle ATP...")
            OracleDs.getConnection().use { oracleConn ->
                println("[${target.name}] ATP connected.")
                oracleConn.autoCommit = false
                try {
                    var lastWm = WatermarkStore.read(oracleConn, target.name)
                    WatermarkStore.beginRun(oracleConn, target.name, target.watermarkColumn)
                    oracleConn.commit()

                    var totalLoaded = 0L
                    var totalMerged = 0L
                    var pageNum = 0
                    var lastKeyValue: String? = null

                    do {
                        pageNum++
                        println("[${target.name}] Connecting to source...")
                        val result = SourceDs.getConnection().use { srcConn ->
                            println("[${target.name}] Source connected, loading page $pageNum (wm=$lastWm, lastKey=$lastKeyValue)...")
                            StagingLoader.load(target, srcConn, oracleConn, lastWm, lastKeyValue)
                        }

                        if (result.rowsLoaded == 0L) break

                        val startMerge = System.currentTimeMillis()
                        val merged = TargetMerger.merge(oracleConn, target)
                        println("[${target.name}] Merged $merged rows in ${System.currentTimeMillis() - startMerge}ms")
                        totalLoaded += result.rowsLoaded
                        totalMerged += merged
                        lastWm = result.newWm
                        lastKeyValue = result.lastKeyValue

                        // Commit watermark after each page so progress is saved
                        WatermarkStore.finishRun(oracleConn, target.name, lastWm, totalLoaded)
                        oracleConn.commit()

                        if (target.pageSize != null) {
                            println("[${target.name}] Page $pageNum: loaded=${result.rowsLoaded}, merged=$merged | Total: loaded=$totalLoaded, merged=$totalMerged | wm=$lastWm")
                        }
                    } while (target.pageSize != null && result.rowsLoaded.toInt() == target.pageSize)

                    println("[${target.name}] OK: total_loaded=$totalLoaded, total_merged=$totalMerged, new_wm=$lastWm")
                } catch (e: Exception) {
                    runCatching { oracleConn.rollback() }
                    OracleDs.getConnection().use { failConn ->
                        failConn.autoCommit = true
                        WatermarkStore.failRun(failConn, target.name, e.message ?: "unknown error")
                    }
                    System.err.println("[${target.name}] FAILED: ${e.message}")
                }
            }
        }
    }
}
