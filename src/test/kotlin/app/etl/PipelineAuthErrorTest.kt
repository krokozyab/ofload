package app.etl

import app.config.PipelineConfig
import my.jdbc.wsdl_driver.auth.TokenExpiredException
import java.sql.SQLException
import java.sql.SQLTimeoutException
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Unit tests for [Pipeline.isSourceAuthError] — the string-matching heuristic that
 * decides whether a source-side failure is worth reconnecting + retrying.
 *
 * False positives only cost one extra SSO reconnect; false negatives mean an auth
 * failure escapes the retry loop and fails the run. Bias is therefore toward
 * matching liberally — these tests guard the known keywords.
 */
class PipelineAuthErrorTest {

    private val pipeline = Pipeline(PipelineConfig(emptyList()))

    private fun assertAuthError(msg: String) {
        assertTrue(
            pipeline.isSourceAuthError(Exception(msg)),
            "expected '$msg' to be classified as auth error"
        )
    }

    private fun assertNotAuthError(msg: String) {
        assertFalse(
            pipeline.isSourceAuthError(Exception(msg)),
            "expected '$msg' to NOT be classified as auth error"
        )
    }

    // ── Positive cases ──────────────────────────────────────────────

    @Test fun `401 status code`() = assertAuthError("HTTP 401 returned from WSDL endpoint")
    @Test fun `unauthorized literal`() = assertAuthError("Unauthorized")
    @Test fun `authentication keyword`() = assertAuthError("Authentication failed: invalid credentials")
    @Test fun `token keyword`() = assertAuthError("Token expired, please re-authenticate")
    @Test fun `session keyword`() = assertAuthError("Session invalidated by server")
    @Test fun `login keyword`() = assertAuthError("Login required")

    @Test fun `case-insensitive matching`() {
        assertAuthError("AUTHENTICATION failure")
        assertAuthError("Token Expired")
        assertAuthError("UNAUTHORIZED access")
    }

    // ── Negative cases ──────────────────────────────────────────────

    @Test fun `Oracle column error is not auth`() =
        assertNotAuthError("ORA-00904: \"INVOICE_ID\": invalid identifier")

    @Test fun `Oracle table-not-found is not auth`() =
        assertNotAuthError("ORA-00942: table or view does not exist")

    @Test fun `null pointer is not auth`() =
        assertNotAuthError("NullPointerException at line 42")

    @Test fun `null message is not auth`() {
        assertFalse(pipeline.isSourceAuthError(Exception(null as String?)))
    }

    @Test fun `empty message is not auth`() =
        assertNotAuthError("")

    @Test fun `unrelated SQL error is not auth`() =
        assertNotAuthError("Column count doesn't match placeholder count")

    // ── Regression guards ──────────────────────────────────────────────

    @Test fun `ora-error containing word 'session' still matches because that is a transient signal`() {
        // "session" substring matches — this IS intentional. "session closed", "session invalidated"
        // etc. are worth a retry even when reported via ORA-* wrappers.
        assertAuthError("ORA-01012: not logged on (session ended)")
    }

    // ── Type-based detection (primary signal) ──────────────────────────

    @Test fun `TokenExpiredException is recognised directly — primary signal not string-matching`() {
        val e = TokenExpiredException("any wording you like, regression-proof", statusCode = 401)
        assertTrue(pipeline.isSourceAuthError(e))
    }

    @Test fun `TokenExpiredException wrapped in SQLTimeoutException is found via cause chain`() {
        // This is the realistic shape after ofjdbc's fetchPageXml exhausts retries:
        // it wraps the underlying TokenExpiredException as the cause of SQLTimeoutException.
        val original = TokenExpiredException("Token expired or unauthorized (401): ...", statusCode = 401)
        val wrapped = SQLTimeoutException("Failed to fetch page after 3 attempts ...", original)
        assertTrue(pipeline.isSourceAuthError(wrapped))
    }

    @Test fun `TokenExpiredException survives deep wrapping`() {
        val original = TokenExpiredException("401")
        val outer = RuntimeException("outermost", SQLException("middle", original))
        assertTrue(pipeline.isSourceAuthError(outer))
    }

    @Test fun `TokenExpiredException detected even with ORA-wording in outer message`() {
        // Pathological case — if some future wrapper mislabels it with "Oracle SQL error"
        // prefix, the type check still wins. Guards against string heuristic being overridden.
        val original = TokenExpiredException("401 unauthorized")
        val wrapped = SQLException("Oracle SQL error: something else", original)
        assertTrue(pipeline.isSourceAuthError(wrapped))
    }

    @Test fun `non-auth exception with non-auth cause is not classified as auth`() {
        val original = SQLException("ORA-00942: table or view does not exist")
        val wrapped = SQLException("wrapper", original)
        assertFalse(pipeline.isSourceAuthError(wrapped))
    }

    // ── Message aggregation across cause chain ─────────────────────────

    @Test fun `auth keyword in cause message (not top) still matches via fallback`() {
        // Realistic: outer layer is generic "Failed to fetch", inner cause has the auth wording.
        val cause = SQLException("Authentication failed - invalid username or password (403): ...")
        val outer = SQLTimeoutException("Failed to fetch page after 3 attempts", cause)
        assertTrue(pipeline.isSourceAuthError(outer))
    }
}
