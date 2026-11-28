package com.lab.air.common.lab;

import com.lab.air.common.BusinessException;
import com.lab.air.common.RequestContextFilter;
import com.lab.air.common.SqlTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;

import javax.sql.DataSource;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Chaos v2 GERÇEK workload'ları. Her biri uygulamanın KENDİ DB rolü ve bağlantı havuzu (veya açıkça ayrı
 * işaretlenmiş fiziksel bağlantılar) ile çalışır; böylece PostgreSQL'deki etki (lock, CPU, temp, WAL, connection)
 * pg_stat_activity / pg_stat_statements / blocking zincirinde doğru uygulamaya atfedilir.
 *
 * <p>Hiçbiri iş verisini yıkıcı biçimde değiştirmez: sentetik workload'lar yalnız {@code chaos.*} tablolarına
 * dokunur; "business" kapsamlı long transaction iş satırına yalnız NO-OP UPDATE (updated_at = updated_at)
 * yapar (gerçek row lock, veri değişmez). Tüm süreler / boyutlar {@link ChaosLabProperties} ile sınırlıdır;
 * SQL'ler {@code lab:chaos-*} etiketlidir ki {@code /lab/cleanup} iptal edebilsin.
 *
 * <p>Error injection: {@code errorPct} verilirse iş yapıldıktan sonra o olasılıkla GERÇEK bir DB hatası
 * (division by zero) üretilir ve transaction rollback olur; PostgreSQL logunda ve uygulama 5xx'inde görünür.
 */
public class ChaosWorkloads {

    private static final Logger log = LoggerFactory.getLogger(ChaosWorkloads.class);
    private static final int MB = 1024 * 1024;
    /**
     * Ballast parça boyutu. 1 MB'lık byte[] küçük heap'lerde (G1 region = 1 MB) başlık nedeniyle humongous olur ve
     * 2 region kaplar → gerçek ballast hesaplananın ~2 katı (ölçüldü: OutOfMemoryError). 256 KB yarım region'ın altında.
     */
    private static final int BALLAST_CHUNK = 256 * 1024;

    private final DataSource ds;
    private final ChaosLabProperties props;
    private final Environment env;
    private final String app;
    private final Semaphore permits;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
            .version(HttpClient.Version.HTTP_1_1).build();      // h2c upgrade denemesi Tomcat ile IOException verebiliyor

    /** cleanup her çağrıldığında artar; uzun süren döngüler (burn, long tx) bunu görüp çıkar. */
    private final AtomicLong generation = new AtomicLong();
    private final AtomicInteger burning = new AtomicInteger();
    private final AtomicInteger longTx = new AtomicInteger();
    private final ConcurrentLinkedQueue<byte[]> ballast = new ConcurrentLinkedQueue<>();
    private final AtomicLong ballastBytes = new AtomicLong();
    private final AtomicLong writes = new AtomicLong();
    private final AtomicLong deadlocksSeen = new AtomicLong();
    private final AtomicLong serializationFailures = new AtomicLong();
    /** chaos.churn satır sayısı (V7 migration ile aynı). */
    private static final int CHURN_ROWS = 50_000;
    /** Advisory lock anahtar sınıfı: iş kodunun kullanabileceği anahtarlarla çakışmaz. */
    private static final int ADVISORY_CLASS = 424_242;
    private static final java.util.regex.Pattern WORKERS_PLANNED =
            java.util.regex.Pattern.compile("\"Workers Planned\":\\s*(\\d+)");

    public ChaosWorkloads(DataSource ds, ChaosLabProperties props, Environment env) {
        this.ds = ds;
        this.props = props;
        this.env = env;
        this.app = env.getProperty("spring.application.name", "app");
        this.permits = new Semaphore(props.maxConcurrentOps());
    }

    // ------------------------------------------------------------------ row contention (S02)

    /** Paylaşılan sentetik satırlardan birini günceller ve lock'u holdMs tutar. records küçüldükçe contention artar. */
    public Map<String, Object> contend(int records, long holdMs, int errorPct) throws SQLException {
        int n = clamp(records, 1, 1000);
        long hold = Math.min(Math.max(0, holdMs), props.maxLongTxMs());
        int id = ThreadLocalRandom.current().nextInt(n);
        return withPermit(() -> {
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(false);
                try {
                    long t0 = System.nanoTime();
                    try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag("chaos-contend",
                            "UPDATE chaos.shared_records SET counter = counter + 1, touched_by = ?, touched_at = now() WHERE id = ?"))) {
                        ps.setString(1, app);
                        ps.setInt(2, id);
                        ps.executeUpdate();
                    }
                    long lockWaitMs = (System.nanoTime() - t0) / 1_000_000;
                    sleepInTx(c, "chaos-contend", hold);
                    maybeFail(c, errorPct);
                    c.commit();
                    return result("record", id, "lockWaitMs", lockWaitMs, "heldMs", hold);
                } catch (SQLException e) {
                    c.rollback();
                    throw e;
                }
            }
        });
    }

    // ------------------------------------------------------------------ long transaction (S03)

    /**
     * Transaction'ı durationMs boyunca açık tutar. scope=chaos → ilk {@code rows} paylaşılan sentetik satır;
     * scope=business → verilen uçuşun envanter satırına NO-OP UPDATE (gerçek iş akışlarıyla aynı satırı kilitler,
     * veriyi değiştirmez). Bekleme kısa pg_sleep adımlarıyla yapılır: oturum "active" kalır, cleanup iptal edebilir.
     */
    public Map<String, Object> longTransaction(String scope, Long flightId, int rows, long durationMs, int errorPct)
            throws SQLException {
        long dur = Math.min(Math.max(0, durationMs), props.maxLongTxMs());
        long gen = generation.get();
        return withPermit(() -> {
            longTx.incrementAndGet();
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(false);
                try {
                    int locked;
                    if ("business".equals(scope)) {
                        if (flightId == null) {
                            throw BusinessException.badRequest("FLIGHT_REQUIRED", "scope=business için flightId gerekli");
                        }
                        try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag("chaos-longtx",
                                "UPDATE flightops.flight_inventory SET updated_at = updated_at WHERE flight_id = ?"))) {
                            ps.setLong(1, flightId);
                            locked = ps.executeUpdate();
                        }
                    } else {
                        try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag("chaos-longtx",
                                "UPDATE chaos.shared_records SET touched_by = ?, touched_at = now() WHERE id < ?"))) {
                            ps.setString(1, app);
                            ps.setInt(2, clamp(rows, 1, 1000));
                            locked = ps.executeUpdate();
                        }
                    }
                    long end = System.nanoTime() + dur * 1_000_000;
                    while (System.nanoTime() < end && generation.get() == gen) {
                        long left = (end - System.nanoTime()) / 1_000_000;
                        sleepInTx(c, "chaos-longtx", Math.min(2_000, Math.max(1, left)));
                    }
                    maybeFail(c, errorPct);
                    c.commit();
                    return result("scope", scope, "rowsLocked", locked, "heldMs", dur,
                            "interrupted", generation.get() != gen);
                } catch (SQLException e) {
                    c.rollback();
                    throw e;
                }
            } finally {
                longTx.decrementAndGet();
            }
        });
    }

    // ------------------------------------------------------------------ CPU-intensive SQL (S04, S17)

    /** Gerçek CPU: md5 + agregasyon. work = bin satır (1..2000) → bir çağrı ~work×1000 satır işler. */
    public Map<String, Object> cpuQuery(int work, int errorPct) throws SQLException {
        int rows = clamp(work, 1, 2_000) * 1_000;
        return withPermit(() -> timedQuery("chaos-cpu", """
                SELECT count(*), max(h) FROM (
                  SELECT md5(md5(g::text || s.payload)) AS h
                    FROM generate_series(1, ?) g
                    JOIN chaos.synthetic_data s ON s.id = (g::bigint * 7919) % 400000
                ) x""", rows, errorPct));
    }

    // ------------------------------------------------------------------ heavy read (S06, S17)

    /** İndeksiz sıralı tarama + agregasyon; scanPct = tablonun taranan yüzdesi. */
    public Map<String, Object> heavyRead(int scanPct, int errorPct) throws SQLException {
        int pct = clamp(scanPct, 1, 100);
        int mod = ThreadLocalRandom.current().nextInt(3, 97);
        return withPermit(() -> {
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(false);
                try {
                    long t0 = System.nanoTime();
                    long cnt;
                    serialOnly(c);
                    try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag("chaos-read", """
                            SELECT count(*), avg(val), max(payload) FROM chaos.synthetic_data
                             WHERE id < (SELECT (max(id) * ?::numeric / 100)::bigint FROM chaos.synthetic_data)
                               AND k % ? = ?"""))) {
                        ps.setInt(1, pct);
                        ps.setInt(2, mod);
                        ps.setInt(3, ThreadLocalRandom.current().nextInt(mod));
                        try (ResultSet rs = ps.executeQuery()) {
                            rs.next();
                            cnt = rs.getLong(1);
                        }
                    }
                    maybeFail(c, errorPct);
                    c.commit();
                    return result("matched", cnt, "scanPct", pct, "ms", (System.nanoTime() - t0) / 1_000_000);
                } catch (SQLException e) {
                    c.rollback();
                    throw e;
                }
            }
        });
    }

    // ------------------------------------------------------------------ temp file pressure (S07)

    /** work_mem'i transaction içinde 64kB'a düşürüp büyük bir sort yapar → gerçek temp file I/O. */
    public Map<String, Object> tempSpill(int kRows, int errorPct) throws SQLException {
        int rows = clamp(kRows, 10, 2_000) * 1_000;
        return withPermit(() -> {
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(false);
                try (Statement st = c.createStatement()) {
                    st.execute("SET LOCAL work_mem = '64kB'");
                    long t0 = System.nanoTime();
                    try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag("chaos-temp", """
                            SELECT payload FROM (
                              SELECT s.payload, md5(s.payload || g::text) AS o
                                FROM generate_series(1, ?) g
                                JOIN chaos.synthetic_data s ON s.id = (g::bigint * 104729) % 400000
                            ) x ORDER BY o OFFSET ? LIMIT 1"""))) {
                        ps.setInt(1, rows);
                        ps.setInt(2, rows / 2);
                        ps.executeQuery().close();
                    }
                    maybeFail(c, errorPct);
                    c.commit();
                    return result("sortedRows", rows, "ms", (System.nanoTime() - t0) / 1_000_000);
                } catch (SQLException e) {
                    c.rollback();
                    throw e;
                }
            }
        });
    }

    // ------------------------------------------------------------------ high-frequency writes (S05)

    /** Toplu INSERT (WAL + I/O). write_sink belirli boyutu aşınca SECURITY DEFINER fonksiyonla budanır. */
    public Map<String, Object> writeBurst(int rows, int payloadBytes, int errorPct) throws SQLException {
        int n = clamp(rows, 1, props.maxWriteRows());
        int reps = clamp(payloadBytes / 32, 1, 64);
        return withPermit(() -> {
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(false);
                try {
                    long t0 = System.nanoTime();
                    try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag("chaos-write", """
                            INSERT INTO chaos.write_sink (app, experiment_id, payload)
                            SELECT ?, ?, repeat(md5(random()::text), ?) FROM generate_series(1, ?)"""))) {
                        ps.setString(1, app);
                        ps.setString(2, MDC.get(RequestContextFilter.MDC_EXPERIMENT_ID));
                        ps.setInt(3, reps);
                        ps.setInt(4, n);
                        ps.executeUpdate();
                    }
                    maybeFail(c, errorPct);
                    c.commit();
                    long total = writes.addAndGet(n);
                    if (total / props.writeSinkKeep() != (total - n) / props.writeSinkKeep()) {
                        trimWriteSink(c, props.writeSinkKeep());
                    }
                    return result("rows", n, "bytesPerRow", reps * 32, "ms", (System.nanoTime() - t0) / 1_000_000);
                } catch (SQLException e) {
                    c.rollback();
                    throw e;
                }
            }
        });
    }

    // ------------------------------------------------------------------ deadlock (S09)

    /**
     * (2·pair, 2·pair+1) satırlarını order=ab veya ba sırasıyla kilitler; iki uygulama zıt sırayla aynı çifti
     * çalıştırınca PostgreSQL deadlock_timeout sonrası birini 40P01 ile iptal eder. Uygulama, gerçek bir istemci
     * gibi sınırlı retry yapar; tüm denemeler başarısızsa hata döner.
     */
    public Map<String, Object> deadlock(int pair, String order, long holdMs, int retries) throws SQLException {
        int p = clamp(pair, 0, 199);
        int a = 2 * p;
        int b = 2 * p + 1;
        boolean ab = !"ba".equals(order);
        long hold = Math.min(Math.max(0, holdMs), 5_000);
        int maxAttempts = clamp(retries, 0, 5) + 1;
        return withPermit(() -> {
            int deadlocks = 0;
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                try (Connection c = ds.getConnection()) {
                    c.setAutoCommit(false);
                    try {
                        updatePair(c, ab ? a : b);
                        sleepInTx(c, "chaos-deadlock", hold);
                        updatePair(c, ab ? b : a);
                        c.commit();
                        return result("pair", p, "order", ab ? "ab" : "ba", "attempts", attempt, "deadlocks", deadlocks);
                    } catch (SQLException e) {
                        c.rollback();
                        if (!"40P01".equals(e.getSQLState())) {
                            throw e;
                        }
                        deadlocks++;
                        deadlocksSeen.incrementAndGet();
                        log.warn("chaos deadlock algılandı (pair={}, attempt={}/{})", p, attempt, maxAttempts);
                        if (attempt == maxAttempts) {
                            throw e;
                        }
                        sleepQuietly(50L * attempt + ThreadLocalRandom.current().nextInt(50));
                    }
                }
            }
            throw new IllegalStateException("unreachable");
        });
    }

    private void updatePair(Connection c, int id) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag("chaos-deadlock",
                "UPDATE chaos.deadlock_pairs SET v = v + 1 WHERE id = ?"))) {
            ps.setInt(1, id);
            ps.executeUpdate();
        }
    }

    // ------------------------------------------------------------------ connection storm (S10)

    /**
     * Havuz DIŞI, kısa ömürlü fiziksel bağlantılar (application_name = &lt;servis&gt;-lab-storm). Her bağlantı
     * gerçek TCP + auth + backend fork maliyeti üretir ve holdMs boyunca bir slot işgal eder. Sunucu tarafı
     * sınırlar: rol CONNECTION LIMIT, max_connections.
     */
    public Map<String, Object> connectionStorm(int count, long holdMs) {
        int n = clamp(count, 1, props.maxStormConnections());
        long hold = Math.min(Math.max(0, holdMs), 10_000);
        String url = env.getRequiredProperty("spring.datasource.url")
                .replaceAll("ApplicationName=[^&]*", "ApplicationName=" + app + "-lab-storm");
        String user = env.getRequiredProperty("spring.datasource.username");
        String pw = env.getRequiredProperty("spring.datasource.password");
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        AtomicLong connectMs = new AtomicLong();
        List<Thread> ts = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ts.add(Thread.ofVirtual().start(() -> {
                long t0 = System.nanoTime();
                try (Connection c = DriverManager.getConnection(url, user, pw);
                     Statement st = c.createStatement()) {
                    connectMs.addAndGet((System.nanoTime() - t0) / 1_000_000);
                    st.execute(SqlTag.labTag("chaos-storm", "SELECT 1"));
                    ok.incrementAndGet();
                    sleepQuietly(hold);
                } catch (SQLException e) {
                    failed.incrementAndGet();
                }
            }));
        }
        for (Thread t : ts) {
            try {
                t.join(hold + 15_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (ok.get() == 0) {
            throw new BusinessException(HttpStatus.SERVICE_UNAVAILABLE, "STORM_ALL_FAILED",
                    "yeni bağlantıların hepsi reddedildi (" + failed.get() + ")");
        }
        return result("opened", ok.get(), "failed", failed.get(),
                "avgConnectMs", ok.get() == 0 ? 0 : connectMs.get() / ok.get(), "heldMs", hold);
    }

    // ------------------------------------------------------------------ table-level lock queue (S01)

    /**
     * mode=hold: chaos.lock_target tablosunu ACCESS EXCLUSIVE kilitleyip holdMs tutar (DDL / VACUUM FULL benzeri).
     * mode=read: aynı tabloyu okur; kilit tutuluyorsa relation lock kuyruğunda bekler (lock_timeout sunucu sınırı).
     */
    public Map<String, Object> tableLock(String mode, long holdMs) throws SQLException {
        boolean hold = "hold".equals(mode);
        long ms = Math.min(Math.max(0, holdMs), Math.min(props.maxLongTxMs(), 20_000));
        long gen = generation.get();
        return withPermit(() -> {
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(false);
                try {
                    long t0 = System.nanoTime();
                    long rows = 0;
                    if (hold) {
                        try (Statement st = c.createStatement()) {
                            st.execute(SqlTag.labTag("chaos-tablelock",
                                    "LOCK TABLE chaos.lock_target IN ACCESS EXCLUSIVE MODE"));
                        }
                    } else {
                        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(
                                SqlTag.labTag("chaos-tablelock", "SELECT count(*) FROM chaos.lock_target"))) {
                            rs.next();
                            rows = rs.getLong(1);
                        }
                    }
                    long lockWaitMs = (System.nanoTime() - t0) / 1_000_000;
                    if (hold) {
                        long end = System.nanoTime() + ms * 1_000_000;
                        while (System.nanoTime() < end && generation.get() == gen) {
                            sleepInTx(c, "chaos-tablelock", Math.min(1_000, Math.max(1, (end - System.nanoTime()) / 1_000_000)));
                        }
                    }
                    c.commit();
                    return result("mode", hold ? "hold" : "read", "lockWaitMs", lockWaitMs, "heldMs", hold ? ms : 0,
                            "rows", rows);
                } catch (SQLException e) {
                    c.rollback();
                    throw e;
                }
            }
        });
    }

    // ------------------------------------------------------------------ serialization conflicts (S08)

    /**
     * REPEATABLE READ transaction: satırı okur, holdMs bekler, günceller. Aynı satırı bu arada başka bir transaction
     * güncellediyse PostgreSQL 40001 (could not serialize access) verir; uygulama sınırlı retry yapar.
     */
    public Map<String, Object> serialization(int records, long holdMs, int retries) throws SQLException {
        int n = clamp(records, 1, 400);
        long hold = Math.min(Math.max(0, holdMs), 5_000);
        int maxAttempts = clamp(retries, 0, 5) + 1;
        int id = 500 + ThreadLocalRandom.current().nextInt(n);      // 0..499 row contention (S02) ile çakışmaz
        return withPermit(() -> {
            int conflicts = 0;
            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                try (Connection c = ds.getConnection()) {
                    int before = c.getTransactionIsolation();
                    c.setAutoCommit(false);
                    c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                    try {
                        try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag("chaos-serial",
                                "SELECT counter FROM chaos.shared_records WHERE id = ?"))) {
                            ps.setInt(1, id);
                            ps.executeQuery().close();
                        }
                        sleepInTx(c, "chaos-serial", hold);
                        try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag("chaos-serial",
                                "UPDATE chaos.shared_records SET counter = counter + 1, touched_by = ?, touched_at = now() WHERE id = ?"))) {
                            ps.setString(1, app);
                            ps.setInt(2, id);
                            ps.executeUpdate();
                        }
                        c.commit();
                        return result("record", id, "attempts", attempt, "conflicts", conflicts);
                    } catch (SQLException e) {
                        c.rollback();
                        if (!"40001".equals(e.getSQLState())) {
                            throw e;
                        }
                        conflicts++;
                        serializationFailures.incrementAndGet();
                        if (attempt == maxAttempts) {
                            throw e;
                        }
                        sleepQuietly(20L * attempt + ThreadLocalRandom.current().nextInt(30));
                    } finally {
                        c.setTransactionIsolation(before);       // bağlantı havuza varsayılan seviyeyle döner
                        c.setAutoCommit(true);
                    }
                }
            }
            throw new IllegalStateException("unreachable");
        });
    }

    // ------------------------------------------------------------------ commit storm / WAL flush (S11)

    /**
     * Her biri kendi transaction'ında commit edilen tek satırlık INSERT'ler: satır hacmi küçük, commit (WAL flush)
     * sayısı yüksek. Toplu INSERT'ten (write-burst) farkı budur.
     */
    public Map<String, Object> commitStorm(int commits, int payloadBytes) throws SQLException {
        int n = clamp(commits, 1, 2_000);
        int reps = clamp(payloadBytes / 32, 1, 16);
        long gen = generation.get();
        return withPermit(() -> {
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(true);
                long t0 = System.nanoTime();
                int done = 0;
                try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag("chaos-commit",
                        "INSERT INTO chaos.write_sink (app, experiment_id, payload) VALUES (?, ?, repeat(md5(random()::text), ?))"))) {
                    ps.setString(1, app);
                    ps.setString(2, MDC.get(RequestContextFilter.MDC_EXPERIMENT_ID));
                    ps.setInt(3, reps);
                    for (; done < n && generation.get() == gen; done++) {
                        ps.executeUpdate();
                    }
                }
                long total = writes.addAndGet(done);
                if (total / props.writeSinkKeep() != (total - done) / props.writeSinkKeep()) {
                    trimWriteSink(c, props.writeSinkKeep());
                }
                return result("commits", done, "ms", (System.nanoTime() - t0) / 1_000_000);
            }
        });
    }

    // ------------------------------------------------------------------ update churn / dead tuples (S12)

    /**
     * chaos.churn satırlarını toplu günceller (indeksli sütun değişir → HOT olmayan güncelleme): dead tuple, WAL ve
     * autovacuum işi üretir. Satırlar SKIP LOCKED ile seçilir; workload'lar birbirini row lock'ta bekletmez.
     */
    public Map<String, Object> updateChurn(int rows, int batches) throws SQLException {
        int n = clamp(rows, 1, 5_000);
        int b = clamp(batches, 1, 50);
        long gen = generation.get();
        return withPermit(() -> {
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(true);
                long t0 = System.nanoTime();
                long updated = 0;
                try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag("chaos-churn", """
                        UPDATE chaos.churn SET v = v + 1, pad = md5(random()::text), updated_at = now()
                         WHERE id IN (SELECT id FROM chaos.churn WHERE id >= ? ORDER BY id LIMIT ?
                                         FOR UPDATE SKIP LOCKED)"""))) {
                    for (int i = 0; i < b && generation.get() == gen; i++) {
                        ps.setInt(1, ThreadLocalRandom.current().nextInt(Math.max(1, CHURN_ROWS - n)));
                        ps.setInt(2, n);
                        updated += ps.executeUpdate();
                    }
                }
                return result("updated", updated, "ms", (System.nanoTime() - t0) / 1_000_000);
            }
        });
    }

    // ------------------------------------------------------------------ advisory lock contention (S13)

    /** Az sayıdaki advisory lock anahtarından birini transaction boyunca tutar; diğer oturumlar aynı anahtarı bekler. */
    public Map<String, Object> advisoryLock(int keys, long holdMs) throws SQLException {
        int k = ThreadLocalRandom.current().nextInt(clamp(keys, 1, 1_000));
        long hold = Math.min(Math.max(0, holdMs), 10_000);
        return withPermit(() -> {
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(false);
                try {
                    long t0 = System.nanoTime();
                    try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag("chaos-advisory",
                            "SELECT pg_advisory_xact_lock(?, ?)"))) {
                        ps.setInt(1, ADVISORY_CLASS);
                        ps.setInt(2, k);
                        ps.execute();
                    }
                    long lockWaitMs = (System.nanoTime() - t0) / 1_000_000;
                    sleepInTx(c, "chaos-advisory", hold);
                    c.commit();
                    return result("key", k, "lockWaitMs", lockWaitMs, "heldMs", hold);
                } catch (SQLException e) {
                    c.rollback();
                    throw e;
                }
            }
        });
    }

    // ------------------------------------------------------------------ idle in transaction (S14)

    /**
     * Havuz DIŞI fiziksel bağlantılar (application_name = &lt;servis&gt;-lab-idle) REPEATABLE READ transaction açıp
     * bir kez okur ve holdMs boyunca hiçbir şey yapmadan bekler: oturum "idle in transaction" kalır, bağlantı slotu
     * dolu durur ve snapshot (backend_xmin) tutulduğu için vacuum dead tuple'ları temizleyemez. Lock tutmaz.
     * Sunucu sınırı: rolün idle_in_transaction_session_timeout değeri (30 s).
     */
    public Map<String, Object> idleInTransaction(int sessions, long holdMs) {
        int n = clamp(sessions, 1, props.maxStormConnections());
        long hold = Math.min(Math.max(0, holdMs), 25_000);
        String url = env.getRequiredProperty("spring.datasource.url")
                .replaceAll("ApplicationName=[^&]*", "ApplicationName=" + app + "-lab-idle");
        String user = env.getRequiredProperty("spring.datasource.username");
        String pw = env.getRequiredProperty("spring.datasource.password");
        long gen = generation.get();
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        List<Thread> ts = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ts.add(Thread.ofVirtual().start(() -> {
                try (Connection c = DriverManager.getConnection(url, user, pw)) {
                    c.setAutoCommit(false);
                    c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                    try (Statement st = c.createStatement()) {
                        st.executeQuery(SqlTag.labTag("chaos-idle", "SELECT count(*) FROM chaos.shared_records")).close();
                    }
                    ok.incrementAndGet();
                    long end = System.nanoTime() + hold * 1_000_000;
                    while (System.nanoTime() < end && generation.get() == gen) {
                        sleepQuietly(200);
                    }
                    c.rollback();
                } catch (SQLException e) {
                    failed.incrementAndGet();
                }
            }));
        }
        for (Thread t : ts) {
            try {
                t.join(hold + 15_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (ok.get() == 0) {
            throw new BusinessException(HttpStatus.SERVICE_UNAVAILABLE, "IDLE_ALL_FAILED",
                    "bağlantıların hepsi reddedildi (" + failed.get() + ")");
        }
        return result("sessions", ok.get(), "failed", failed.get(), "heldMs", hold,
                "interrupted", generation.get() != gen);
    }

    // ------------------------------------------------------------------ parallel query fan-out (S15)

    /**
     * Tek sorgu için parallel query açılır (yalnız bu transaction): sorgu, istenen sayıda parallel worker süreci
     * ister. Eşzamanlı çağrılar sunucunun ortak parallel worker havuzunu (max_parallel_workers) tüketir.
     */
    public Map<String, Object> parallelQuery(int workers, int scanPct) throws SQLException {
        int w = clamp(workers, 1, 8);
        int pct = clamp(scanPct, 1, 100);
        return withPermit(() -> {
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(false);
                try (Statement st = c.createStatement()) {
                    st.execute("SET LOCAL max_parallel_workers_per_gather = " + w);
                    st.execute("SET LOCAL parallel_setup_cost = 0");
                    st.execute("SET LOCAL parallel_tuple_cost = 0");
                    st.execute("SET LOCAL min_parallel_table_scan_size = 0");
                    String sql = "SELECT count(*), max(md5(payload)) FROM chaos.synthetic_data WHERE k % 100 < " + pct;
                    long t0 = System.nanoTime();
                    int planned = 0;
                    // k üzerinde indeks yok → parallel seq scan. Plan, ANALYZE'sız EXPLAIN ile okunur (query çalışmaz);
                    // asıl query düz SELECT olarak gönderilir. EXPLAIN ANALYZE utility statement'tır: onunla
                    // çalıştırılan query pg_stat_statements'a yazılmaz (track_utility = off) ve yük, gerçek bir
                    // uygulamanın rapor query'sinden farklı olarak statement istatistiklerinde görünmez
                    // (2026-11-28 koşusunda görüldü). Havuz doluysa başlatılan worker sayısı planlanandan az olabilir.
                    try (ResultSet rs = st.executeQuery("EXPLAIN (COSTS OFF, FORMAT JSON) " + sql)) {
                        if (rs.next()) {
                            var m = WORKERS_PLANNED.matcher(rs.getString(1));
                            if (m.find()) {
                                planned = Integer.parseInt(m.group(1));
                            }
                        }
                    }
                    long rows = 0;
                    try (ResultSet rs = st.executeQuery(SqlTag.labTag("chaos-parallel", sql))) {
                        if (rs.next()) {
                            rows = rs.getLong(1);
                        }
                    }
                    c.commit();
                    return result("requestedWorkers", w, "plannedWorkers", planned, "scanPct", pct, "rows", rows,
                            "ms", (System.nanoTime() - t0) / 1_000_000);
                } catch (SQLException e) {
                    c.rollback();
                    throw e;
                }
            }
        });
    }

    // ------------------------------------------------------------------ point query flood (S16)

    /** Çok sayıda küçük primary key sorgusu (her biri ayrı transaction): sorgu başına iş az, sorgu sayısı yüksek. */
    public Map<String, Object> pointQueries(int queries) throws SQLException {
        int n = clamp(queries, 1, 5_000);
        long gen = generation.get();
        return withPermit(() -> {
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(true);
                long t0 = System.nanoTime();
                int done = 0;
                try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag("chaos-point",
                        "SELECT val FROM chaos.synthetic_data WHERE id = ?"))) {
                    for (; done < n && generation.get() == gen; done++) {
                        ps.setLong(1, ThreadLocalRandom.current().nextLong(400_000));
                        ps.executeQuery().close();
                    }
                }
                return result("queries", done, "ms", (System.nanoTime() - t0) / 1_000_000);
            }
        });
    }

    // ------------------------------------------------------------------ in-memory sort / work_mem (S18)

    /**
     * work_mem transaction içinde yükseltilip büyük bir sort bellekte yapılır: temp dosyası yazılmaz, backend
     * belleği büyür. (temp-spill'in tersi: orada work_mem düşürülür ve sort diske taşar.) work_mem veri boyutundan
     * küçük verilirse sort diske taşar; çağıran, kRows × 0,3 kB'tan büyük bir değer vermelidir.
     */
    public Map<String, Object> memorySort(int workMemMb, int kRows, long holdMs) throws SQLException {
        int mb = clamp(workMemMb, 4, 256);
        // sort bittikten sonra bellek, sorgu bitene dek tutulur: CPU harcamadan bellek baskısı (statement_timeout 15 s)
        double holdSec = Math.min(Math.max(0, holdMs), 10_000) / 1000.0;
        int rows = clamp(kRows, 10, 400) * 1_000;       // satır başına ≈ 0,3 kB → en fazla ≈ 120 MB
        return withPermit(() -> {
            try (Connection c = ds.getConnection()) {
                c.setAutoCommit(false);
                try (Statement st = c.createStatement()) {
                    st.execute("SET LOCAL work_mem = '" + mb + "MB'");
                    st.execute("SET LOCAL max_parallel_workers_per_gather = 0");
                    long t0 = System.nanoTime();
                    // window function tam sort ister (LIMIT'li top-N kısayolu yok); veri tabloya dokunmadan üretilir
                    try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag("chaos-memsort", """
                            SELECT max(rn), pg_sleep(?) FROM (
                              SELECT row_number() OVER (ORDER BY o COLLATE "C") AS rn
                                FROM (SELECT repeat(md5(g::text), 8) AS o FROM generate_series(1, ?) g) s
                            ) x"""))) {
                        ps.setDouble(1, holdSec);
                        ps.setInt(2, rows);
                        ps.executeQuery().close();
                    }
                    c.commit();
                    return result("sortedRows", rows, "workMemMb", mb, "heldMs", (long) (holdSec * 1000),
                            "ms", (System.nanoTime() - t0) / 1_000_000);
                } catch (SQLException e) {
                    c.rollback();
                    throw e;
                }
            }
        });
    }

    // ------------------------------------------------------------------ application CPU (senaryolarda kullanılmaz)

    /** JVM içinde sınırlı CPU burn (container cpus limitiyle gerçek CPU saturation). */
    public Map<String, Object> cpuBurn(long ms, int threads) {
        long dur = Math.min(Math.max(1, ms), props.maxCpuBurnMs());
        int want = clamp(threads, 1, props.maxCpuThreads());
        int got = 0;
        long gen = generation.get();
        List<Thread> ts = new ArrayList<>();
        for (int i = 0; i < want; i++) {
            if (burning.incrementAndGet() > props.maxCpuThreads()) {
                burning.decrementAndGet();
                break;
            }
            got++;
            ts.add(Thread.ofPlatform().name("chaos-burn-" + i).daemon(true).start(() -> {
                try {
                    long end = System.nanoTime() + dur * 1_000_000;
                    double x = 1.0;
                    while (System.nanoTime() < end && generation.get() == gen) {
                        for (int k = 0; k < 20_000; k++) {
                            x = Math.sqrt(x * 1.000001 + k) % 1_000_003;
                        }
                    }
                    if (x == -1) {
                        log.trace("unreachable {}", x);
                    }
                } finally {
                    burning.decrementAndGet();
                }
            }));
        }
        if (got == 0) {
            throw new BusinessException(HttpStatus.TOO_MANY_REQUESTS, "CPU_BURN_LIMIT", "CPU burn thread limiti dolu");
        }
        for (Thread t : ts) {
            try {
                t.join(dur + 2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return result("threads", got, "ms", dur);
    }

    // ------------------------------------------------------------------ JVM memory (senaryolarda kullanılmaz)

    /** Gerçek heap ballast: mb kadar dokunulmuş byte[] holdMs tutulur; toplam max heap oranıyla sınırlı. */
    public Map<String, Object> memory(int mb, long holdMs) {
        long max = Runtime.getRuntime().maxMemory();
        long cap = (long) (max * props.maxMemoryFraction());
        long want = (long) clamp(mb, 1, 4_096) * MB;
        long allowed = Math.max(0, Math.min(want, cap - ballastBytes.get()));
        if (allowed < MB) {
            throw new BusinessException(HttpStatus.TOO_MANY_REQUESTS, "MEMORY_LIMIT",
                    "ballast limiti dolu (" + ballastBytes.get() / MB + " / " + cap / MB + " MB)");
        }
        long hold = Math.min(Math.max(0, holdMs), props.maxMemoryHoldMs());
        List<byte[]> mine = new ArrayList<>();
        for (long done = 0; done + BALLAST_CHUNK <= allowed; done += BALLAST_CHUNK) {
            byte[] chunk = new byte[BALLAST_CHUNK];
            for (int i = 0; i < chunk.length; i += 4096) {
                chunk[i] = (byte) i;               // sayfaları gerçekten dokun (RSS)
            }
            mine.add(chunk);
        }
        long bytes = (long) mine.size() * BALLAST_CHUNK;
        ballast.addAll(mine);
        ballastBytes.addAndGet(bytes);
        long gen = generation.get();
        Thread.ofVirtual().start(() -> {
            sleepQuietly(hold);
            if (generation.get() == gen) {
                release(mine, bytes);
            }
        });
        return result("allocatedMb", bytes / MB, "retainedMb", ballastBytes.get() / MB, "capMb", cap / MB, "holdMs", hold);
    }

    private void release(List<byte[]> mine, long bytes) {
        if (ballast.removeAll(mine)) {
            ballastBytes.addAndGet(-bytes);
        }
    }

    // ------------------------------------------------------------------ app → app dependency (senaryolarda kullanılmaz)

    private static final Map<String, String> PEER_PATHS = Map.of(
            "reservation", "/api/flights/hot",
            "checkin", "/api/checkin/candidates?limit=20",
            "flightops", "/api/flights/sample?limit=20",
            "baggage", "/api/bags/sample?limit=20");

    /**
     * Allowlist'teki bir peer uygulamanın DB'ye dokunan okuma ucunu çağırır; timeout veya 5xx'te retry eder.
     * Döndürülen attempts/calls oranı retry amplification'dır. Bütün denemeler başarısızsa 502 döner.
     */
    public Map<String, Object> dependencyCall(String peer, int timeoutMs, int retries, long backoffMs) {
        String base = props.peers().get(peer);
        String path = PEER_PATHS.get(peer);
        if (base == null || path == null) {
            throw BusinessException.badRequest("PEER_NOT_ALLOWED", "peer allowlist'te değil: " + peer);
        }
        int maxAttempts = clamp(retries, 0, props.maxDependencyAttempts() - 1) + 1;
        int to = clamp(timeoutMs, 100, 15_000);
        long backoff = Math.min(Math.max(0, backoffMs), 2_000);
        long t0 = System.nanoTime();
        String last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            var b = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofMillis(to)).GET()
                    .header("X-Request-Id", "dep-" + ThreadLocalRandom.current().nextLong(1L << 40));
            String exp = MDC.get(RequestContextFilter.MDC_EXPERIMENT_ID);
            if (exp != null) {
                b.header(RequestContextFilter.EXPERIMENT_ID, exp);
            }
            try {
                HttpResponse<Void> res = http.send(b.build(), HttpResponse.BodyHandlers.discarding());
                if (res.statusCode() < 400) {
                    return result("peer", peer, "attempts", attempt, "status", res.statusCode(),
                            "ms", (System.nanoTime() - t0) / 1_000_000);
                }
                last = "HTTP " + res.statusCode();
            } catch (java.net.http.HttpTimeoutException e) {
                last = "timeout";
            } catch (Exception e) {
                last = e.getClass().getSimpleName();
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (attempt < maxAttempts && backoff > 0) {
                sleepQuietly(backoff * attempt);
            }
        }
        throw new BusinessException(HttpStatus.BAD_GATEWAY, "DEPENDENCY_FAILED",
                peer + " " + maxAttempts + " denemede başarısız (son: " + last + ")");
    }

    // ------------------------------------------------------------------ cleanup / status

    /** İdempotent: burn ve long tx döngülerini bitirir, ballast'ı bırakır, write_sink'i boşaltır. */
    public Map<String, Object> cleanup() {
        generation.incrementAndGet();
        long freed = ballastBytes.getAndSet(0);
        ballast.clear();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ballastFreedMb", freed / MB);
        out.put("burningThreads", burning.get());
        try (Connection c = ds.getConnection()) {
            out.put("writeSinkTrimmed", trimWriteSink(c, 0));
        } catch (SQLException e) {
            out.put("writeSinkTrimmed", -1);
            out.put("writeSinkError", e.getSQLState());
        }
        return out;
    }

    public Map<String, Object> status() {
        Runtime rt = Runtime.getRuntime();
        return result("activeOps", props.maxConcurrentOps() - permits.availablePermits(),
                "maxOps", props.maxConcurrentOps(), "burningThreads", burning.get(), "longTransactions", longTx.get(),
                "ballastMb", ballastBytes.get() / MB, "heapUsedMb", (rt.totalMemory() - rt.freeMemory()) / MB,
                "heapMaxMb", rt.maxMemory() / MB, "deadlocksSeen", deadlocksSeen.get(),
                "serializationFailures", serializationFailures.get(),
                "peers", props.peers().keySet());
    }

    // ------------------------------------------------------------------ yardımcılar

    private long trimWriteSink(Connection c, int keep) throws SQLException {
        boolean auto = c.getAutoCommit();
        c.setAutoCommit(true);
        try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag("chaos-trim", "SELECT chaos.trim_write_sink(?)"))) {
            ps.setInt(1, keep);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } finally {
            c.setAutoCommit(auto);
        }
    }

    private Map<String, Object> timedQuery(String kind, String sql, int param, int errorPct) throws SQLException {
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try {
                long t0 = System.nanoTime();
                serialOnly(c);
                try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag(kind, sql))) {
                    ps.setInt(1, param);
                    ps.executeQuery().close();
                }
                maybeFail(c, errorPct);
                c.commit();
                return result("rows", param, "ms", (System.nanoTime() - t0) / 1_000_000);
            } catch (SQLException e) {
                c.rollback();
                throw e;
            }
        }
    }

    /**
     * Parallel query kapalı (yalnız bu transaction): aksi halde tek chaos sorgusu max_parallel_workers_per_gather
     * kadar ek süreçle ~3 vCPU kullanır ve adaptive controller'ın en küçük adımı (1 worker) DB CPU'yu hedefin
     * çok üstüne taşır (ölçüldü: 1–2 worker → %96). Seri sorguda 1 worker ≈ 1 vCPU.
     */
    private static void serialOnly(Connection c) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute("SET LOCAL max_parallel_workers_per_gather = 0");
        }
    }

    private static void sleepInTx(Connection c, String kind, long ms) throws SQLException {
        if (ms <= 0) {
            return;
        }
        try (PreparedStatement ps = c.prepareStatement(SqlTag.labTag(kind, "SELECT pg_sleep(?)"))) {
            ps.setDouble(1, ms / 1000.0);
            ps.execute();
        }
    }

    /** errorPct olasılıkla GERÇEK bir SQL hatası üretir (22012 division_by_zero) → çağıran rollback eder. */
    private static void maybeFail(Connection c, int errorPct) throws SQLException {
        if (errorPct > 0 && ThreadLocalRandom.current().nextInt(100) < Math.min(errorPct, 100)) {
            try (Statement st = c.createStatement()) {
                st.execute(SqlTag.labTag("chaos-error", "SELECT 1 / 0"));
            }
        }
    }

    @FunctionalInterface
    private interface SqlWork {
        Map<String, Object> run() throws SQLException;
    }

    private Map<String, Object> withPermit(SqlWork w) throws SQLException {
        if (!permits.tryAcquire()) {
            throw new BusinessException(HttpStatus.TOO_MANY_REQUESTS, "CHAOS_OPS_LIMIT",
                    "eşzamanlı chaos operasyonu limiti dolu (" + props.maxConcurrentOps() + ")");
        }
        try {
            return w.run();
        } finally {
            permits.release();
        }
    }

    private static Map<String, Object> result(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    private static int clamp(int v, int lo, int hi) {
        return Math.max(lo, Math.min(hi, v));
    }

    private static void sleepQuietly(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
