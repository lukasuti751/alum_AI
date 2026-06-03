#!/usr/bin/env python3
"""Generate contracts/alum__AI.java — hive-mind off-chain EVM alignment engine."""

from __future__ import annotations

import secrets
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "contracts" / "alum__AI.java"

# Pre-generated unique values (mixed-case addresses / hex — not reused from prior contracts)
ADDRESS_A = "0xF44F57B28AC1A4EACacB23515b3613C0d0C0c954"
ADDRESS_B = "0x106127E223C005DB56152E162615990Df25ADc92"
ADDRESS_C = "0x434E67584dB27132de9D9D93dea14864d0CE92D9"
ADDRESS_D = "0x014465b28D6E46C7221393D4931375c410C67b14"
ADDRESS_E = "0x26c5cb544c39EE7F6f1269E16DF4f73fedB3BC71"
ADDRESS_F = "0x2920517a28D10Ae12756D1F77e2739A01c02CcA7"
ADDRESS_G = "0xD7519Cc678e0f283e45EF193E0e7908695F90A4E"
ADDRESS_H = "0x2c8dC6E72b9BcD06537EB5f51b465E86A53f8875"
SWARM_DOMAIN_HEX = "0x5f0DB1AD46fB9cBBEa57F783464A1f702E96DD42DB2Ae3329f73aFd9Bb44Da59"
PHEROMONE_SALT = "0x83FAe5e4fF9b185e169fDbAD1daAE5BBEAfCA8A2753A2E0AF4e4db463a8571ae"
MESH_SEED_HEX = "0xABFcBBDe20B33cEb1bCF6ecF59Fee7faa2FdbFf2386d42c3E84CcaBfA3EAEBad"
QUORUM_ANCHOR_HEX = "0xfFD91a92f3B9AeD2cd955FF83fd546f38AD62e5b8EEb7CE7452bE86044C5f1e1"

HEADER = f'''/*
 * alum__AI — distributed cognition mesh for swarm-weighted inference rounds.
 * Codename: nectar-quorum. Calibrated against chain id 1 attestation envelopes.
 * Mesh anchor: {MESH_SEED_HEX}
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Off-chain hive-mind engine: swarm node registry, pheromone routing, thought pools,
 * quorum ballots, spore memory vault, and EVM attestation relay for alum__AI deployments.
 */
public final class alum__AI {{

    public static final String ENGINE_LABEL = "alum__AI";
    public static final String RELEASE_TAG = "nectar-quorum-v3.7";
    public static final int MAX_SWARM_NODES = 768;
    public static final int MAX_THOUGHT_SLOTS = 4096;
    public static final int MAX_PHEROMONE_TRAILS = 2048;
    public static final int MAX_QUORUM_ROUNDS = 512;
    public static final int MAX_SPORE_FRAGMENTS = 1024;
    public static final int MAX_TASK_QUEUE = 256;
    public static final int METRIC_RING_CAP = 8192;
    public static final int ATTESTATION_TTL_SECONDS = 172800;
    public static final int REWARD_BASIS_POINTS = 63;
    public static final long BPS_DENOM = 10_000L;
    public static final long DEFAULT_CHAIN_ID = 1L;
    public static final String DOMAIN_SEPARATOR = "alum__AI_nectar_quorum_v3";
    public static final String DIGEST_ALGORITHM = "SHA-256";
    public static final int MIN_QUORUM_WEIGHT = 3;
    public static final int MAX_QUORUM_WEIGHT = 97;

    public static final String ADDRESS_A = "{ADDRESS_A}";
    public static final String ADDRESS_B = "{ADDRESS_B}";
    public static final String ADDRESS_C = "{ADDRESS_C}";
    public static final String ADDRESS_D = "{ADDRESS_D}";
    public static final String ADDRESS_E = "{ADDRESS_E}";
    public static final String ADDRESS_F = "{ADDRESS_F}";
    public static final String ADDRESS_G = "{ADDRESS_G}";
    public static final String ADDRESS_H = "{ADDRESS_H}";
    public static final String SWARM_DOMAIN_HEX = "{SWARM_DOMAIN_HEX}";
    public static final String PHEROMONE_SALT_HEX = "{PHEROMONE_SALT}";
    public static final String MESH_SEED_HEX = "{MESH_SEED_HEX}";
    public static final String QUORUM_ANCHOR_HEX = "{QUORUM_ANCHOR_HEX}";

    private final HiveRuntimeConfig runtimeConfig;
    private final SwarmNodeRegistry swarmNodeRegistry;
    private final ThoughtPool thoughtPool;
    private final PheromoneTrailIndex pheromoneTrailIndex;
    private final ConsensusMesh consensusMesh;
    private final MemorySporeVault memorySporeVault;
    private final SwarmScheduler swarmScheduler;
    private final AttestationRelay attestationRelay;
    private final HiveLedger hiveLedger;
    private final HiveMetricsRing hiveMetricsRing;
    private final HiveValidator hiveValidator;
    private final HiveReportEmitter hiveReportEmitter;
    private final AtomicBoolean gridFrozen;
    private final AtomicLong hiveEpoch;
    private final AtomicReference<String> pendingSwarmMarshal;
    private final Instant bootInstant;

    public alum__AI(HiveRuntimeConfig runtimeConfig) {{
        this.runtimeConfig = Objects.requireNonNull(runtimeConfig, "runtimeConfig");
        this.swarmNodeRegistry = new SwarmNodeRegistry(MAX_SWARM_NODES);
        this.thoughtPool = new ThoughtPool(MAX_THOUGHT_SLOTS);
        this.pheromoneTrailIndex = new PheromoneTrailIndex(MAX_PHEROMONE_TRAILS);
        this.consensusMesh = new ConsensusMesh(MAX_QUORUM_ROUNDS, MIN_QUORUM_WEIGHT, MAX_QUORUM_WEIGHT);
        this.memorySporeVault = new MemorySporeVault(MAX_SPORE_FRAGMENTS);
        this.swarmScheduler = new SwarmScheduler(MAX_TASK_QUEUE);
        this.attestationRelay = new AttestationRelay(runtimeConfig);
        this.hiveLedger = new HiveLedger();
        this.hiveMetricsRing = new HiveMetricsRing(METRIC_RING_CAP);
        this.hiveValidator = new HiveValidator();
        this.hiveReportEmitter = new HiveReportEmitter();
        this.gridFrozen = new AtomicBoolean(false);
        this.hiveEpoch = new AtomicLong(0L);
        this.pendingSwarmMarshal = new AtomicReference<>(null);
        this.bootInstant = Instant.now();
    }}

    public static alum__AI bootstrapDefault() {{
        HiveRuntimeConfig cfg = new HiveRuntimeConfig(
                DEFAULT_CHAIN_ID,
                ADDRESS_A,
                ADDRESS_B,
                ADDRESS_C,
                ADDRESS_D,
                ADDRESS_E,
                SWARM_DOMAIN_HEX,
                RELEASE_TAG
        );
        return new alum__AI(cfg);
    }}

    public HiveRuntimeConfig getRuntimeConfig() {{ return runtimeConfig; }}
    public SwarmNodeRegistry nodes() {{ return swarmNodeRegistry; }}
    public ThoughtPool thoughts() {{ return thoughtPool; }}
    public PheromoneTrailIndex trails() {{ return pheromoneTrailIndex; }}
    public ConsensusMesh quorum() {{ return consensusMesh; }}
    public MemorySporeVault spores() {{ return memorySporeVault; }}
    public SwarmScheduler scheduler() {{ return swarmScheduler; }}
    public AttestationRelay attestation() {{ return attestationRelay; }}
    public HiveLedger ledger() {{ return hiveLedger; }}
    public HiveMetricsRing metrics() {{ return hiveMetricsRing; }}
    public HiveValidator validator() {{ return hiveValidator; }}
    public HiveReportEmitter reports() {{ return hiveReportEmitter; }}

    public boolean isGridFrozen() {{ return gridFrozen.get(); }}

    public void setGridFrozen(boolean frozen, String actorAddress) {{
        hiveValidator.requireHiveKeeper(actorAddress, runtimeConfig.getHiveKeeperAddress());
        gridFrozen.set(frozen);
        hiveLedger.append(new HiveEvent(
                frozen ? "GridFrozen" : "GridThawed",
                actorAddress,
                hiveEpoch.get(),
                Instant.now(),
                Map.of("release", RELEASE_TAG)
        ));
    }}

    public void proposeSwarmMarshal(String nextMarshal, String actorAddress) {{
        hiveValidator.requireHiveKeeper(actorAddress, runtimeConfig.getHiveKeeperAddress());
        hiveValidator.requireValidAddress(nextMarshal);
        pendingSwarmMarshal.set(nextMarshal.trim());
        hiveLedger.append(new HiveEvent("MarshalPending", actorAddress, hiveEpoch.get(), Instant.now(),
                Map.of("candidate", nextMarshal)));
    }}

    public void acceptSwarmMarshal(String actorAddress) {{
        String candidate = pendingSwarmMarshal.get();
        if (candidate == null) {{
            throw new AlumHive_NoPendingMarshalException();
        }}
        hiveValidator.requireAddressMatch(actorAddress, candidate);
        runtimeConfig.assignSwarmMarshal(candidate);
        pendingSwarmMarshal.set(null);
        hiveLedger.append(new HiveEvent("MarshalAccepted", actorAddress, hiveEpoch.get(), Instant.now(),
                Map.of("marshal", candidate)));
    }}

    public long tickHiveEpoch() {{
        long next = hiveEpoch.incrementAndGet();
        hiveMetricsRing.recordGauge("hiveEpoch", next);
        return next;
    }}

    public long currentHiveEpoch() {{ return hiveEpoch.get(); }}
    public Instant getBootInstant() {{ return bootInstant; }}

    public void requireActiveGrid() {{
        if (gridFrozen.get()) {{
            throw new AlumHive_GridFrozenException();
        }}
    }}

    public String computeHiveDigest(String swarmTag, String thoughtKey, byte[] payload) {{
        try {{
            MessageDigest md = MessageDigest.getInstance(DIGEST_ALGORITHM);
            md.update(runtimeConfig.getDomainSeed());
            md.update(swarmTag.getBytes(StandardCharsets.UTF_8));
            md.update(thoughtKey.getBytes(StandardCharsets.UTF_8));
            if (payload != null) {{
                md.update(payload);
            }}
            return "0x" + HexFormat.of().formatHex(md.digest());
        }} catch (NoSuchAlgorithmException e) {{
            throw new AlumHive_DigestFailureException(e);
        }}
    }}

    public Map<String, Object> buildHealthSnapshot() {{
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("engine", ENGINE_LABEL);
        snap.put("release", RELEASE_TAG);
        snap.put("chainId", runtimeConfig.getChainId());
        snap.put("hiveEpoch", hiveEpoch.get());
        snap.put("gridFrozen", gridFrozen.get());
        snap.put("swarmNodes", swarmNodeRegistry.size());
        snap.put("thoughts", thoughtPool.size());
        snap.put("trails", pheromoneTrailIndex.size());
        snap.put("openQuorums", consensusMesh.openRoundCount());
        snap.put("spores", memorySporeVault.size());
        snap.put("pendingTasks", swarmScheduler.pendingCount());
        snap.put("bootUtc", bootInstant.toString());
        snap.put("metricSamples", hiveMetricsRing.sampleCount());
        return snap;
    }}
'''


def emit_runtime_config() -> str:
    return '''
    // --- Runtime configuration (constructor-injected final fields) ---

    public static final class HiveRuntimeConfig {
        private final long chainId;
        private final String hiveKeeperAddress;
        private String swarmMarshalAddress;
        private final String nectarOracleAddress;
        private final String relayAddress;
        private final String attestationSinkAddress;
        private final String swarmDomainHex;
        private final String versionTag;
        private final byte[] domainSeed;

        public HiveRuntimeConfig(
                long chainId,
                String hiveKeeperAddress,
                String swarmMarshalAddress,
                String nectarOracleAddress,
                String relayAddress,
                String attestationSinkAddress,
                String swarmDomainHex,
                String versionTag
        ) {
            this.chainId = chainId;
            this.hiveKeeperAddress = normalizeAddress(hiveKeeperAddress);
            this.swarmMarshalAddress = normalizeAddress(swarmMarshalAddress);
            this.nectarOracleAddress = normalizeAddress(nectarOracleAddress);
            this.relayAddress = normalizeAddress(relayAddress);
            this.attestationSinkAddress = normalizeAddress(attestationSinkAddress);
            this.swarmDomainHex = swarmDomainHex == null ? "" : swarmDomainHex.trim();
            this.versionTag = versionTag == null ? RELEASE_TAG : versionTag;
            this.domainSeed = buildDomainSeed(this.chainId, this.swarmDomainHex, this.versionTag);
        }

        private static byte[] buildDomainSeed(long chainId, String domainHex, String version) {
            try {
                MessageDigest md = MessageDigest.getInstance(DIGEST_ALGORITHM);
                md.update(DOMAIN_SEPARATOR.getBytes(StandardCharsets.UTF_8));
                md.update(ByteBuffer.allocate(8).putLong(chainId).array());
                md.update(domainHex.getBytes(StandardCharsets.UTF_8));
                md.update(version.getBytes(StandardCharsets.UTF_8));
                return md.digest();
            } catch (NoSuchAlgorithmException e) {
                throw new AlumHive_DigestFailureException(e);
            }
        }

        private static String normalizeAddress(String addr) {
            if (addr == null || addr.isBlank()) {
                throw new AlumHive_InvalidAddressException("empty address");
            }
            String trimmed = addr.trim();
            if (!trimmed.startsWith("0x") || trimmed.length() != 42) {
                throw new AlumHive_InvalidAddressException(trimmed);
            }
            return trimmed;
        }

        void assignSwarmMarshal(String next) {
            this.swarmMarshalAddress = normalizeAddress(next);
        }

        public long getChainId() { return chainId; }
        public String getHiveKeeperAddress() { return hiveKeeperAddress; }
        public String getSwarmMarshalAddress() { return swarmMarshalAddress; }
        public String getNectarOracleAddress() { return nectarOracleAddress; }
        public String getRelayAddress() { return relayAddress; }
        public String getAttestationSinkAddress() { return attestationSinkAddress; }
        public String getSwarmDomainHex() { return swarmDomainHex; }
        public String getVersionTag() { return versionTag; }
        public byte[] getDomainSeed() { return Arrays.copyOf(domainSeed, domainSeed.length); }
    }
'''


def emit_exceptions() -> str:
    return '''
    // --- Domain exceptions ---

    public static class AlumHive_GridFrozenException extends RuntimeException {
        public AlumHive_GridFrozenException() { super("AlumHive: grid frozen"); }
    }

    public static class AlumHive_CapacityExceededException extends RuntimeException {
        public AlumHive_CapacityExceededException(String detail) {
            super("AlumHive: capacity exceeded — " + detail);
        }
    }

    public static class AlumHive_NotFoundException extends RuntimeException {
        public AlumHive_NotFoundException(String id) {
            super("AlumHive: record not found — " + id);
        }
    }

    public static class AlumHive_InvalidAddressException extends RuntimeException {
        public AlumHive_InvalidAddressException(String addr) {
            super("AlumHive: invalid address — " + addr);
        }
    }

    public static class AlumHive_UnauthorizedException extends RuntimeException {
        public AlumHive_UnauthorizedException(String role) {
            super("AlumHive: unauthorized — " + role);
        }
    }

    public static class AlumHive_DigestFailureException extends RuntimeException {
        public AlumHive_DigestFailureException(Throwable cause) {
            super("AlumHive: digest failure", cause);
        }
    }

    public static class AlumHive_QuorumNotReachedException extends RuntimeException {
        public AlumHive_QuorumNotReachedException(long roundId) {
            super("AlumHive: quorum not reached — round " + roundId);
        }
    }

    public static class AlumHive_NoPendingMarshalException extends RuntimeException {
        public AlumHive_NoPendingMarshalException() {
            super("AlumHive: no pending swarm marshal");
        }
    }

    public static class AlumHive_TrailExpiredException extends RuntimeException {
        public AlumHive_TrailExpiredException(long trailId) {
            super("AlumHive: pheromone trail expired — " + trailId);
        }
    }
'''

