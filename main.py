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


def emit_swarm_node_registry() -> str:
    return '''
    // --- Swarm node registry ---

    public enum SwarmNodeTier {
        SCOUT, WORKER, SYNTHESIZER, ORACLE_LIAISON
    }

    public enum SwarmNodeStatus {
        IDLE, BUSY, QUARANTINED, RETIRED
    }

    public static final class SwarmNode {
        private final long nodeId;
        private final String alias;
        private final String operatorAddress;
        private final SwarmNodeTier tier;
        private final int computeWeight;
        private final Instant enlistedAt;
        private SwarmNodeStatus status;
        private long lastHeartbeatEpoch;
        private final Set<Long> boundThoughtIds;

        public SwarmNode(long nodeId, String alias, String operatorAddress, SwarmNodeTier tier, int computeWeight) {
            this.nodeId = nodeId;
            this.alias = alias == null ? "node-" + nodeId : alias;
            this.operatorAddress = operatorAddress;
            this.tier = tier == null ? SwarmNodeTier.WORKER : tier;
            this.computeWeight = Math.max(1, Math.min(64, computeWeight));
            this.enlistedAt = Instant.now();
            this.status = SwarmNodeStatus.IDLE;
            this.lastHeartbeatEpoch = 0L;
            this.boundThoughtIds = new LinkedHashSet<>();
        }

        public long getNodeId() { return nodeId; }
        public String getAlias() { return alias; }
        public String getOperatorAddress() { return operatorAddress; }
        public SwarmNodeTier getTier() { return tier; }
        public int getComputeWeight() { return computeWeight; }
        public Instant getEnlistedAt() { return enlistedAt; }
        public SwarmNodeStatus getStatus() { return status; }
        public long getLastHeartbeatEpoch() { return lastHeartbeatEpoch; }
        public Set<Long> getBoundThoughtIds() { return Collections.unmodifiableSet(boundThoughtIds); }

        void setStatus(SwarmNodeStatus status) { this.status = status; }
        void touchHeartbeat(long epoch) { this.lastHeartbeatEpoch = epoch; }
        void bindThought(long thoughtId) { boundThoughtIds.add(thoughtId); }
        void unbindThought(long thoughtId) { boundThoughtIds.remove(thoughtId); }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("nodeId", nodeId);
            m.put("alias", alias);
            m.put("operator", operatorAddress);
            m.put("tier", tier.name());
            m.put("weight", computeWeight);
            m.put("status", status.name());
            m.put("heartbeatEpoch", lastHeartbeatEpoch);
            m.put("thoughtBindings", boundThoughtIds.size());
            return m;
        }
    }

    public static final class SwarmNodeRegistry {
        private final int capacity;
        private final AtomicLong idSeq = new AtomicLong(0L);
        private final Map<Long, SwarmNode> nodes = new ConcurrentHashMap<>();

        public SwarmNodeRegistry(int capacity) {
            this.capacity = Math.max(1, capacity);
        }

        public int size() { return nodes.size(); }
        public int getCapacity() { return capacity; }

        public long enlist(String alias, String operatorAddress, SwarmNodeTier tier, int computeWeight) {
            if (nodes.size() >= capacity) {
                throw new AlumHive_CapacityExceededException("swarm nodes");
            }
            long id = idSeq.incrementAndGet();
            SwarmNode node = new SwarmNode(id, alias, operatorAddress, tier, computeWeight);
            nodes.put(id, node);
            return id;
        }

        public SwarmNode requireNode(long nodeId) {
            SwarmNode node = nodes.get(nodeId);
            if (node == null) {
                throw new AlumHive_NotFoundException("node:" + nodeId);
            }
            return node;
        }

        public Optional<SwarmNode> get(long nodeId) {
            return Optional.ofNullable(nodes.get(nodeId));
        }

        public void heartbeat(long nodeId, long epoch) {
            requireNode(nodeId).touchHeartbeat(epoch);
        }

        public void quarantine(long nodeId, String actorAddress) {
            SwarmNode node = requireNode(nodeId);
            node.setStatus(SwarmNodeStatus.QUARANTINED);
        }

        public void retire(long nodeId) {
            SwarmNode node = requireNode(nodeId);
            node.setStatus(SwarmNodeStatus.RETIRED);
        }

        public List<SwarmNode> listByTier(SwarmNodeTier tier) {
            return nodes.values().stream()
                    .filter(n -> n.getTier() == tier && n.getStatus() != SwarmNodeStatus.RETIRED)
                    .sorted(Comparator.comparingLong(SwarmNode::getNodeId))
                    .collect(Collectors.toList());
        }

        public Map<Long, SwarmNode> snapshot() {
            return Collections.unmodifiableMap(new TreeMap<>(nodes));
        }

        public int totalActiveWeight() {
            return nodes.values().stream()
                    .filter(n -> n.getStatus() != SwarmNodeStatus.RETIRED && n.getStatus() != SwarmNodeStatus.QUARANTINED)
                    .mapToInt(SwarmNode::getComputeWeight)
                    .sum();
        }
    }
'''


def emit_thought_pool() -> str:
    return '''
    // --- Thought pool (shared cognition fragments) ---

    public enum ThoughtPhase {
        RAW, REFINED, CONSENSUS_READY, ARCHIVED
    }

    public static final class ThoughtFragment {
        private final long thoughtId;
        private final String swarmTag;
        private final String contentDigest;
        private final String authorAddress;
        private final Instant depositedAt;
        private ThoughtPhase phase;
        private int refinementScore;
        private final List<Long> upstreamThoughtIds;

        public ThoughtFragment(
                long thoughtId,
                String swarmTag,
                String contentDigest,
                String authorAddress,
                List<Long> upstreamThoughtIds
        ) {
            this.thoughtId = thoughtId;
            this.swarmTag = swarmTag == null ? "default" : swarmTag;
            this.contentDigest = contentDigest == null ? "" : contentDigest;
            this.authorAddress = authorAddress;
            this.depositedAt = Instant.now();
            this.phase = ThoughtPhase.RAW;
            this.refinementScore = 0;
            this.upstreamThoughtIds = upstreamThoughtIds == null
                    ? new ArrayList<>()
                    : new ArrayList<>(upstreamThoughtIds);
        }

        public long getThoughtId() { return thoughtId; }
        public String getSwarmTag() { return swarmTag; }
        public String getContentDigest() { return contentDigest; }
        public String getAuthorAddress() { return authorAddress; }
        public Instant getDepositedAt() { return depositedAt; }
        public ThoughtPhase getPhase() { return phase; }
        public int getRefinementScore() { return refinementScore; }
        public List<Long> getUpstreamThoughtIds() { return Collections.unmodifiableList(upstreamThoughtIds); }

        void advancePhase(ThoughtPhase next) { this.phase = next; }
        void addRefinement(int delta) { this.refinementScore = Math.min(1000, refinementScore + delta); }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("thoughtId", thoughtId);
            m.put("swarmTag", swarmTag);
            m.put("digest", contentDigest);
            m.put("author", authorAddress);
            m.put("phase", phase.name());
            m.put("refinement", refinementScore);
            m.put("upstream", upstreamThoughtIds);
            return m;
        }
    }

    public static final class ThoughtPool {
        private final int capacity;
        private final AtomicLong idSeq = new AtomicLong(0L);
        private final Map<Long, ThoughtFragment> fragments = new ConcurrentHashMap<>();
        private final Map<String, List<Long>> bySwarmTag = new ConcurrentHashMap<>();

        public ThoughtPool(int capacity) {
            this.capacity = Math.max(1, capacity);
        }

        public int size() { return fragments.size(); }

        public long deposit(String swarmTag, String contentDigest, String authorAddress, List<Long> upstream) {
            if (fragments.size() >= capacity) {
                throw new AlumHive_CapacityExceededException("thought pool");
            }
            long id = idSeq.incrementAndGet();
            ThoughtFragment frag = new ThoughtFragment(id, swarmTag, contentDigest, authorAddress, upstream);
            fragments.put(id, frag);
            bySwarmTag.computeIfAbsent(swarmTag, k -> new CopyOnWriteArrayList<>()).add(id);
            return id;
        }

        public ThoughtFragment requireThought(long thoughtId) {
            ThoughtFragment f = fragments.get(thoughtId);
            if (f == null) {
                throw new AlumHive_NotFoundException("thought:" + thoughtId);
            }
            return f;
        }

        public void refine(long thoughtId, int scoreDelta) {
            ThoughtFragment f = requireThought(thoughtId);
            f.addRefinement(scoreDelta);
            if (f.getRefinementScore() >= 120) {
                f.advancePhase(ThoughtPhase.REFINED);
            }
            if (f.getRefinementScore() >= 480) {
                f.advancePhase(ThoughtPhase.CONSENSUS_READY);
            }
        }

        public void archive(long thoughtId) {
            requireThought(thoughtId).advancePhase(ThoughtPhase.ARCHIVED);
        }

        public List<ThoughtFragment> listByTag(String swarmTag) {
            List<Long> ids = bySwarmTag.getOrDefault(swarmTag, List.of());
            return ids.stream()
                    .map(fragments::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        public List<ThoughtFragment> listConsensusReady() {
            return fragments.values().stream()
                    .filter(f -> f.getPhase() == ThoughtPhase.CONSENSUS_READY)
                    .sorted(Comparator.comparingLong(ThoughtFragment::getThoughtId))
                    .collect(Collectors.toList());
        }
    }
'''


def emit_pheromone() -> str:
    return '''
    // --- Pheromone trail index (signal routing) ---

    public static final class PheromoneTrail {
        private final long trailId;
        private final long sourceNodeId;
        private final long targetNodeId;
        private final String signalDigest;
        private final int intensity;
        private final Instant laidAt;
        private final long expiryEpoch;
        private boolean evaporated;

        public PheromoneTrail(
                long trailId,
                long sourceNodeId,
                long targetNodeId,
                String signalDigest,
                int intensity,
                long expiryEpoch
        ) {
            this.trailId = trailId;
            this.sourceNodeId = sourceNodeId;
            this.targetNodeId = targetNodeId;
            this.signalDigest = signalDigest == null ? "" : signalDigest;
            this.intensity = Math.max(1, Math.min(255, intensity));
            this.laidAt = Instant.now();
            this.expiryEpoch = expiryEpoch;
            this.evaporated = false;
        }

        public long getTrailId() { return trailId; }
        public long getSourceNodeId() { return sourceNodeId; }
        public long getTargetNodeId() { return targetNodeId; }
        public String getSignalDigest() { return signalDigest; }
        public int getIntensity() { return intensity; }
        public Instant getLaidAt() { return laidAt; }
        public long getExpiryEpoch() { return expiryEpoch; }
        public boolean isEvaporated() { return evaporated; }

        void evaporate() { this.evaporated = true; }

        public boolean isExpired(long currentEpoch) {
            return evaporated || currentEpoch > expiryEpoch;
        }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("trailId", trailId);
            m.put("from", sourceNodeId);
            m.put("to", targetNodeId);
            m.put("signal", signalDigest);
            m.put("intensity", intensity);
            m.put("expiryEpoch", expiryEpoch);
            m.put("evaporated", evaporated);
            return m;
        }
    }

    public static final class PheromoneTrailIndex {
        private final int capacity;
        private final AtomicLong idSeq = new AtomicLong(0L);
        private final Map<Long, PheromoneTrail> trails = new ConcurrentHashMap<>();
        private final Map<Long, List<Long>> outboundByNode = new ConcurrentHashMap<>();

        public PheromoneTrailIndex(int capacity) {
            this.capacity = Math.max(1, capacity);
        }

        public int size() { return trails.size(); }

        public long layTrail(long sourceNodeId, long targetNodeId, String signalDigest, int intensity, long expiryEpoch) {
            if (trails.size() >= capacity) {
                throw new AlumHive_CapacityExceededException("pheromone trails");
            }
            long id = idSeq.incrementAndGet();
            PheromoneTrail trail = new PheromoneTrail(id, sourceNodeId, targetNodeId, signalDigest, intensity, expiryEpoch);
            trails.put(id, trail);
            outboundByNode.computeIfAbsent(sourceNodeId, k -> new CopyOnWriteArrayList<>()).add(id);
            return id;
        }

        public PheromoneTrail requireTrail(long trailId) {
            PheromoneTrail t = trails.get(trailId);
            if (t == null) {
                throw new AlumHive_NotFoundException("trail:" + trailId);
            }
            return t;
        }

        public List<PheromoneTrail> outboundFrom(long nodeId, long currentEpoch) {
            List<Long> ids = outboundByNode.getOrDefault(nodeId, List.of());
            return ids.stream()
                    .map(trails::get)
                    .filter(Objects::nonNull)
                    .filter(t -> !t.isExpired(currentEpoch))
                    .sorted(Comparator.comparingInt(PheromoneTrail::getIntensity).reversed())
                    .collect(Collectors.toList());
        }

        public void evaporateExpired(long currentEpoch) {
            for (PheromoneTrail t : trails.values()) {
                if (t.isExpired(currentEpoch) && !t.isEvaporated()) {
                    t.evaporate();
                }
            }
        }

        public int activeTrailCount(long currentEpoch) {
            return (int) trails.values().stream().filter(t -> !t.isExpired(currentEpoch)).count();
        }
    }
'''


def emit_consensus() -> str:
    return '''
    // --- Consensus mesh (quorum ballots) ---

    public enum QuorumRoundStatus {
        OPEN, TALLYING, REACHED, FAILED, SEALED
    }

    public static final class QuorumBallot {
        private final long roundId;
        private final String proposalDigest;
        private final String proposerAddress;
        private final int requiredWeight;
        private final Instant openedAt;
        private QuorumRoundStatus status;
        private final Map<String, Integer> votesByAddress;
        private int accumulatedWeight;

        public QuorumBallot(long roundId, String proposalDigest, String proposerAddress, int requiredWeight) {
            this.roundId = roundId;
            this.proposalDigest = proposalDigest;
            this.proposerAddress = proposerAddress;
            this.requiredWeight = Math.max(MIN_QUORUM_WEIGHT, requiredWeight);
            this.openedAt = Instant.now();
            this.status = QuorumRoundStatus.OPEN;
            this.votesByAddress = new ConcurrentHashMap<>();
            this.accumulatedWeight = 0;
        }

        public long getRoundId() { return roundId; }
        public String getProposalDigest() { return proposalDigest; }
        public String getProposerAddress() { return proposerAddress; }
        public int getRequiredWeight() { return requiredWeight; }
        public Instant getOpenedAt() { return openedAt; }
        public QuorumRoundStatus getStatus() { return status; }
        public int getAccumulatedWeight() { return accumulatedWeight; }

        void castVote(String voterAddress, int weight) {
            if (status != QuorumRoundStatus.OPEN) {
                throw new IllegalStateException("AlumHive: round not open");
            }
            votesByAddress.merge(voterAddress, weight, Integer::sum);
            accumulatedWeight += weight;
            if (accumulatedWeight >= requiredWeight) {
                status = QuorumRoundStatus.REACHED;
            }
        }

        void seal() { status = QuorumRoundStatus.SEALED; }
        void fail() { status = QuorumRoundStatus.FAILED; }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("roundId", roundId);
            m.put("proposal", proposalDigest);
            m.put("proposer", proposerAddress);
            m.put("required", requiredWeight);
            m.put("accumulated", accumulatedWeight);
            m.put("status", status.name());
            m.put("votes", votesByAddress.size());
            return m;
        }
    }

    public static final class ConsensusMesh {
        private final int capacity;
        private final int minWeight;
        private final int maxWeight;
        private final AtomicLong idSeq = new AtomicLong(0L);
        private final Map<Long, QuorumBallot> rounds = new ConcurrentHashMap<>();

        public ConsensusMesh(int capacity, int minWeight, int maxWeight) {
            this.capacity = Math.max(1, capacity);
            this.minWeight = minWeight;
            this.maxWeight = maxWeight;
        }

        public long openRound(String proposalDigest, String proposerAddress, int requiredWeight) {
            if (rounds.size() >= capacity) {
                throw new AlumHive_CapacityExceededException("quorum rounds");
            }
            int clamped = Math.max(minWeight, Math.min(maxWeight, requiredWeight));
            long id = idSeq.incrementAndGet();
            rounds.put(id, new QuorumBallot(id, proposalDigest, proposerAddress, clamped));
            return id;
        }

        public QuorumBallot requireRound(long roundId) {
            QuorumBallot b = rounds.get(roundId);
            if (b == null) {
                throw new AlumHive_NotFoundException("round:" + roundId);
            }
            return b;
        }

        public void castVote(long roundId, String voterAddress, int weight, SwarmNodeRegistry registry) {
            QuorumBallot ballot = requireRound(roundId);
            SwarmNode node = registry.listByTier(SwarmNodeTier.WORKER).stream()
                    .filter(n -> n.getOperatorAddress().equalsIgnoreCase(voterAddress))
                    .findFirst()
                    .orElse(null);
            int effectiveWeight = node != null ? node.getComputeWeight() : Math.max(1, weight);
            ballot.castVote(voterAddress, effectiveWeight);
        }

        public void requireReached(long roundId) {
            QuorumBallot b = requireRound(roundId);
            if (b.getStatus() != QuorumRoundStatus.REACHED && b.getStatus() != QuorumRoundStatus.SEALED) {
                throw new AlumHive_QuorumNotReachedException(roundId);
            }
        }

        public void sealRound(long roundId) {
            requireRound(roundId).seal();
        }

        public int openRoundCount() {
            return (int) rounds.values().stream()
                    .filter(r -> r.getStatus() == QuorumRoundStatus.OPEN)
                    .count();
        }

        public List<QuorumBallot> listOpen() {
            return rounds.values().stream()
                    .filter(r -> r.getStatus() == QuorumRoundStatus.OPEN)
                    .collect(Collectors.toList());
        }
    }
'''


def emit_spore_vault() -> str:
    return '''
    // --- Memory spore vault ---

    public static final class SporeFragment {
        private final long sporeId;
        private final String lineageTag;
        private final String payloadDigest;
        private final String depositorAddress;
        private final long boundThoughtId;
        private final Instant storedAt;
        private boolean propagated;

        public SporeFragment(
                long sporeId,
                String lineageTag,
                String payloadDigest,
                String depositorAddress,
                long boundThoughtId
        ) {
            this.sporeId = sporeId;
            this.lineageTag = lineageTag == null ? "root" : lineageTag;
            this.payloadDigest = payloadDigest == null ? "" : payloadDigest;
            this.depositorAddress = depositorAddress;
            this.boundThoughtId = boundThoughtId;
            this.storedAt = Instant.now();
            this.propagated = false;
        }

        public long getSporeId() { return sporeId; }
        public String getLineageTag() { return lineageTag; }
        public String getPayloadDigest() { return payloadDigest; }
        public String getDepositorAddress() { return depositorAddress; }
        public long getBoundThoughtId() { return boundThoughtId; }
        public Instant getStoredAt() { return storedAt; }
        public boolean isPropagated() { return propagated; }

        void markPropagated() { this.propagated = true; }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("sporeId", sporeId);
            m.put("lineage", lineageTag);
            m.put("digest", payloadDigest);
            m.put("depositor", depositorAddress);
            m.put("thoughtId", boundThoughtId);
            m.put("propagated", propagated);
            return m;
        }
    }

    public static final class MemorySporeVault {
        private final int capacity;
        private final AtomicLong idSeq = new AtomicLong(0L);
        private final Map<Long, SporeFragment> spores = new ConcurrentHashMap<>();

        public MemorySporeVault(int capacity) {
            this.capacity = Math.max(1, capacity);
        }

        public int size() { return spores.size(); }

        public long store(String lineageTag, String payloadDigest, String depositorAddress, long boundThoughtId) {
            if (spores.size() >= capacity) {
                throw new AlumHive_CapacityExceededException("spore vault");
            }
            long id = idSeq.incrementAndGet();
            spores.put(id, new SporeFragment(id, lineageTag, payloadDigest, depositorAddress, boundThoughtId));
            return id;
        }

        public SporeFragment requireSpore(long sporeId) {
            SporeFragment s = spores.get(sporeId);
            if (s == null) {
                throw new AlumHive_NotFoundException("spore:" + sporeId);
            }
            return s;
        }

        public void propagate(long sporeId) {
            requireSpore(sporeId).markPropagated();
        }

        public List<SporeFragment> listByLineage(String lineageTag) {
            return spores.values().stream()
                    .filter(s -> s.getLineageTag().equals(lineageTag))
                    .collect(Collectors.toList());
        }

        public long countPropagated() {
            return spores.values().stream().filter(SporeFragment::isPropagated).count();
        }
    }
'''


def emit_scheduler() -> str:
    return '''
    // --- Swarm task scheduler ---

    public enum SwarmTaskStatus {
        QUEUED, DISPATCHED, COMPLETED, ABORTED
    }

    public static final class SwarmTask {
        private final long taskId;
        private final String taskDigest;
        private final long assignedNodeId;
        private final String requesterAddress;
        private final int priority;
        private final Instant enqueuedAt;
        private SwarmTaskStatus status;
        private Instant completedAt;

        public SwarmTask(long taskId, String taskDigest, long assignedNodeId, String requesterAddress, int priority) {
            this.taskId = taskId;
            this.taskDigest = taskDigest;
            this.assignedNodeId = assignedNodeId;
            this.requesterAddress = requesterAddress;
            this.priority = Math.max(0, Math.min(9, priority));
            this.enqueuedAt = Instant.now();
            this.status = SwarmTaskStatus.QUEUED;
            this.completedAt = null;
        }

        public long getTaskId() { return taskId; }
        public String getTaskDigest() { return taskDigest; }
        public long getAssignedNodeId() { return assignedNodeId; }
        public String getRequesterAddress() { return requesterAddress; }
        public int getPriority() { return priority; }
        public Instant getEnqueuedAt() { return enqueuedAt; }
        public SwarmTaskStatus getStatus() { return status; }
        public Instant getCompletedAt() { return completedAt; }

        void dispatch() { status = SwarmTaskStatus.DISPATCHED; }
        void complete() { status = SwarmTaskStatus.COMPLETED; completedAt = Instant.now(); }
        void abort() { status = SwarmTaskStatus.ABORTED; }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("taskId", taskId);
            m.put("digest", taskDigest);
            m.put("nodeId", assignedNodeId);
            m.put("requester", requesterAddress);
            m.put("priority", priority);
            m.put("status", status.name());
            return m;
        }
    }

    public static final class SwarmScheduler {
        private final int capacity;
        private final AtomicLong idSeq = new AtomicLong(0L);
        private final Map<Long, SwarmTask> tasks = new ConcurrentHashMap<>();

        public SwarmScheduler(int capacity) {
            this.capacity = Math.max(1, capacity);
        }

        public int pendingCount() {
            return (int) tasks.values().stream()
                    .filter(t -> t.getStatus() == SwarmTaskStatus.QUEUED || t.getStatus() == SwarmTaskStatus.DISPATCHED)
                    .count();
        }

        public long enqueue(String taskDigest, long nodeId, String requesterAddress, int priority) {
            if (tasks.size() >= capacity) {
                throw new AlumHive_CapacityExceededException("task queue");
            }
            long id = idSeq.incrementAndGet();
            tasks.put(id, new SwarmTask(id, taskDigest, nodeId, requesterAddress, priority));
            return id;
        }

        public SwarmTask requireTask(long taskId) {
            SwarmTask t = tasks.get(taskId);
            if (t == null) {
                throw new AlumHive_NotFoundException("task:" + taskId);
            }
            return t;
        }

        public void dispatch(long taskId) {
            requireTask(taskId).dispatch();
        }

        public void complete(long taskId) {
            requireTask(taskId).complete();
        }

        public List<SwarmTask> listQueued() {
            return tasks.values().stream()
                    .filter(t -> t.getStatus() == SwarmTaskStatus.QUEUED)
                    .sorted(Comparator.comparingInt(SwarmTask::getPriority).reversed())
                    .collect(Collectors.toList());
        }
    }
'''


def emit_support_classes() -> str:
    return '''
    // --- Hive ledger ---

    public static final class HiveEvent {
        private final String kind;
        private final String actorAddress;
        private final long hiveEpoch;
        private final Instant recordedAt;
        private final Map<String, Object> metadata;

        public HiveEvent(String kind, String actorAddress, long hiveEpoch, Instant recordedAt, Map<String, Object> metadata) {
            this.kind = kind;
            this.actorAddress = actorAddress;
            this.hiveEpoch = hiveEpoch;
            this.recordedAt = recordedAt;
            this.metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        }

        public String getKind() { return kind; }
        public String getActorAddress() { return actorAddress; }
        public long getHiveEpoch() { return hiveEpoch; }
        public Instant getRecordedAt() { return recordedAt; }
        public Map<String, Object> getMetadata() { return metadata; }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("kind", kind);
            m.put("actor", actorAddress);
            m.put("epoch", hiveEpoch);
            m.put("at", recordedAt.toString());
            m.put("meta", metadata);
            return m;
        }
    }

    public static final class HiveLedger {
        private final CopyOnWriteArrayList<HiveEvent> events = new CopyOnWriteArrayList<>();

        public void append(HiveEvent event) {
            events.add(event);
        }

        public List<HiveEvent> tail(int limit) {
            int size = events.size();
            int from = Math.max(0, size - limit);
            return new ArrayList<>(events.subList(from, size));
        }

        public int size() { return events.size(); }
    }

    // --- Attestation relay ---

    public static final class AttestationRecord {
        private final String digestHex;
        private final String attesterAddress;
        private final Instant attestedAt;
        private final long expiryEpoch;
        private boolean revoked;

        public AttestationRecord(String digestHex, String attesterAddress, long expiryEpoch) {
            this.digestHex = digestHex;
            this.attesterAddress = attesterAddress;
            this.attestedAt = Instant.now();
            this.expiryEpoch = expiryEpoch;
            this.revoked = false;
        }

        public boolean isValid(long currentEpoch) {
            return !revoked && currentEpoch <= expiryEpoch;
        }

        public void revoke() { revoked = true; }
        public String getDigestHex() { return digestHex; }
        public String getAttesterAddress() { return attesterAddress; }
    }

    public static final class AttestationRelay {
        private final HiveRuntimeConfig config;
        private final Map<String, AttestationRecord> records = new ConcurrentHashMap<>();

        public AttestationRelay(HiveRuntimeConfig config) {
            this.config = config;
        }

        public void attest(String digestHex, String attesterAddress, long currentEpoch) {
            long expiry = currentEpoch + (ATTESTATION_TTL_SECONDS / 12L);
            records.put(digestHex.toLowerCase(Locale.ROOT), new AttestationRecord(digestHex, attesterAddress, expiry));
        }

        public boolean verify(String digestHex, long currentEpoch) {
            AttestationRecord rec = records.get(digestHex.toLowerCase(Locale.ROOT));
            return rec != null && rec.isValid(currentEpoch);
        }

        public int countActive(long currentEpoch) {
            return (int) records.values().stream().filter(r -> r.isValid(currentEpoch)).count();
        }

        public Map<String, Object> buildRelayEnvelope(String digestHex) {
            Map<String, Object> env = new LinkedHashMap<>();
            env.put("digest", digestHex);
            env.put("chainId", config.getChainId());
            env.put("relay", config.getRelayAddress());
            env.put("sink", config.getAttestationSinkAddress());
            env.put("oracle", config.getNectarOracleAddress());
            env.put("domain", config.getSwarmDomainHex());
            return env;
        }
    }

    // --- Metrics ring ---

    public static final class HiveMetricsRing {
        private final int capacity;
        private final CopyOnWriteArrayList<Map<String, Object>> samples = new CopyOnWriteArrayList<>();
        private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();

        public HiveMetricsRing(int capacity) {
            this.capacity = Math.max(64, capacity);
        }

        public void recordGauge(String name, long value) {
            gauges.computeIfAbsent(name, k -> new AtomicLong()).set(value);
            Map<String, Object> sample = new LinkedHashMap<>();
            sample.put("name", name);
            sample.put("value", value);
            sample.put("at", Instant.now().toString());
            samples.add(sample);
            while (samples.size() > capacity) {
                samples.remove(0);
            }
        }

        public void recordCounter(String name, long delta) {
            gauges.computeIfAbsent(name, k -> new AtomicLong()).addAndGet(delta);
        }

        public int sampleCount() { return samples.size(); }

        public Map<String, Long> gaugeSnapshot() {
            Map<String, Long> snap = new LinkedHashMap<>();
            gauges.forEach((k, v) -> snap.put(k, v.get()));
            return snap;
        }
    }

    // --- Validator ---

    public static final class HiveValidator {
        public void requireHiveKeeper(String actor, String expected) {
            requireAddressMatch(actor, expected);
        }

        public void requireSwarmMarshal(String actor, String expected) {
            requireAddressMatch(actor, expected);
        }

        public void requireAddressMatch(String actor, String expected) {
            if (actor == null || expected == null || !actor.equalsIgnoreCase(expected)) {
                throw new AlumHive_UnauthorizedException("role mismatch");
            }
        }

        public void requireValidAddress(String addr) {
            if (addr == null || addr.isBlank() || !addr.startsWith("0x") || addr.length() != 42) {
                throw new AlumHive_InvalidAddressException(String.valueOf(addr));
            }
        }
    }

    // --- Report emitter ---

    public static final class HiveReportEmitter {
        private static final DateTimeFormatter UTC_FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

        public String renderHealth(Map<String, Object> snapshot) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.println("=== alum__AI hive health ===");
            snapshot.forEach((k, v) -> pw.println("  " + k + ": " + v));
            pw.println("  generated: " + UTC_FMT.format(Instant.now()));
            pw.flush();
            return sw.toString();
        }

        public String renderQuorumSummary(List<QuorumBallot> ballots) {
