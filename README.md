# ResonanceDB  
**A waveform-native database for context-resonant retrieval**

---

## About

ResonanceDB is a next-generation semantic database designed to store and retrieve meaning-rich patterns using **complex-valued waveforms**.  
Instead of treating data as static vectors in geometric space, it represents information as structured waveforms — enabling retrieval by **resonance, not distance**.

Queries are resolved via **phase-coherent scoring and constructive interference** between patterns, yielding context-sensitive matches across modalities.  
With **phase-sharded storage**, **memory-mapped segments**, and **optional SIMD acceleration**, the system supports ultra-low-latency, deterministic recall even across millions of entries.

---

## 🧠 Resonance-Based Retrieval  
### A Classical Analogue of Amplitude Amplification

ResonanceDB exhibits **amplitude-amplification-like behavior in a classical, deterministic execution environment**, adapted to semantic and cognitive search spaces.

Unlike traditional vector databases that rely on linear scans, approximate nearest-neighbor heuristics, or purely geometric proximity, ResonanceDB employs a **phase-aware resonance kernel**.  
This kernel selectively enhances the effective contribution of semantically aligned patterns through **single-pass, phase-coherent accumulation and interference-style aggregation**.

The resulting behavior is **analogous** to amplitude amplification:  
coherent matches gain relative prominence, while incoherent or weakly aligned patterns are suppressed — without requiring quantum hardware, probabilistic measurement, or stochastic sampling.

This mechanism operates within a **memory-mapped, phase-sharded, classical execution model**, enabling scalable and fully deterministic retrieval over large semantic datasets.

> **Note:**  
> The specific architecture, algorithms, and execution model used to implement this resonance-based amplification mechanism are developed by EvaCortex Lab and may be subject to one or more pending patent applications.  
> No patent rights are granted except as expressly provided under the Prosperity Public License 3.0. Any such rights apply only while you remain in full compliance with its terms.

---

## 🔬 Amplitude-Balanced Resonance Normalization

ResonanceDB’s scoring mechanism combines **phase-coherent inner-product accumulation** with an **explicit amplitude-balancing normalization**.

In addition to directional (phase) alignment, the resonance score incorporates a normalization factor based on the **ratio between geometric and arithmetic energy means** of the compared patterns.  
This design penalizes large energy (scale) imbalance even when directional alignment is high.

As a result:

- Patterns that are directionally aligned but vastly different in overall intensity do **not** receive artificially maximal scores.
- Semantic stability improves in mixed-intensity or heterogeneous datasets.
- The resulting score reflects both **coherence** and **balanced contribution**, rather than angle alone.

While individual components of this mechanism resemble normalized inner products known from signal processing, the **combined amplitude-balanced, phase-aware scoring behavior** differs from standard cosine similarity and produces materially different ranking dynamics.

---

## What Makes It Different

| Feature | Why It Matters |
|------|------|
| Waveform representation | Patterns are stored as amplitude + phase, preserving intensity, structure, and context — closer to cognitive representations than static vectors. |
| Phase-coherent scoring | Retrieval is driven by interference-style aggregation, not purely geometric proximity. |
| Amplitude-balanced normalization | Explicitly penalizes scale dominance, stabilizing semantic relevance under energy imbalance. |
| Phase-sharded scaling | Patterns are routed by mean phase, enabling horizontal scaling and parallel search. |
| Deterministic kernel interface | Java and native SIMD backends share the same mathematical contract and scoring behavior. |
| Zero-copy memory | Patterns are accessed directly from disk via memory mapping — no unpacking or deserialization. |
| Crash-safe writes | Atomic commits using checksums and commit flags ensure safe recovery after failures. |
| Modular by design | Clean Gradle multi-project structure, easy to extend or integrate. |

---

## Typical Use Cases

- **Memory for cognitive agents** — store semantic or affective traces that evolve over time.
- **Hybrid reasoning systems** — combine symbolic DAGs with resonant memory.
- **Multimodal AI** — unify text, image, and sensor data using a shared waveform substrate.
- **Edge-native memory cache** — deploy on-device with zero-deserialization and memory-safe reads.
- **Exploratory AI research** — prototype alternatives to vector search and embedding similarity.

See also: *Applications of Wave-Based Memory*

---

## Technology Snapshot

| Layer | Snapshot |
|------|---------|
| Language | Java 22 + optional native C/SIMD (via Panama FFI) |
| Storage | `.segment` files with memory-mapped access; amplitude + phase per pattern |
| Routing | Phase-based sharding by mean phase φ̄ |
| Build | Modular Gradle 8 workspace |
| License | Prosperity Public License 3.0 (non-commercial; 30-day commercial evaluation) |

---

## ⚡ Why Java 22?

ResonanceDB uses Java 22 for its **Foreign Function & Memory (Panama) API**, not for legacy reasons.

This enables:

- Off-heap operation without garbage-collector involvement on critical paths.
- Near-C performance while preserving memory safety and strong encapsulation.
- Tightly packed, interleaved primitive layouts optimized for SIMD and CPU cache locality — specifically chosen for waveform-based semantic processing.

---

## 🚀 Build & Run

### ✅ Requirements

- JDK 22 +
- GCC / Clang (for native kernel)
- Gradle 8 +

---

## 🧩 Kernel Modes: Java vs Native

| Backend | Description | SIMD Optimized | Platform Dependent |
|------|-------------|---------------|--------------------|
| JavaKernel | Pure Java fallback | ❌ | ❌ |
| NativeKernel | Panama FFI + C (`libresonance`) | ✅ | ✅ (Linux/macOS) |

---

## 🧱 How to Build

### 🛠 1. Build the native library (optional)

```bash
./gradlew :resonance-native:buildNativeLib
````

This generates `libresonance.so` (or platform equivalent) under `resonance-native/libs/`.

### 🔨 2. Build all modules

```bash
./gradlew build
```

---

## ▶️ Run CLI

```bash
./gradlew :resonance-cli:run
```

The CLI initializes with `NativeKernel` if available, otherwise falls back to `JavaKernel`.

---

## 🔧 Selecting the Resonance Kernel

```java
import ai.evacortex.resonancedb.core.engine.ResonanceEngine;
import ai.evacortex.resonancedb.core.engine.NativeKernel;

public class Main {
    public static void main(String[] args) {
        ResonanceEngine.setBackend(new NativeKernel());
    }
}
```

---

## 📘 Example: Creating a WavePattern

```java
double[] amplitude = {0.9, 0.6, 0.3, 0.0, 0.1};
double[] phase = {0.0, Math.PI/4, Math.PI/2, Math.PI, 3*Math.PI/2};

WavePattern psi = new WavePattern(amplitude, phase);
resonanceStore.insert(psi, Map.of("label", "custom input"));

List<ResonanceMatch> results = resonanceStore.query(psi, 10);
```

---

## 📦 Binary Segment Format
### 🧱 Segment Header

```
[Magic (4 B)] [Version (2 B)] [Timestamp (8 B)] [Record Count (4 B)]
[Last Offset (8 B)] [Checksum (implementation-dependent)] [Commit Flag (1 B)] [Padding]
```

* **Magic:** `RDSN`
* **Checksum:** implementation-dependent (e.g., CRC32, truncated hash)
* **Commit flag** is written **only after successful checksum validation**

This layout enables **atomic segment validation** after crash scenarios and safe remapping of memory-mapped files without partial-write ambiguity.

---

### 🧩 WavePattern Entry

```
[ID (16 B)] [Length (4 B)] [Reserved (4 B)]
[Amplitude[]] [Phase[]]
```

* **ID:** MD5 hash over amplitude + phase (content-addressed)
* **Amplitude / Phase:** IEEE-754 `double`
* **Alignment:** 8-byte aligned
* **Tombstones:** may be retained for crash safety and compaction, but are skipped during reads

---

## 📚 Index & Metadata

* **`manifest.idx`** — maps `ID → (segment, offset, mean phase)`
* **`pattern-meta.json`** — auxiliary labels, annotations, and external metadata

---

## 🧭 Shard-Aware Routing

Patterns are assigned to shards based on **mean phase (φ̄)**.
Routing is handled by `PhaseShardSelector`, enabling **parallel search**, **phase-locality**, and **scalable horizontal growth** without global reindexing.

---

## 📄 License & Commercial Use

Licensed under the **Prosperity Public License 3.0**.

* Free for non-commercial use
* Commercial use beyond 30 days requires a paid license

Contact: **[license@evacortex.ai](mailto:license@evacortex.ai)**

Whitepapers and documentation in `docs/` are licensed under
**CC BY-ND 4.0**.

---

## 🧠 Patent Status

A provisional U.S. patent application related to ResonanceDB was filed on **June 18, 2025**.

Certain techniques described here may be covered by one or more pending patent applications.
No patent rights are granted except as expressly provided under the Prosperity Public License 3.0.

---

## 🤖 Machine Learning & Training Use

Use of this repository or its materials for training, fine-tuning, or evaluation of machine-learning systems **may constitute commercial use**.

See [TRAINING-NOTICE.md](./TRAINING-NOTICE.md).
In case of conflict, the [LICENSE](./LICENSE) file controls.

---

## 🛡️ Algorithmic Integrity

ResonanceDB’s observable behavior arises from the **specific composition** of:

* phase-coherent scoring,
* amplitude-balanced normalization,
* phase-sharded routing,
* memory-mapped execution, and
* deterministic SIMD-oriented kernels.

These characteristics result from system-level design choices, **not from any single mathematical primitive**, and may produce identifiable computational signatures when replicated at scale.

---

## 📫 Contact

**Author:** Aleksandr Listopad
**Security & Licensing:** [license@evacortex.ai](mailto:license@evacortex.ai)
**SPDX:** Prosperity-3.0

