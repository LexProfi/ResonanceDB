# ResonanceDB  
**A waveform-native database for context-resonant retrieval**

---

## About

ResonanceDB is a next-generation semantic database designed to store and retrieve meaning-rich patterns using **complex-valued waveforms**.  
Instead of treating data as static vectors in geometric space, it represents information as structured waveforms — enabling retrieval by **resonance, not distance**.

Queries are resolved via **constructive interference** between patterns, yielding context-sensitive matches across modalities.  
With **phase-sharded storage**, **memory-mapped segments**, and **optional SIMD acceleration**, the system supports ultra-low-latency recall even across millions of entries.

---
## 🧠 Resonance-Based Retrieval as a Classical Analogue of Grover’s Amplification

ResonanceDB implements a **classical, software-based analogue of Grover-style amplitude amplification** adapted to semantic and cognitive search spaces.

Unlike traditional vector databases that rely on linear scans or heuristic nearest-neighbor search, ResonanceDB employs a **phase-aware resonance kernel** that selectively enhances the effective contribution of semantically aligned patterns through **constructive interference**.

In this formulation, the resonance comparison mechanism plays a role **analogous to an oracle-guided amplification process**, increasing the relative prominence of coherent matches without requiring quantum hardware or probabilistic measurement.

This approach realizes amplitude-amplification-like behavior within a **memory-mapped, phase-sharded, classical execution environment**, enabling scalable, deterministic retrieval over large semantic datasets.

> **Note:**
> The specific architecture, algorithms, and execution model used to implement this resonance-based amplification mechanism are developed by EvaCortex Lab and may be subject to one or more pending patent
applications. Any patent license, if and when applicable, is limited to the scope expressly provided under the Prosperity Public License 3.0 and applies only while you remain in full compliance with its terms.
---

## What Makes It Different

| Feature | Why It Matters |
|------|------|
| Meaning-first storage | Patterns are stored as waveforms preserving context, intensity, and structure — closer to cognitive representations than static vectors. |
| Resonant retrieval | Results emerge through interference and coherence, not geometric proximity. |
| Phase-sharded scaling | Patterns are routed by average phase, enabling horizontal scaling and parallel search. |
| Performance-tunable kernels | Portable Java or native SIMD backends, sharing the same mathematical core. |
| Zero-copy memory | Patterns are accessed directly from disk via memory mapping — no unpacking or deserialization. |
| Crash-safe writes | Patterns are atomically committed using checksums and commit flags. |
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
| Storage | `.segment` files with memory-mapped access; each pattern is stored as amplitude + phase |
| Routing | Phase-based sharding by mean phase φ̄ |
| Build | Modular Gradle 8 workspace |
| License | Prosperity Public License 3.0 (non-commercial; 30-day commercial evaluation) |

---

## ⚡ Why Java 22?

We chose Java 22 not for legacy reasons, but for the **Foreign Function & Memory (Panama) API**.

This allows ResonanceDB to:

* Remove garbage-collector involvement from performance-critical paths by operating on off-heap memory.
* Approach C-level performance while preserving memory safety and strong encapsulation guarantees.
* Use tightly packed, interleaved primitive memory layouts optimized for SIMD and CPU cache locality — a design specifically chosen for wave-based semantic processing.

---

## 🚀 Build & Run

### ✅ Requirements

- JDK 22+ (Panama FFI required)
- GCC / Clang (for native kernel)
- Gradle 8+

---

## 🧩 Kernel Modes: Java vs Native

ResonanceDB supports two execution backends for comparing wave patterns:

| Backend | Description | SIMD Optimized | Platform Dependent |
|------|-------------|---------------|--------------------|
| JavaKernel | Pure Java fallback | ❌ | ❌ |
| NativeKernel | Panama FFI + C (libresonance) | ✅ | ✅ (Linux/macOS) |

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

The CLI will initialize with `NativeKernel` if available, otherwise fall back to `JavaKernel`.

---

## 🔧 Selecting the Resonance Kernel

`resonance-core` is fully decoupled from `resonance-native`.

You must explicitly select the backend:

```java
import ai.evacortex.resonancedb.core.engine.ResonanceEngine;
import ai.evacortex.resonancedb.core.engine.NativeKernel;

public class Main {
    public static void main(String[] args) {
        ResonanceEngine.setBackend(new NativeKernel());
    }
}
```

If no backend is set, a fallback may be used depending on configuration.

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
[Last Offset (8 B)] [Checksum (4–32 B)] [Commit Flag (1 B)] [Padding]
```

* Magic: `RDSN`
* Commit flag is written only after successful checksum validation.

This design enables atomic validation after crash scenarios and safe remapping.

---

### 🧩 WavePattern Entry

```
[ID (16 B)] [Length (4 B)] [Reserved (4 B)]
[Amplitude[]] [Phase[]]
```

* ID: MD5 hash over amplitude + phase
* Arrays stored as IEEE-754 doubles
* Entries are 8-byte aligned
* Tombstones may be retained but skipped during reads

---

## 📚 Index and Metadata

* `manifest.idx`: ID → segment, offset, mean phase
* `pattern-meta.json`: auxiliary labels and annotations

---

## 🧭 Shard-Aware Routing

Each pattern is assigned to a shard based on average phase.
Routing is handled by `PhaseShardSelector`, enabling parallel search and scalable growth.

---

## 📄 License & Commercial Use

Licensed under the [Prosperity Public License 3.0](./LICENSE).

* Free for non-commercial use
* Commercial use beyond 30 days requires a paid license

Contact: **[license@evacortex.ai](mailto:license@evacortex.ai)**

* All **whitepapers and documentation** located in the `docs/` directory are licensed under the
  [Creative Commons Attribution-NoDerivatives 4.0 International (CC BY-ND 4.0)](./LICENSE-docs) license.
> 📄 The **whitepapers** may describe techniques or algorithms that are part of a pending patent application.  
> Use of those methods in commercial products may require separate patent licensing, even if the implementation differs.

---

## 🧠 Patent Status

A provisional U.S. patent application related to ResonanceDB was filed on **June 18, 2025**.

Certain techniques described here **may be covered by one or more pending patent applications**.

At present, no patents have been granted in connection with ResonanceDB.
Any patent license, if and when applicable, is limited to the scope
expressly provided under the Prosperity Public License 3.0 and applies
only while you remain in full compliance with its terms.

---

## 🤖 Machine Learning & Training Use

Use of this repository or its materials for training, fine-tuning, or evaluating machine learning models **may constitute commercial use**.

See [TRAINING-NOTICE.md](./TRAINING-NOTICE.md).
In case of conflict, the [LICENSE](./LICENSE) file controls.

---

## 🛡️ Algorithmic Integrity

ResonanceDB exhibits a distinctive algorithmic structure arising from its phase-aware resonance model and SIMD-oriented execution.

These characteristics may, in principle, produce identifiable computational or statistical signatures when replicated at scale.

---

## 📫 Contact

**Author:** Aleksandr Listopad
**Security & Licensing:** [license@evacortex.ai](mailto:license@evacortex.ai)
**SPDX:** Prosperity-3.0
