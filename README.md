# ResonanceDB · Cognitive Wave Database

---

## About 

ResonanceDB is a waveform-native database built for systems that already think in waves. Instead of embedding content as static vectors, ResonanceDB stores pre-encoded waveforms for cognitive resonance-based retrieval.
By storing complex-valued waveforms and matching them through resonance, it gives cognitive systems a field-aware substrate for memory, retrieval, and context.

Each query is matched by resonance, not distance — delivering context-aware results through constructive interference.
The system supports practically instantaneous recall, across millions of patterns, via phase-sharded storage, memory-mapped segments, and SIMD acceleration.

### What makes it different

| Capability                            | Value for AI teams & researchers                                                                                                     |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| **Pre-symbolic representation**       | Stores latent meaning agnostic to language or modality; suitable for LLM memory, multi-sensor fusion, agent state snapshots.         |
| **Field-coherent retrieval**          | Matches are selected based on **constructive interference**, not distance — supporting cognitive dynamics and phase-aware filtering. |
| **Phase-sharded architecture**        | Partitions patterns by average phase φ̄, enabling linear horizontal scaling and highly parallel search.                              |
| **Dual kernels (Java / Native SIMD)** | Choose pure-Java portability or Panama/C backend for AVX-optimized cosine–phase resonance.                                           |
| **Memory-mapped segments**            | Zero-copy reads; predictable low-latency access on NVMe & PMEM.                                                                      |
| **Atomic write path**                 | Checksum → commit-flag sequence guarantees crash-consistent persistence without blocking readers.                                    |
| **Modular Gradle workspace**          | `core`, `native`, `cli`, and future plug-ins kept isolated; no circular deps.                                                        |

### Typical use cases

* **Wave-based semantic memory** for AGI agents with pre-symbolic, affective, or identity-grounded cognition
* **Symbolic memory** for reasoning agents and cognitive architectures
* **Multimodal similarity search** across text, images, audio, or sensor traces
* **Edge inference caches** where bandwidth or power precludes full embeddings
* **Research platforms** exploring non-Euclidean semantic spaces or quantum-inspired metrics

>*See also: [Applications of Wave-Based Memory](./docs/whitepapers/Applications-of-ResonanceDB-in-AGI-Memory-and-Affective-Modeling.md)*

### Technology snapshot

| Layer    | Key details                                                                                                                                             |
|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| Language | Java 22 (⁠Panama FFI) + optional C/SIMD backend                                                                                                         |
| Storage  | `.segment` = `[BinaryHeader]` + multiple entries: each entry = `[ID (16 B)] [Length (4 B)] [Meta Offset (4 B)] [Amplitude[] (double)] [Phase[] (double)]` |
| Routing  | `PhaseShardSelector` with explicit range map + hash fallback                                                                                            |
| Build    | Gradle 8 multi-project; native lib compiled via `:resonance-native:buildNativeLib`                                                                      |
| License  | **Prosperity Public License 3.0** (non-commercial) • 30-day commercial evaluation                                                                       |
| Patent   | US priority date 18 Jun 2025 – patent application in preparation                                                                                        |

---

## 🚀 Build & Run

### ✅ Requirements

- JDK **22+** (Panama FFI required)
- GCC (for native `libresonance.so` compilation)
- Gradle 8+

---

## 🧩 Kernel Modes: Java vs Native

ResonanceDB supports two execution backends for comparing wave patterns:

| Backend       | Description                                 | SIMD Optimized | Platform Dependent |
|---------------|---------------------------------------------|----------------|---------------------|
| `JavaKernel`  | Pure Java fallback (portable)               | ❌              | ❌                  |
| `NativeKernel`| Panama FFI + C (uses `libresonance.so`)     | ✅              | ✅ (Linux/macOS)    |

---

## 🧱 How to Build

### 🛠 1. Build the native library

This step is **only needed if using `NativeKernel`**:

```bash
./gradlew :resonance-native:buildNativeLib
````

It generates `libresonance.so` under `resonance-native/libs/`.

> ✅ Make sure `gcc` is available and your OS supports shared libraries.

---

### 🔨 2. Build all modules

```bash
./gradlew build
```

---

## ▶️ Run CLI

```bash
./gradlew :resonance-cli:run
```

The default CLI will initialize with `NativeKernel` if available.

---

## 🔧 Selecting the Resonance Kernel

By design, `resonance-core` is modular and **does not depend on `resonance-native`**.
This ensures clean architecture and prevents cyclic dependencies.

You must **explicitly select the backend** in your application (CLI or service entry point):

```java
import ai.evacortex.resonancedb.core.engine.ResonanceEngine;
import ai.evacortex.resonancedb.nativeffi.NativeKernel; // or JavaKernel

public class Main {
    public static void main(String[] args) {
        ResonanceEngine.setBackend(new NativeKernel()); // or new JavaKernel()
        ...
    }
}
```

> ℹ️ If no backend is set, a fallback may be used depending on configuration.

---

## 📘 Example: Manually Creating a WavePattern

To create a wave-based semantic pattern manually:

```java
double[] amplitude = {0.9, 0.6, 0.3, 0.0, 0.1}; // semantic intensity
double[] phase = {0.0, Math.PI/4, Math.PI/2, Math.PI, 3*Math.PI/2}; // context/modality

WavePattern psi = new WavePattern(amplitude, phase);
````

Use this pattern to insert into the database or as a query:

```java
resonanceStore.insert(psi, Map.of("label", "custom input"));
List<ResonanceMatch> results = resonanceStore.query(psi, 10);
```
---

## 🛠 Runtime Notes

* When using **`NativeKernel`**, ensure that:

    * `libresonance.so` is compiled and on your `java.library.path`
    * You are on a supported OS (Linux/macOS); Windows requires `.dll`
* When using **`JavaKernel`**, no native code is needed

---

## 📦 Binary Segment Format

Each `.segment` file in ResonanceDB consists of two parts:

1. **Segment Header** (one per file)
2. **WavePattern Entries** (multiple per file)

### 🧱 1. Segment Header

The segment begins with a binary header containing file-level metadata and atomicity markers:

```
[Magic (4 B)] [Version (2 B)] [Timestamp (8 B)] [Record Count (4 B)]
[Last Offset (8 B)] [Checksum (4–32 B)] [Commit Flag (1 B)] [Padding (0–3 B)]
```

* `Magic`: fixed ASCII signature `RDSN` (`0x5244534E`)
* `Version`: segment version (currently 1)
* `Timestamp`: UNIX time (ms) of creation
* `Record Count`: number of stored patterns
* `Last Offset`: file offset of last valid pattern
* `Checksum`: hash (e.g., CRC32) over all `WavePattern` entries; header and commit flag are excluded
* `Commit Flag`: written *only after* successful checksum validation
* Entire header is padded to a multiple of 4 bytes for alignment

> ✅ This header enables atomic validation after crash and supports safe remapping.

---

### 🧩 2. WavePattern Entries

Each stored pattern is serialized as:

```
[ID (16 B)] [Length (4 B)] [Reserved (4 B)] [Amplitude[] (8·L B)] [Phase[] (8·L B)]
```

* `ID`: 16-byte MD5 hash derived from `amplitude[] + phase[]`
* `Length`: number of elements in `amplitude[]` and `phase[]` (must be equal)
* `Meta Offset`: reserved (currently unused, placeholder for future metadata position)
* `Amplitude[]`: array of `double` values (IEEE 754, little-endian)
* `Phase[]`: array of `double` values (same length as amplitude)

Each entry is **8-byte aligned**, and tombstones (logically deleted patterns) are retained structurally but skipped during reads.

---

### 📚 Index and Metadata

* **Manifest** (`manifest.idx`): maps each ID to `[segment name, byte offset, mean phase]`
* **Metadata** (`pattern-meta.json`): stores auxiliary labels, tags, or annotations

---

### ⚡ Access Model

* All segments are loaded with `MappedByteBuffer` for **zero-copy** access
* Reads are parallelized across segments via `PhaseShardSelector`
* Writes are appended with validation and atomic commit semantics

---

## 🧭 Shard-Aware Routing

Each `WavePattern` is assigned to a phase shard based on its average phase.  
Routing is handled via the `PhaseShardSelector`:

- **Explicit range map**: `[phaseStart .. phaseEnd] → segment`
- **Fallback hash** routing is reserved but not active by default (`TODO` in current implementation).

This enables scalable partitioning of stored waveforms and parallelized queries.

---

## 🗺 Roadmap

* [x] Gradle multi-project structure (`core`, `native`, `cli`)
* [x] Binary storage format (`.segment`, `manifest.idx`)
* [x] `WavePattern` serialization and ID hashing
* [x] Panama FFI integration (`compare()` native backend)
* [x] Configurable kernel backend (`JavaKernel`, `NativeKernel`)
* [x] `query(WavePattern, topK)` with scoring and self-match boost
* [x] Shard-aware routing and phase-based segment indexing
* [x] Thread-safe in-memory caches
* [ ] CLI interface for insert/update/query
* [ ] Optional: Index caching (pluggable; not yet integrated)

---

## 📄 License & Commercial Use

* The **core software** is licensed under the [Prosperity Public License 3.0](./LICENSE),
  which permits **non-commercial use only**.

> 🔒 **Commercial use beyond a 30-day evaluation period requires a paid license.**
> To obtain a commercial license, contact: [license@evacortex.ai](mailto:license@evacortex.ai)

* All **whitepapers and documentation** located in the `docs/` directory are licensed under the
  [Creative Commons Attribution-NoDerivatives 4.0 International (CC BY-ND 4.0)](./LICENSE-docs) license.
> 📄 The **whitepapers** may describe techniques or algorithms that are part of a pending patent application.  
> Use of those methods in commercial products requires proper licensing, even if the implementation differs.
---


## 🧠 Patent Status

A **provisional U.S. patent application** related to ResonanceDB was filed on **June 18, 2025** (USPTO), establishing legal priority.

> Covered by one or more pending patents. Contact: [license@evacortex.ai](mailto:license@evacortex.ai)

No patent license is granted at this time.  
Terms will be updated upon filing of the non-provisional application.

---


## 📫 Contact

* Author: [Aleksandr Listopad](mailto:license@evacortex.ai)
* Security & Licensing: `license@evacortex.ai`
* SPDX Identifier: `SPDX-License-Identifier: Prosperity-3.0`

