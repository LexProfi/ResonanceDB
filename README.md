# ResonanceDB
### A waveform-native database for context-resonant retrieval.

---

## About 

ResonanceDB is a next-generation semantic database designed to store and retrieve meaning-rich patterns using complex-valued waveforms.
Instead of treating data as static vectors in geometric space, it represents information as structured waveforms - enabling retrieval by resonance, not distance.

Queries are resolved via constructive interference between patterns, yielding context-sensitive matches across modalities.
With phase-sharded storage, memory-mapped segments, and optional SIMD acceleration, the system supports ultra-low latency recall even across millions of entries.

### What makes it different

| Feature                         | Why it matters                                                                                                                              |
| ------------------------------- |---------------------------------------------------------------------------------------------------------------------------------------------|
| **Meaning-first storage**       | Instead of fixed vectors, patterns are stored as waveforms that preserve context, intensity, and structure - closer to how cognition works. |
| **Resonant retrieval**          | Finds results not by distance but by **interference** - surfacing patterns that align in meaning, not just position.                        |
| **Phase-sharded scaling**       | Automatically routes patterns by average phase, enabling effortless horizontal growth and blazing-fast parallel search.                     |
| **Performance-tunable kernels** | Pick Java for portability or native SIMD for speed - both backed by the same core logic.                                                    |
| **Zero-copy memory**            | Access patterns directly from disk without unpacking - instant recall even on edge devices.                                                 |
| **Crash-safe writes**           | Every pattern is atomically committed using checksum + commit flag - no corruption, no downtime.                                            |
| **Modular by design**           | Easy to extend, integrate, or fork - the Gradle workspace is clean, isolated, and ready for evolution.                                      |


### Typical use cases

* **Memory for cognitive agents** - store affective or semantic traces that evolve over time, without reducing them to tokens.
* **Hybrid reasoning systems** - combine symbolic DAGs with resonant memory for recall that adapts to phase and meaning.
* **Multimodal AI** - unify image, text, and sensor data using a shared waveform substrate.
* **Edge-native memory cache** - deploy on-device with zero-deserialization and memory-safe reads.
* **Exploratory AI research** - prototype alternatives to vector search, embedding similarity, and symbolic memory.

>*See also: [Applications of Wave-Based Memory](./docs/whitepapers/Applications-of-ResonanceDB-in-AGI-Memory-and-Affective-Modeling.md)*

### Technology snapshot

| Layer    | Snapshot                                                                                                                     |
| -------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Language | Java 22 + optional native C/SIMD (via Panama FFI)                                                                            |
| Storage  | `.segment` files with memory-mapped access - each pattern is stored as complex-valued waveform (amplitude + phase)           |
| Routing  | Patterns are routed by average phase φ̄; optional fallback strategies can be plugged in                                      |
| Build    | Modular Gradle 8 workspace with clean project boundaries                                                                     |
| License  | **Prosperity Public License 3.0** - free for non-commercial use; 30-day commercial evaluation                                |

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

### 🤖 Machine Learning & Training Use

Use of this repository or its materials for training machine learning or
artificial intelligence models may constitute commercial use.
See [TRAINING-NOTICE.md](TRAINING-NOTICE.md).

---


## 📫 Contact

* Author: [Aleksandr Listopad](mailto:license@evacortex.ai)
* Security & Licensing: `license@evacortex.ai`
* SPDX Identifier: `SPDX-License-Identifier: Prosperity-3.0`

