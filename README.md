# ResonanceDB · Cognitive Wave Database

**ResonanceDB** is a next-generation experimental database engine for storing and querying cognitive wave patterns using phase-aware resonance matching.  
It is designed for symbolic memory, reasoning systems, and AI semantic architectures.

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

## 🛠 Runtime Notes

* When using **`NativeKernel`**, ensure that:

    * `libresonance.so` is compiled and on your `java.library.path`
    * You are on a supported OS (Linux/macOS); Windows requires `.dll`
* When using **`JavaKernel`**, no native code is needed

## 📦 Binary Segment Format

Each `.segment` file stores `WavePattern` entries in binary format:

```
[ID (16 bytes)] [Length (4 bytes)] [Meta Offset (4 bytes)] [Amplitude...] [Phase...]
```

* Memory-mapped I/O (`MappedByteBuffer`) is used for fast access.
* Pattern metadata is stored separately in `pattern-meta.json`.
* Index (`manifest.idx`) maps pattern IDs to their offsets and segment IDs.

---

## 🧭 Shard-Aware Routing

Each `WavePattern` is assigned to a phase shard based on its average phase.  
Routing is handled via the `PhaseShardSelector`:

- **Explicit range map**: `[phaseStart .. phaseEnd] → segment`
- **Fallback hash**: `hash(meanPhase) mod N`

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
* [ ] CLI interface for insert/update/query
* [x] Thread-safe in-memory caches
* [ ] Optional RocksDB index support
* [ ] Patent filing and export control compliance

---

## 📄 License & Commercial Use

* The **core software** is licensed under the [Prosperity Public License 3.0](./LICENSE),
  which permits **non-commercial use only**.

> 🔒 **Commercial use beyond a 30-day evaluation period requires a paid license.**
> To obtain a commercial license, contact: `license@evacortex.ai`

* All **whitepapers and documentation** located in the `docs/` directory are licensed under the
  [Creative Commons Attribution-NoDerivatives 4.0 International (CC BY-ND 4.0)](./LICENSE-docs) license.
> 📄 The **whitepapers** may describe techniques or algorithms that are part of a pending patent application.  
> Use of those methods in commercial products requires proper licensing, even if the implementation differs.
---


## 🧠 Patent Status

A patent application is being prepared.
No patent license is granted at this time.
License terms will be updated upon filing.

---

## 📫 Contact

* Author: [Aleksandr Listopad](mailto:license@evacortex.ai)
* Security & Licensing: `license@evacortex.ai`
* SPDX Identifier: `SPDX-License-Identifier: Prosperity-3.0`

