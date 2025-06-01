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

---

## 🗺 Roadmap

* [x] Gradle multi-project structure (`core`, `native`, `cli`)
* [x] Binary storage format (`.segment`, `manifest.idx`)
* [x] `WavePattern` serialization and ID hashing
* [x] Panama FFI integration (`compare()`)
* [x] Configurable kernel backend (`JavaKernel`, `NativeKernel`)
* [ ] `query(WavePattern, topK)` with scoring
* [ ] Shard-aware routing and segment indexing
* [ ] CLI interface for insert/update/query
* [ ] Thread-safe in-memory caches
* [ ] Optional RocksDB index support
* [ ] Patent filing and export control compliance

---

## 📄 License & Commercial Use

* The **core software** is licensed under the [Prosperity Public License 3.0](./LICENSE),
  which permits **non-commercial use only**.

> 🔒 **Commercial use beyond a 30-day evaluation period requires a paid license.**
> To obtain a commercial license, contact: `license@evacortex.com`

* All **whitepapers and documentation** located in the `docs/` directory are licensed under the
  [Creative Commons Attribution-NoDerivatives 4.0 International (CC BY-ND 4.0)](./LICENSE-docs) license.
---


## 🧠 Patent Status

A patent application is being prepared.
No patent license is granted at this time.
License terms will be updated upon filing.

---

## 📫 Contact

* Author: [Alexander Listopad](mailto:license@evacortex.com)
* Security & Licensing: `license@evacortex.com`
* SPDX Identifier: `SPDX-License-Identifier: Prosperity-3.0`

