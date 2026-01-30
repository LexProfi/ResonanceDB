# ResonanceDB

**A waveform-native database for context-resonant retrieval**

---

## About

ResonanceDB is a next-generation semantic database designed to store and retrieve meaning-rich patterns using **complex-valued waveforms**. Instead of treating data as static vectors in geometric space, it represents information as structured waveforms — enabling retrieval by **resonance, not distance**.

Queries are resolved via **phase-coherent scoring and constructive interference** between patterns, yielding context-sensitive matches across modalities. With **phase-sharded storage**, **memory-mapped segments**, and **optional SIMD acceleration**, the system supports ultra-low-latency, deterministic recall even across millions of entries.

This repository and its contents are provided and made available solely under the terms of the applicable license files in this repository, including **[LICENSE](./LICENSE)** and (for certain technical contexts) **[TRAINING_NOTICE.md](./TRAINING_NOTICE.md)**. Permitted uses include research, evaluation, and non-commercial use, as well as any other use expressly authorized by the applicable license(s). No rights are granted except as explicitly set forth therein.

> **Informational notice**
> This README is informational only and does not grant any rights. All rights and permissions are governed solely by the applicable license files.

---

## 🧠 Resonance-Based Retrieval

### A Classical Analogue of Amplitude Amplification

ResonanceDB exhibits **amplitude-amplification-like behavior in a classical, deterministic execution environment**, adapted to semantic and cognitive search spaces.

Unlike traditional vector databases that rely on linear scans, approximate nearest-neighbor heuristics, or purely geometric proximity, ResonanceDB employs a **phase-aware resonance kernel**. This kernel selectively enhances the effective contribution of semantically aligned patterns through **single-pass, phase-coherent accumulation and interference-style aggregation**.

The resulting behavior is **functionally analogous** to amplitude amplification: coherent matches gain relative prominence, while incoherent or weakly aligned patterns are suppressed — without requiring quantum hardware, probabilistic measurement, or stochastic sampling.

This analogy is descriptive rather than algorithmic and does **not** imply equivalence to any known quantum algorithm, quantum computational model, or quantum speedup.

This mechanism operates within a **memory-mapped, phase-sharded, classical execution model**, enabling scalable and fully deterministic retrieval over large semantic datasets.

> **Patent and rights notice**
> The specific architecture, algorithms, data layouts, and execution model used to implement resonance-based retrieval are developed by **EvaCortex Lab** and may be subject to one or more pending patent applications.
> No patent rights are granted except as expressly provided under the repository **[LICENSE](./LICENSE)**, and only while you remain in full compliance with its terms. No implied license arises from publication, description, or use of this documentation.

---

## 🔬 Amplitude-Balanced Resonance Normalization

ResonanceDB’s scoring mechanism combines **phase-coherent inner-product accumulation** with an **explicit amplitude-balancing normalization step**.

In addition to directional (phase) alignment, the resonance score incorporates a normalization factor derived from the **relationship between geometric and arithmetic energy means** of the compared patterns. This design penalizes large energy (scale) imbalance even when directional alignment is high.

As a result:

* Directionally aligned patterns with disproportionate energy do **not** receive artificially maximal scores.
* Semantic stability improves in mixed-intensity or heterogeneous datasets.
* The resulting score reflects both **coherence** and **balanced contribution**, rather than angular similarity alone.

While individual components resemble normalized inner products known in signal processing, the **combined amplitude-balanced, phase-aware scoring behavior** differs materially from standard cosine similarity and produces distinct ranking dynamics.

> **Implementation note**
> The exact mathematical form, parameterization, thresholds, and integration of amplitude-balancing into the resonance score are implementation-defined, intentionally undisclosed, and may be subject to patent protection.

---

## What Makes It Different

| Feature                          | Why It Matters                                                                                                                   |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Waveform representation          | Patterns are stored as amplitude and phase, preserving intensity, structure, and contextual relationships beyond static vectors. |
| Phase-coherent scoring           | Retrieval is driven by interference-style accumulation rather than purely geometric proximity.                                   |
| Amplitude-balanced normalization | Explicitly penalizes scale dominance, stabilizing relevance under energy imbalance.                                              |
| Phase-sharded scaling            | Patterns are routed by mean phase, enabling horizontal scaling and parallel search.                                              |
| Deterministic kernel interface   | Java and native SIMD backends share a strict mathematical contract.                                                              |
| Zero-copy memory access          | Patterns are read directly from memory-mapped segments without deserialization.                                                  |
| Crash-safe writes                | Atomic commits with checksums and commit flags support safe recovery.                                                            |
| Modular architecture             | Clean Gradle multi-project structure designed for extension and integration.                                                     |

---

## Typical Use Cases

* **Memory for cognitive agents** — storing semantic or affective traces over time.
* **Hybrid reasoning systems** — combining symbolic DAGs with resonant memory.
* **Multimodal AI research** — unifying text, image, and sensor data via a shared waveform substrate.
* **Edge-native memory caches** — on-device deployment with deterministic, zero-deserialization reads.
* **Exploratory research** — investigating alternatives to vector embeddings and metric similarity.

See also: [Applications of Wave-Based Memory](./docs/whitepapers/Applications-of-ResonanceDB-in-AGI-Memory-and-Affective-Modeling.md)

---

## Technology Snapshot

| Layer    | Snapshot                                                       |
| -------- | -------------------------------------------------------------- |
| Language | Java 22 with optional native C/SIMD via Panama FFI             |
| Storage  | Memory-mapped `.segment` files storing amplitude and phase     |
| Routing  | Phase-based sharding using mean phase φ̄                       |
| Build    | Modular Gradle 8 workspace                                     |
| License  | See **[LICENSE](./LICENSE)** (Prosperity Public License 3.0.0) |

---

## ⚡ Why Java 22?

ResonanceDB uses Java 22 primarily for the **Foreign Function & Memory (Panama) API**, enabling:

* Off-heap execution without garbage-collector involvement on critical paths.
* Near-C performance while preserving memory safety and strong encapsulation.
* Data layouts optimized for SIMD execution and CPU cache locality, suitable for waveform-based semantic processing.

---

## 🚀 Build & Run

### ✅ Requirements

* JDK 22 or newer
* GCC or Clang (for the optional native kernel)
* Gradle 8 or newer

---

## 🧩 Kernel Modes

| Backend      | Description                     | SIMD Optimized | Platform Dependent |
| ------------ | ------------------------------- | -------------- | ------------------ |
| JavaKernel   | Pure Java implementation        | ❌              | ❌                  |
| NativeKernel | Panama FFI + C (`libresonance`) | ✅              | ✅ (Linux/macOS)    |

---

## 🧱 Build Instructions

### 🛠 1. Build the native library (optional)

```bash
./gradlew :resonance-native:buildNativeLib
```

This produces `libresonance.so` (or the platform equivalent) under `resonance-native/libs/`.

### 🔨 2. Build all modules

```bash
./gradlew build
```

---

## ▶️ Run CLI

```bash
./gradlew :resonance-cli:run
```

The CLI initializes with `NativeKernel` when available and falls back to `JavaKernel` otherwise.

---

## 🔧 Kernel Selection Example

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
double[] phase = {0.0, Math.PI / 4, Math.PI / 2, Math.PI, 3 * Math.PI / 2};

WavePattern psi = new WavePattern(amplitude, phase);
resonanceStore.insert(psi, Map.of("label", "custom input"));

List<ResonanceMatch> results = resonanceStore.query(psi, 10);
```

---

## 📦 Binary Segment Format (Informational)

The following describes the **structural layout** of on-disk segments for interoperability and diagnostic purposes only. It does not disclose internal algorithms, scoring logic, or optimization strategies.

### 🧱 Segment Header

```
[Magic (4 B)] [Version (2 B)] [Timestamp (8 B)] [Record Count (4 B)]
[Last Offset (8 B)] [Checksum] [Commit Flag (1 B)] [Padding]
```

* **Magic:** `RDSN`
* **Checksum:** implementation-dependent
* **Commit flag:** written only after successful checksum validation

---

### 🧩 WavePattern Entry

```
[ID (16 B)] [Length (4 B)] [Reserved (4 B)]
[Amplitude[]] [Phase[]]
```

* **ID:** content-addressed identifier
* **Amplitude / Phase:** IEEE-754 `double`
* **Alignment:** 8-byte aligned

---

## 📄 License, Training, and Commercial Use

ResonanceDB is provided under the repository **[LICENSE](./LICENSE)**, which reproduces the **Prosperity Public License 3.0.0** verbatim and includes repository-specific interpretive notices.

**Training / automated ingestion contexts.** The repository also includes **[TRAINING_NOTICE.md](./TRAINING_NOTICE.md)**, which provides the Contributor’s interpretive clarification and advance enforcement position regarding how the Prosperity Public License 3.0.0 applies in certain technical contexts (including machine learning training and submission to third-party hosted AI services). If anything in the Training Notice appears inconsistent with the [LICENSE](./LICENSE) file, the [LICENSE](./LICENSE) file controls.

**[TRAINING_NOTICE.md](./TRAINING_NOTICE.md)** is published as advance notice of the Contributor’s interpretation and enforcement posture; it does not add obligations beyond the **[LICENSE](./LICENSE)**.

* Free for non-commercial use under the terms of the [LICENSE](./LICENSE)
* Commercial use beyond the built-in thirty (30) day commercial trial requires a paid license.

Documentation authored in this repository under `docs/` is licensed under **CC BY-ND 4.0** (see [LICENSE-docs](./LICENSE-docs)
). Externally published or third-party materials in `docs/` (e.g., arXiv PDFs) remain governed by their own notices and terms.

See **[LICENSE](./LICENSE)**, **[LICENSE-docs](./LICENSE-docs)**, and **[TRAINING_NOTICE.md](./TRAINING_NOTICE.md)** for the complete and controlling terms.

---

## 🧠 Patent Status

A provisional U.S. patent application related to ResonanceDB was filed on **June 18, 2025**.

Certain techniques described in this repository may be covered by pending patent applications. No patent rights are granted except as expressly provided under the repository **[LICENSE](./LICENSE)**, and no implied rights arise by estoppel or otherwise.

---

## 🛡️ Algorithmic Integrity

The observable behavior of ResonanceDB results from the **combined interaction** of multiple architectural elements, including, without limitation:

* phase-coherent scoring mechanisms,
* amplitude-balancing normalization,
* phase-based routing strategies,
* memory-mapped execution models, and
* deterministic kernel implementations.

Descriptions provided in this repository are **architectural and illustrative in nature**. They are intended to convey system-level concepts and design rationale, and **do not disclose** the internal parameters, execution ordering, optimization strategies, or decision logic employed in the operational implementation.

Accordingly, nothing in this section shall be interpreted as:

* an admission of algorithmic or functional equivalence to any other system;
* a disclosure of trade secrets or proprietary implementation details;
* a representation or guarantee that the described behavior is independently reproducible; or
* a grant of any license or right beyond those expressly provided under the applicable license(s).

Any rights to use, reproduce, or implement the described techniques arise, if at all, **solely under and subject to** the terms of the applicable license(s).

---

## 📫 Contact

**Author:** Aleksandr Listopad
**Licensing & Security:** [license@evacortex.ai](mailto:license@evacortex.ai)
