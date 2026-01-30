# TRAINING NOTICE — Machine Learning, Model Training, and Automated Ingestion

**Publication date (first publication in this repository):** 2026-01-08

**Applies to:** ResonanceDB source code and other materials in this repository that are licensed under the Prosperity Public License 3.0.0 (the “Prosperity-Licensed Materials”).

**Exclusions:** This notice does **not** apply to documentation, whitepapers, preprints, or other materials that are licensed under separate licenses (including, without limitation, Creative Commons licenses), nor to third-party materials included in this repository under their respective licenses.

---

## 1. Purpose, Status, and Relationship to the LICENSE

This document provides the Contributor’s **good-faith interpretation** and **advance enforcement position** regarding how the **Prosperity Public License 3.0.0** applies to certain technical contexts involving machine learning systems, automated ingestion, and third-party AI services.

This notice is **not** a substitute for the `LICENSE` file and does **not** edit, replace, amend, narrow, expand, or otherwise modify the verbatim text of the Prosperity Public License 3.0.0 reproduced in the `LICENSE` file.

**Controlling terms.** In the event of any inconsistency (or perceived inconsistency) between this notice and the `LICENSE` file (including the verbatim Prosperity text), the `LICENSE` file controls. Any such inconsistent statement in this notice must be read as **non-binding** and **informational** only.

**Role of this notice.** For avoidance of doubt, this notice exists to:

* reduce ambiguity in emerging technical uses (including training and automated ingestion);
* provide advance notice of how the Contributor will evaluate compliance under the Prosperity Public License 3.0.0 in these contexts; and
* provide an evidentiary record of the Contributor’s interpretation and enforcement posture.

---

## 2. Training and Model Development as “Use” (Interpretive Clarification)

For purposes of interpretation, compliance evaluation, and enforcement under the Prosperity Public License 3.0.0, the Contributor considers the following activities to constitute **“use of the software”** when they involve the Prosperity-Licensed Materials (in whole or in part):

* training, fine-tuning, pretraining, alignment, evaluation, benchmarking, or distillation of machine learning or AI systems;
* dataset or corpus creation intended for any of the foregoing; and
* automated analysis intended to improve, evaluate, align, benchmark, or otherwise inform a model or system that is deployed, offered, or intended for deployment.

**Interpretive clarification.**  
This section does **not** redefine any term of the Prosperity Public License 3.0.0. It describes how the Contributor will evaluate certain factual patterns as “use” under the **ordinary meaning** of that license for purposes of compliance assessment and enforcement.

This includes, without limitation:

* ingestion into training datasets, corpora, or evaluation sets;
* feature extraction, embedding generation, representation learning, or automated summarization for training purposes;
* gradient-based, statistical, or heuristic learning;
* supervised, unsupervised, or self-supervised learning;
* reinforcement learning with human or synthetic feedback;
* dataset curation, transformation, labeling, augmentation, or filtering derived from the Prosperity-Licensed Materials.

For avoidance of doubt, training-related activities are treated as **use** where undertaken to develop, evaluate, or improve a model or system. Passive reading, inspection, or ordinary compilation/testing of the Software is not, by itself, “training.”

---

## 3. Training in Commercial Settings (Interpretation of “Commercial Use”)

This section explains how the Contributor will assess whether training-related “use” is **for a commercial purpose** under the Prosperity Public License 3.0.0.

For purposes of interpretation and enforcement, training-related use of the Prosperity-Licensed Materials will be treated as **commercial use** when it occurs:

* by or for a commercial entity or for-profit organization;
* within a commercial research lab, internal R&D group, or contract research setting;
* in connection with a product, service, or platform offered commercially;
* as internal tooling that supports commercial products or services;
* in the development, evaluation, or operation of foundation models, APIs, or systems offered for a fee or monetized directly or indirectly; or
* in any activity intended to produce, improve, validate, benchmark, or inform systems that will be deployed in a commercial setting or used to obtain commercial advantage.

This assessment applies regardless of whether:

* the trained system reproduces source code verbatim;
* the trained system exposes similar APIs or interfaces;
* the technical implementation differs;
* the training data is mixed with other sources; or
* the resulting weights or parameters are non-reversible or non-extractable.

**Commercial trial and commercial licensing.** The Prosperity Public License 3.0.0 permits a thirty (30) day commercial trial. Commercial use beyond that built-in trial requires a separate paid commercial license.

For commercial licensing inquiries, contact: **[license@evacortex.ai](mailto:license@evacortex.ai)**.

---

## 4. Non-Commercial Research Use (Consistency With Prosperity Exceptions)

Nothing in this notice is intended to restrict or override the **Personal Uses** or **Noncommercial Organizations** provisions of the Prosperity Public License 3.0.0.

Accordingly, use of the Prosperity-Licensed Materials by a **Noncommercial Organization**, as defined in the Prosperity Public License 3.0.0, is treated as **non-commercial** to the extent and for so long as such use falls within the scope permitted by the Prosperity Public License 3.0.0.

### 4.1 Research involving commercial counterparties (evidentiary and compliance framing)

In arrangements involving both noncommercial and commercial participants, the Contributor may evaluate the **factual substance** of the arrangement when assessing compliance and enforcement (for example: who controls the work, who receives the benefit, and whether a commercial entity is itself making use of the Software).

For avoidance of doubt:

* A commercial entity that uses the Prosperity-Licensed Materials (directly or indirectly) must independently ensure its own compliance with the Prosperity Public License 3.0.0, including the commercial trial limitation and any requirement to obtain a paid commercial license beyond that trial.
* This section is not intended to redefine “Noncommercial Organizations” or otherwise alter any exception in the Prosperity Public License 3.0.0.

### 4.2 Transition to commercial deployment

If your use of the Prosperity-Licensed Materials (or any work you create using them) transitions from non-commercial research into any activity that constitutes **commercial use** under the Prosperity Public License 3.0.0 (including, without limitation, commercial deployment, commercialization, provision of paid services, or delivery of outputs for commercial advantage), then commercial use beyond the built-in thirty (30) day trial requires a paid commercial license.

---

## 5. Submissions to Third-Party AI Systems (Hosted Services)

Submitting the Prosperity-Licensed Materials (in whole or in part) to third-party AI systems operated commercially (including hosted APIs, SaaS platforms, foundation models, large language models (LLMs), code assistants, embedding services, inference platforms, and cloud-based AI services) can create compliance and IP risks, including retention and secondary use outside your control.

For purposes of interpretation and enforcement, where such submission is undertaken in a manner that **enables or facilitates** training, evaluation, distillation, alignment, benchmarking, or model improvement by the third party, the Contributor will generally treat that activity as **“use”** and (where undertaken for commercial advantage or in commercial settings) as **commercial use** under the Prosperity Public License 3.0.0.

Characterizing such submission as “analysis”, “testing”, or “experimentation” does not alter this assessment if the factual substance of the submission knowingly enables or facilitates model improvement by a third party.

For avoidance of doubt:

* This section addresses hosted systems where retention, reuse, or training may occur outside your control.
* The Prosperity Public License 3.0.0 grants rights only to the extent stated in its text, and does not provide any additional permissions to authorize third-party commercial training or reuse beyond those rights.
* No implied consent or implied license is granted by publication of this repository for third-party training or reuse beyond the rights expressly granted under the Prosperity Public License 3.0.0.

---

## 6. Relationship to Patent Rights (Notice)

Certain techniques described or implemented in the Prosperity-Licensed Materials may be subject to one or more pending patent applications.

No patent license is granted beyond what is expressly provided under the Prosperity Public License 3.0.0, and any such license applies only while the user remains in full compliance with its terms.

Use of the Prosperity-Licensed Materials to develop, train, or evaluate machine learning systems that implement, approximate, reproduce, or are derived from techniques described in this repository may implicate patent rights, even where the resulting implementation differs technically.

---

## 7. Notice to Dataset and Model Distributors (Downstream Transparency)

If you redistribute, mirror, or include the Prosperity-Licensed Materials in datasets, corpora, archives, or collections (including model-training datasets), the Contributor strongly recommends that you include:

* the `LICENSE` file; and
* this `TRAINING_NOTICE.md`.

This recommendation is intended to help downstream recipients understand the Contributor’s compliance and enforcement posture in training-related contexts and to reduce ambiguity.

Nothing in this section is intended to add to or modify the Prosperity Public License 3.0.0 “Notices” rule or to impose additional binding notice obligations beyond those contained in the Prosperity Public License 3.0.0.

---

## 8. Contact

For commercial licensing, training permissions, compliance clarification, or patent-related inquiries, contact:

**[license@evacortex.ai](mailto:license@evacortex.ai)**