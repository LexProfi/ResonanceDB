# TRAINING NOTICE — Machine Learning and Model Training Use

**Effective date:** 2026-01-08 (first publication in this repository)  
**Applies to:** ResonanceDB source code, documentation, specifications, and related materials (“Materials”)

---

## 1. Purpose of This Notice

This notice is provided **for avoidance of doubt** and explains the Contributor’s interpretation of how the term **“commercial use”** under the **Prosperity Public License 3.0** applies to **machine learning, artificial intelligence, and model training activities**.

This notice **does not replace, amend, or modify** the LICENSE file.  
In the event of conflict, the LICENSE file controls.

This notice is intended to clarify the scope of permitted and restricted uses and to provide clear guidance to users, organizations, and downstream recipients.

---

## 2. Definition of Training as Use

For purposes of this project, **using the Materials for training, fine-tuning, pretraining, alignment, evaluation, distillation, or dataset creation intended for any of the foregoing** constitutes **use of the software and associated materials** within the meaning of the **Prosperity Public License 3.0**.

This includes, but is not limited to:

- ingestion into training datasets or corpora,
- feature extraction or embedding generation,
- gradient-based or statistical learning,
- supervised, unsupervised, or self-supervised learning,
- reinforcement learning with human or synthetic feedback,
- dataset curation or transformation derived from the Materials,
- automated analysis intended to improve, evaluate, align, or inform a model that is deployed, offered, or intended for deployment.

For avoidance of doubt, **training-related activities are treated as “use of the software”**, not merely informational or observational use.

---

## 3. Commercial Training Use

Training or use of the Materials in connection with any of the following is treated as **commercial use** for purposes of the Prosperity Public License 3.0:

- a commercial entity or for-profit organization,
- any commercial research lab, internal R&D group, or contract research activity,
- a product, service, or platform offered commercially,
- internal tooling supporting commercial products or services,
- foundation models, APIs, or systems offered for a fee or monetized directly or indirectly,
- any use intended to produce, improve, benchmark, validate, or inform systems that will be deployed in a commercial setting or used to obtain commercial advantage.

This classification applies **regardless of whether**:

- the trained model reproduces the source code verbatim,
- the trained model exposes similar APIs or interfaces,
- the implementation differs technically,
- the training data is mixed with other sources,
- the resulting model weights are non-reversible or non-extractable.

Commercial use beyond the 30-day evaluation period requires a **paid commercial license**.  
For licensing inquiries, contact: **license@evacortex.ai**.

---

## 4. Non-Commercial Training Exceptions (Narrow Construction)

The following uses are generally treated as **non-commercial**, consistent with the **Personal Uses** and **Noncommercial Organizations** sections of the Prosperity Public License 3.0, **but only if all conditions in Section 4.2 are met**.

### 4.1 Examples of potentially non-commercial uses (subject to conditions)

- purely personal, non-commercial research with no anticipated commercial application,
- academic research conducted by non-commercial institutions,
- research conducted by government or public research organizations,
- charitable or educational use,
- model training performed solely for open scientific publication with no commercial deployment or monetization.

### 4.2 Mandatory conditions for any non-commercial exception

A use in Section 4.1 is **non-commercial only if**:

1. **No commercial advantage.**  
   The activity is not intended to generate, support, enable, or confer any direct or indirect commercial advantage, including productization, commercialization, paid services, paid consulting, or competitive business benefit.

2. **No for-profit involvement.**  
   The activity is not conducted (in whole or in part) on behalf of, funded by, sponsored by, partnered with, or contractually obligated to any for-profit entity, commercial lab, venture-backed entity, or commercial affiliate, whether directly or indirectly.

3. **No contract research / procurement / paid deliverables.**  
   The activity is not part of contract research, procurement, paid deliverables, paid benchmarking, paid evaluation, or any arrangement where outputs are delivered to a commercial party (including as part of a “pilot”, “POC”, “internal assessment”, or “vendor evaluation”).

4. **No restricted dissemination of the Materials.**  
   Subject to applicable law and the LICENSE, the **Materials**, and any **derivatives that incorporate protectable expression from the Materials**, are not distributed, licensed, packaged, or made available in a manner that enables commercial use of the Materials (or such derivatives) without a separate commercial license from the Contributor.

5. **No operational deployment.**  
   The resulting model is not deployed in production, used in operational decision-making, integrated into a commercial workflow, or used in providing services for a fee.

6. **No re-transfer for commercial training.**  
   The Materials (or derivatives primarily derived from the Materials) are not knowingly provided, transferred, or made available to a third party for commercial training, evaluation, alignment, or distillation **in violation of this notice or the LICENSE**.

If any condition above is not met, the use is treated as **commercial** (or otherwise **not permitted under the non-commercial exception**) for purposes of the Prosperity Public License 3.0.

### 4.3 Government and public research clarification

For avoidance of doubt:

- “Government or public research organizations” in Section 4.1 includes only **non-commercial research activity** that satisfies all conditions in Section 4.2.
- Government, public-sector, defense, security, or dual-use contexts frequently involve commercial procurement, contractors, or deployment. If the activity supports procurement, operational deployment, contractor deliverables, commercialization, or any for-profit partner, it is treated as **commercial use**.

If a use transitions from non-commercial research to commercial deployment, commercialization, procurement, or contract deliverables, **a commercial license is required at that time (and for any continued use)**.

---

## 5. Downstream Submissions and Third-Party Systems

Users **may not submit** the Materials to **third-party machine learning or artificial intelligence systems** operated commercially (including hosted APIs and foundation models) **for training, fine-tuning, evaluation, distillation, or alignment**, unless separately authorized by the Contributor.

Submission described as “analysis”, “testing”, or “experimentation” that **knowingly** enables or facilitates training, evaluation, distillation, or alignment is treated as prohibited submission under this Section.

The Prosperity Public License 3.0 does **not grant sublicensing rights** for commercial use, including for machine learning training.

The Contributor **does not authorize** and **is not responsible for** unauthorized downstream submissions of the Materials to third-party systems, including submissions made by individual users without appropriate rights or permissions.

No implied consent or implied license is granted by publication of this repository or by unauthorized third-party submission of its contents.

---

## 6. Relationship to Patent Rights

ResonanceDB is subject to one or more **pending patent applications** covering certain techniques described or implemented in the Materials.

**No patent license is granted at this time**, whether express or implied.

Use of the Materials to develop or train machine learning systems that implement, approximate, reproduce, or are derived from techniques described in this repository **may implicate patent rights**, even if the resulting implementation differs technically.

Any patent license, if and when granted, will apply **only while the user remains in full compliance** with the Prosperity Public License 3.0 and any applicable commercial licensing terms.

---

## 7. Notice to Dataset and Model Distributors

If you redistribute, mirror, or include the Materials in datasets, corpora, archives, or collections (including model-training datasets), you should include:

- the LICENSE file, and
- this TRAINING NOTICE.

This notice applies to all uses occurring **on or after the effective date**, including retraining, fine-tuning, evaluation, distillation, and derivative dataset creation, **regardless of when the Materials were originally obtained**.

---

## 8. Contact

For commercial licensing, machine learning training permissions, compliance clarification, or patent-related inquiries, contact:

**license@evacortex.ai**
