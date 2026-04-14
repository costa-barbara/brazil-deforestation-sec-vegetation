# Deforestation & Secondary Vegetation — Temporal Rules

**MapBiomas Brazil · Collection 10.1**

This document describes the temporal rules applied to detect deforestation and secondary vegetation dynamics. Rules are implemented in [`deforestation_secondary_vegetation.py`](./deforestation_secondary_vegetation.py) and invoked from [`mapbiomas_brazil_export_pipeline.py`](./mapbiomas_brazil_export_pipeline.py).

---

## Table of Contents

- [1. Class Legend](#1-class-legend)
- [2. Rule Format](#2-rule-format)
- [3. The `min_start` Parameter](#3-the-min_start-parameter)
- [4. Processing Order](#4-processing-order)
- [5. RULES\_KERNEL4](#5-rules_kernel4--primary-vegetation-suppression)
- [6. RULES\_KERNEL4\_SECONDARY](#6-rules_kernel4_secondary--secondary-vegetation-dynamics)
- [7. RULES\_KERNEL3\_SECONDARY](#7-rules_kernel3_secondary--suppression-within-secondary-vegetation)
- [8. RULES\_KERNEL4\_END](#8-rules_kernel4_end--unconfirmed-end-of-series-transitions)
- [9. Post-Processing Corrections](#9-post-processing-corrections)
- [10. Known Issue: Secondary Vegetation in Early Years](#10-known-issue-secondary-vegetation-in-early-years)

---

## 1. Class Legend

These are the simplified class codes used internally by the deforestation pipeline, after aggregation from the original MapBiomas classification via `aggregate_classes()`.

| ID | Name                        | Description                                              |
|----|-----------------------------|----------------------------------------------------------|
| 0  | No Data                     | Unobserved or masked pixels                              |
| 1  | Anthropic                   | Any anthropic land use (pasture, agriculture, urban...)  |
| 2  | Primary Vegetation          | Native vegetation with no prior anthropic history        |
| 3  | Secondary Vegetation        | Vegetation regrowth on previously anthropic land         |
| 4  | Primary Veg. Suppression    | Confirmed deforestation event on primary vegetation      |
| 5  | Recovery to Secondary       | First year of vegetation regrowth after anthropic use    |
| 6  | Secondary Veg. Suppression  | Deforestation event on secondary vegetation              |
| 7  | Other Transitions           | Water, rocky outcrops, beaches, filtered noise           |

---

## 2. Rule Format

Each rule is defined as a Python list with the following structure:

```python
[kernel_bef, kernel_aft, min_start]
```

| Field        | Type       | Required | Description                                                                 |
|--------------|------------|----------|-----------------------------------------------------------------------------|
| `kernel_bef` | `list[int]`| yes      | Sequence of class IDs that must match the input bands (before state)        |
| `kernel_aft` | `list[int]`| yes      | Sequence of class IDs to write on match (after state)                       |
| `min_start`  | `int`      | **yes**  | Minimum index in `YEARS` from which the rule may be applied (`0` = 1985)   |

> **Enforcement:** `min_start` is mandatory. Both `apply_rule_kernel_4` and `apply_rule_kernel_3` raise a `ValueError` if the third element is missing, preventing silent errors from unreviewed rules.

**Example:**

```python
[[1, 2, 2, 2], [1, 5, 3, 3], 1]
#  ↑ before     ↑ after       ↑ min_start=1 → first valid window starts at 1986
```

For a 4-year kernel, position indices map to years as follows:

```
index:  i=0   i=1   i=2   i=3
        t1    t2    t3    t4
        1985  1986  1987  1988   ← when window starts at index 0
        1986  1987  1988  1989   ← when window starts at index 1 (min_start=1)
```

---

## 3. The `min_start` Parameter

### Problem

Some rules assign class `3` (Secondary Vegetation) or `5` (Recovery to Secondary) to position `t2`. When the sliding window starts at index `i=0`, `t2` corresponds to **1986** — the second year of the series. This is incorrect because there is no historical context before 1985 to confirm that a prior anthropic or secondary vegetation state existed.

### Decision Criteria

| Condition                                                  | `min_start` | Rationale                                                              |
|------------------------------------------------------------|-------------|------------------------------------------------------------------------|
| `kernel_aft[1]` assigns class `3` or `5` (new assignment) | `1`         | `t1` must represent a confirmed prior state; 1985 has no prior history |
| `kernel_aft[1]` is already class `3` (no new assignment)  | `0`         | `t2` already holds class `3` from input; no risk of false assignment   |
| `kernel_aft` does not assign class `3` or `5` anywhere    | `0`         | No secondary vegetation classes written; no risk                       |
| `RULES_KERNEL4_END` (uses `years_override=YEARS_END`)     | `0`         | Window never reaches 1985/1986; `years_override` already constrains scope |

> **Note:** `min_start=0` is always set **explicitly** — it is not an omission, but a documented confirmation that the rule was evaluated and considered safe from the first year of the series.

### Implementation

Both `apply_rule_kernel_4` and `apply_rule_kernel_3` enforce `min_start` in the same way: a `ValueError` is raised if the third element is absent, and the condition `.And(i.gte(min_start))` is added to the pixel mask.

```python
def apply_rule_kernel_4(self, rule, years):
    if len(rule) < 3:
        raise ValueError(
            f"Rule is missing min_start (3rd element): {rule}. "
            "Set min_start=0 explicitly if the rule is safe from the first year of the series."
        )
    kernel_bef = rule[0]
    kernel_aft = rule[1]
    min_start  = ee.Number(rule[2])

    def apply_kernel_4(i, image):
        ...
        mask = (
            t1.eq(kernel_bef[0])
            .And(t2.eq(kernel_bef[1]))
            .And(t3.eq(kernel_bef[2]))
            .And(t4.eq(kernel_bef[3]))
            .And(i.gte(min_start))   # blocks windows before min index
        )
```

```python
def apply_rule_kernel_3(self, rule, years):
    if len(rule) < 3:
        raise ValueError(
            f"Rule is missing min_start (3rd element): {rule}. "
            "Set min_start=0 explicitly if the rule is safe from the first year of the series."
        )
    kernel_bef = rule[0]
    kernel_aft = rule[1]
    min_start  = ee.Number(rule[2])

    def apply_kernel_3(i, image):
        ...
        mask = (
            t1.eq(kernel_bef[0])
            .And(t2.eq(kernel_bef[1]))
            .And(t3.eq(kernel_bef[2]))
            .And(i.gte(min_start))   # blocks windows before min index
        )
```

The `.And(i.gte(min_start))` condition is a scalar EE expression broadcast to all pixels, adding no significant computational cost.

---

## 4. Processing Order

Rules are applied sequentially in `export_deforestation()`. Each step operates on the output of the previous one.

```python
# Step 1 — aggregate original MapBiomas class IDs to simplified codes (0–7)
aggregated = DeforestationSecondaryVegetation.aggregate_classes(integration, LOOKUP_IN, LOOKUP_OUT)

processor = DeforestationSecondaryVegetation(aggregated, YEARS)

# Step 2 — confirmed primary vegetation suppression
processor.apply_rules(RULES_KERNEL4, kernel_size=4)

# Step 3 — secondary vegetation dynamics (establishment, persistence, suppression)
processor.apply_rules(RULES_KERNEL4_SECONDARY, kernel_size=4)

# Step 4 — suppression within secondary vegetation
processor.apply_rules(RULES_KERNEL3_SECONDARY, kernel_size=3)

# Step 5 — unconfirmed transitions at end of series
processor.apply_rules(RULES_KERNEL4_END, kernel_size=4, years_override=YEARS_END)

# Step 6 — frequency-based corrections and last-year post-processing
```

---

## 5. RULES\_KERNEL4 — Primary Vegetation Suppression

**Kernel size:** 4 years · **Applied to:** full series (1985–2024) · **`min_start` needed:** no

Detects confirmed deforestation of primary vegetation: two prior years of primary vegetation followed by a confirmed transition to anthropic use.

No rule in this group assigns class `3` or `5`, so `min_start=0` for all rules.

```python
RULES_KERNEL4 = [
    [[2, 2, 1, 1], [2, 2, 4, 1], 0],  # Primary vegetation suppression
]
```

| Rule | t1 | t2 | t3 | → | t1' | t2' | t3' | t4' | min_start |
|------|----|----|----|----|-----|-----|-----|-----|-----------|
| Primary vegetation suppression | 2 | 2 | 1 | 1 | 2 | 2 | **4** | 1 | 0 |

**Reading the rule:** if the pixel was primary vegetation (`2`) in two consecutive years, then became anthropic (`1`) for two years, the third year is marked as suppression (`4`). The confirmation requires two years of anthropic use to avoid false positives from single-year noise.

---

## 6. RULES\_KERNEL4\_SECONDARY — Secondary Vegetation Dynamics

**Kernel size:** 4 years · **Applied to:** full series (1985–2024) · **`min_start` needed:** yes (3 rules)

The most complex rule group, covering establishment, persistence, and suppression of secondary vegetation. Three rules require `min_start=1` due to new assignment of class `3` or `5` at `t2`.

```python
RULES_KERNEL4_SECONDARY = [
    [[1, 2, 2, 2], [1, 5, 3, 3], 1],  # Establishment of secondary vegetation
    [[5, 3, 3, 2], [5, 3, 3, 3], 0],  # Recovery to secondary vegetation: persistence
    [[3, 2, 2, 2], [3, 3, 3, 3], 1],  # Secondary vegetation persistence
    [[3, 2, 2, 4], [3, 3, 3, 4], 1],  # Secondary vegetation with suppression at end
    [[3, 3, 2, 4], [3, 3, 3, 6], 0],  # Suppression with recovery
    [[3, 3, 2, 2], [3, 3, 3, 3], 0],  # Persistence with interrupted primary
    [[3, 3, 3, 2], [3, 3, 3, 3], 0],  # Persistence: 3 years confirmed, last year primary
    [[1, 2, 2, 4], [1, 1, 1, 1], 0],  # Primary suppression with subsequent recovery
]
```

| Rule | t1 | t2 | t3 | t4 | → | t1' | t2' | t3' | t4' | min_start | Note |
|------|----|----|----|----|---|-----|-----|-----|-----|-----------|------|
| Establishment of secondary vegetation | 1 | 2 | 2 | 2 | | 1 | **5** | **3** | **3** | **1** | ⚠️ t2 receives class 5 |
| Recovery to secondary: persistence    | 5 | 3 | 3 | 2 | | 5 | 3   | 3   | **3** | 0 | t2 already class 3 |
| Secondary vegetation persistence      | 3 | 2 | 2 | 2 | | 3 | **3** | **3** | **3** | **1** | ⚠️ t2 receives class 3 |
| Secondary vegetation + suppression    | 3 | 2 | 2 | 4 | | 3 | **3** | **3** | 4   | **1** | ⚠️ t2 receives class 3 |
| Suppression with recovery             | 3 | 3 | 2 | 4 | | 3 | 3   | **3** | **6** | 0 | t2 already class 3 |
| Persistence with interrupted primary  | 3 | 3 | 2 | 2 | | 3 | 3   | **3** | **3** | 0 | t2 already class 3 |
| Persistence: 3 confirmed years        | 3 | 3 | 3 | 2 | | 3 | 3   | 3   | **3** | 0 | t2 already class 3 |
| Primary suppression + recovery        | 1 | 2 | 2 | 4 | | 1 | 1   | 1   | 1   | 0 | no class 3 or 5 written |

**Reading the ⚠️ rules:**

- `[[1, 2, 2, 2], [1, 5, 3, 3], 1]` — detects `anthropic → primary → primary → primary` and interprets the primary vegetation as secondary regrowth. Requires `t1` to be a confirmed prior anthropic state, which 1985 cannot provide. `min_start=1` ensures the earliest window is `[1986, 1987, 1988, 1989]`.
- `[[3, 2, 2, 2], [3, 3, 3, 3], 1]` — detects `secondary → primary → primary → primary` and consolidates all years as secondary vegetation. For `t1=1985` to be class `3`, a prior rule would have had to write it — which is impossible in the first year. `min_start=1` prevents the propagation.
- `[[3, 2, 2, 4], [3, 3, 3, 4], 1]` — same rationale as above, with a suppression event at `t4`.

---

## 7. RULES\_KERNEL3\_SECONDARY — Suppression Within Secondary Vegetation

**Kernel size:** 3 years · **Applied to:** full series (1985–2024) · **`min_start` needed:** no

Detects suppression events within areas already classified as secondary vegetation. No rule assigns class `3` or `5` at `t2`, so `min_start=0` for all rules.

Additionally, this group depends on class `3` being present at `t1`, which only occurs if a prior rule in `RULES_KERNEL4_SECONDARY` already wrote it correctly. With `min_start` constraints in place for that group, the dependency chain is safe.

```python
RULES_KERNEL3_SECONDARY = [
    [[3, 4, 1], [3, 6, 1], 0],  # Suppression of secondary vegetation
]
```

| Rule | t1 | t2 | t3 | → | t1' | t2' | t3' | min_start |
|------|----|----|----|----|-----|-----|-----|-----------|
| Suppression of secondary vegetation | 3 | 4 | 1 | | 3 | **6** | 1 | 0 |

**Reading the rule:** if the pixel was secondary vegetation (`3`), then had a primary vegetation suppression event (`4`), and then returned to anthropic (`1`), the suppression event is reclassified as secondary vegetation suppression (`6`). `t2` receives class `6`, not `3` or `5`, so no risk at early years.

---

## 8. RULES\_KERNEL4\_END — Unconfirmed End-of-Series Transitions

**Kernel size:** 4 years · **Applied to:** `YEARS_END` (last 4 years) · **`min_start` needed:** no

Handles transitions that cannot be confirmed by a following year because the series has ended. Applied with `years_override=YEARS_END`, so the window never reaches 1985/1986. Writes only class `4` or `6` at `t4` — never class `3` or `5`.

```python
RULES_KERNEL4_END = [
    [[2, 2, 2, 1], [2, 2, 2, 4], 0],  # Unconfirmed primary vegetation deforestation
    [[3, 3, 3, 1], [3, 3, 3, 6], 0],  # Unconfirmed secondary vegetation deforestation
]
```

| Rule | t1 | t2 | t3 | t4 | → | t1' | t2' | t3' | t4' | min_start |
|------|----|----|----|----|---|-----|-----|-----|-----|-----------|
| Unconfirmed primary deforestation   | 2 | 2 | 2 | 1 | | 2 | 2 | 2 | **4** | 0 |
| Unconfirmed secondary deforestation | 3 | 3 | 3 | 1 | | 3 | 3 | 3 | **6** | 0 |

**Reading the rules:** in a confirmed deforestation rule (e.g. `RULES_KERNEL4`), the transition requires two years of anthropic use to avoid noise. At the end of the series, the second confirmation year does not exist yet. These rules mark the last year as suppression (`4` or `6`) based on a single year of anthropic use, accepting the lower confidence.

---

## 9. Post-Processing Corrections

After all temporal rules are applied, two additional corrections are performed directly in `export_deforestation()`.

### 9.1 Frequency-Based Reclassification

Computes per-pixel anthropic frequency across all years using `get_class_frequency()`.

| Condition | Action | Rationale |
|-----------|--------|-----------|
| `freq_anthropic > 1` AND pixel = class `4` | → class `6` | If a pixel has been anthropic more than once, a primary suppression is likely a secondary suppression |
| `freq_anthropic > 0` AND pixel = class `2` | → class `3` | If a pixel has any prior anthropic history, current primary vegetation is likely secondary regrowth |

```python
deforestation_transitions = deforestation_transitions.where(
    freq_anthropic.gt(1).And(deforestation_transitions.eq(CLASS_PRIMARY_VEG_SUPPRESSION)),
    CLASS_SECONDARY_VEG_SUPPRESSION
)
deforestation_transitions = deforestation_transitions.where(
    freq_anthropic.gt(0).And(deforestation_transitions.eq(CLASS_PRIMARY_VEGETATION)),
    CLASS_SECONDARY_VEGETATION
)
```

### 9.2 Last Three Years Adjustment

Corrects unconfirmed secondary vegetation assignments in the final three years of the series (`y1=YEARS[-3]`, `y2=YEARS[-2]`, `y3=YEARS[-1]`):

| Condition | Action | Purpose |
|-----------|--------|---------|
| `t1 = anthropic` AND `t2 = secondary veg.` | revert `t2` to anthropic | Avoids false secondary vegetation with only 2 years of context |
| `t1 = anthropic` AND `t3 = secondary veg.` | revert `t3` to anthropic | Same, applied to last year |
| `t2 = anthropic` AND `t3 = secondary veg.` | revert `t3` to anthropic | Single-year regrowth at end of series is unreliable |
| `t2 = secondary veg.` AND `t3 = anthropic` | revert `t3` to secondary veg. | Avoids false deforestation in the last year without confirmation |

---

## 10. Known Issue: Secondary Vegetation in Early Years

### Symptom

Classes `3` (Secondary Vegetation) and `5` (Recovery to Secondary) were incorrectly appearing in the second year of the historical series (1986) on the output deforestation map.

### Root Cause

Rules in `RULES_KERNEL4_SECONDARY` that assign class `3` or `5` at position `t2` were being applied starting from window index `i=0`, which corresponds to the year window `[1985, 1986, 1987, 1988]`. In this window, `t2 = 1986` would receive class `5` or `3` — even though no prior anthropic or secondary vegetation history existed before 1985 to validate the transition.

The three affected rules were:

```python
[[1, 2, 2, 2], [1, 5, 3, 3], ...]  →  t2 (1986) assigned class 5
[[3, 2, 2, 2], [3, 3, 3, 3], ...]  →  t2 (1986) assigned class 3
[[3, 2, 2, 4], [3, 3, 3, 4], ...]  →  t2 (1986) assigned class 3
```

### Fix Applied

The `min_start` parameter was introduced to the rule format and set to `1` for the three affected rules. This ensures the sliding window starts at index `i=1` at minimum, so the earliest window begins at `[1986, 1987, 1988, 1989]` and `t2 = 1987`.

The parameter was also added explicitly with value `0` to all other rules to make the interface consistent and self-documenting. A value of `0` means the rule was explicitly reviewed and confirmed safe from the first year of the series.
