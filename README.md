# diced — NYC Crashes × Infrastructure

A dbt + DuckDB analytics engineering project that layers NYC road infrastructure onto motor vehicle crash data. Runs entirely locally — no cloud account, no external APIs required.

**Central question:** Which street segments have the highest injury-weighted crash rate, and what road design factors are associated with it?

---

## Quickstart

```bash
dbt run        # build all models
dbt test       # run all schema tests
dbt docs serve # browse the DAG and column docs at localhost:8080
```

Requires a `~/.dbt/profiles.yml` pointing at `diced` with a DuckDB target.

---

## Data Sources

| Dataset | Source |
|---|---|
| NYC Motor Vehicle Collisions | NYC Open Data (h9gi-nx95) |
| NYC Centerline (CSCL) Shapefile | NYC Dept of City Planning |
| DOT Pavement Condition Index | NYC DOT Open Data |

---

## DAG

```
stg_collisions ──┬─→ int_crash_factors_unpivoted ──→ fct_crash_factors
                 └─→ int_collisions_geocoded ──────┐
stg_street_segments ──→ dim_street_segments ───────┤
                              └─→ dim_intersections │
                                                    └──→ fct_crashes_enriched
                                                              │
                                         ┌────────────────────┘
                                         ├──→ rpt_factor_contributions
                                         └──→ rpt_dangerous_corridors
stg_pavement (staged but unlinked — see Limitations)
```

### Layer Summary

**Staging** — Raw sources cast, decoded, and filtered. Codes translated to readable values. No business logic.

**Intermediate** — Transformation steps not suitable for direct consumption. `int_collisions_geocoded` spatially joins each crash to its nearest driveable segment using `ST_DWithin` + `ST_Distance` + `QUALIFY ROW_NUMBER()`. `int_crash_factors_unpivoted` pivots five per-vehicle factor columns into one row per (crash, factor).

**Marts** — Public-facing dimensional models. `fct_crashes_enriched` is the primary crash fact (one row per geocoded crash). `dim_street_segments` and `dim_intersections` are the road dimensions. `fct_crash_factors` is the crash × factor junction table.

**Reports** — Analytical outputs. `rpt_factor_contributions` ranks road attributes by crash rate-ratio. `rpt_dangerous_corridors` ranks segments by severity-weighted crash score with top contributing factors.

---

## Key Decisions

**`ST_DWithin(..., 0.001)` pre-filter before `ST_Distance`**
A brute-force cross join between crashes and segments OOM'd at ~50GB. The pre-filter limits candidates to segments within ~111m before computing exact distance.

**Decode coded columns in staging; apply business rules in intermediate**
Staging is a faithful representation of the source with human-readable labels. Filters for driveable road types live in `int_collisions_geocoded` and `dim_street_segments`, not in staging.

**`UNION ALL` over `UNPIVOT` for contributing factors**
More explicit for a learning context; easier to trace which source column each row came from.

**`fct_crash_factors` uses `DISTINCT` to collapse per-vehicle duplicates**
NYC's factor columns are per-vehicle — the same factor can appear for multiple vehicles in one crash. Without `DISTINCT`, `count(*)` in reports inflates factor counts by ~7%.

**Severity score: `(persons_killed × 5) + (persons_injured × 1)`**
NYC crash data does not distinguish injury severity (incapacitating vs. non-incapacitating). Full KABCO weighting requires five tiers; this is a two-tier approximation. Property-only crashes score 0 and are excluded from corridor rankings.

**Borough excluded from `fct_crashes_enriched`**
Borough is an attribute of the segment, reachable via `segment_id → dim_street_segments.borough`. Duplicating it on the fact would violate dimensional discipline and relies on a sparse self-reported field in the collision source.

**Rate-ratio baseline: total geocoded crashes / total segments**
`rpt_factor_contributions` uses crashes-per-segment as the unit. The baseline is the overall rate across all geocoded crashes and all driveable segments. A `rate_ratio > 1` means that (attribute, factor) combination crashes more per segment than average.

---

## Assumptions

- Crashes filtered to those with valid lat/lon coordinates in the source. Ungeocoded crashes are excluded from all spatial models.
- Contributing factors labeled `Unspecified` are excluded — treated as missing, not meaningful.
- Driveable segments are: `street`, `highway`, `ramp`, `alley`, `driveway`. All other CSCL road types are filtered out.
- Pavement data is 1–10 scale (not 0–100 as originally documented). Staged but not yet linked to segments — see Limitations.
- Bike lane classes follow NYC DOT classification: Class I = protected, Class II = painted, Class III = sharrow.

---

## Limitations

**No traffic volume normalization**
Crash rates are per segment, not per vehicle. A busy arterial will always score higher than a quiet residential street regardless of actual risk per trip. NYC DOT publishes automated traffic volume counts but coverage is sparse.

**Pavement data unlinked**
`stg_pavement` is staged but has no `segment_id` — linking requires a spatial or name-based join not yet implemented.

**Permits dataset excluded**
The NYC Street Opening Permits CSV had sub-1% WKT geometry coverage, making a spatial join unviable. The construction-activity dimension was dropped from scope.

**Segment length not normalized**
Longer segments accumulate more crashes by geometry alone. `rpt_dangerous_corridors` ranks by total severity score, not crashes per mile.

**Two-tier severity approximation**
Full injury severity breakdown (fatal / incapacitating / non-incapacitating / possible / property-only) is not available in the NYC crash source. The severity score is a simplified proxy.

---

## Insights

- **Highways and ramps dominate the danger corridor ranking** due to high speed limits amplifying severity scores. This reflects severity accumulation, not necessarily elevated risk per trip.
- **Protected bike lane segments** show elevated crash rates, likely because they tend to be on busier arterials — not evidence that protected lanes cause crashes.
- **"Driver Inattention/Distraction"** is the most cited contributing factor across nearly all road types and attribute values, consistent with national crash data.
- **The spatial join pre-filter threshold (0.001°)** excludes crashes more than ~111m from any driveable segment — affecting crashes in parking lots, private roads, or with imprecise coordinates.

---

## Future Work

- Normalize `rpt_dangerous_corridors` by segment length (crashes per mile) using `ST_Length(geom)`
- Add a street-only filter to corridor rankings, separating city streets from highways and ramps
- Link pavement ratings to segments via spatial or name-based join
- Incorporate traffic volume data for exposure-based crash rates (crashes per vehicle)
- Extend `rpt_factor_contributions` to Option A grain: one row per `(speed_limit, bike_lane, road_class, contributing_factor)` for full intersection of attributes
