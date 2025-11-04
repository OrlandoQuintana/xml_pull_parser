# XML → Flat Record Parser (Rust)

A high-performance, statically-typed XML parser and flattener built in Rust using [`quick-xml`](https://docs.rs/quick-xml/latest/quick_xml/) and zero dynamic dispatch.  
Designed for **large hierarchical XML datasets** that must be converted into **flat tabular rows** for storage in **Parquet / Delta Lake / Arrow**.

---

## Core Concept

This project demonstrates how to **stream-parse** arbitrarily nested XML into strongly-typed Rust structs and then **flatten** the hierarchical data into row-wise records.

The architecture is **static** (no reflection, no dynamic schema maps), meaning:
- Everything compiles down to pure, monomorphized Rust.
- All XML paths are handled via explicit match arms.
- Flattening behavior is predictable and type-safe.

---

## Example Schema Overview

Each `<record>` in the XML can contain:
- Scalar fields like `<name>`, `<value>`, `<category>`
- List fields like `<id>` and `<tag>`
- Nested collections like:
  ```xml
  <readings>
    <reading>
      <timestamp>2025-10-24</timestamp>
      <sensor>temp</sensor>
      <value>42.1</value>
    </reading>
    ...
  </readings>
  ```

These nested `<reading>` entries are parsed into typed structs (`Reading`) and then cross-joined with the outer lists (e.g. IDs, tags) to produce fully flattened rows.

---

## Architectural Layers

### 1. `Record` (Hierarchical In-Memory Representation)
```rust
struct Record {
    id: Option<Vec<i32>>,
    name: Option<Vec<String>>,
    value: Option<Vec<f64>>,
    category: Option<Vec<String>>,
    tags: Option<Vec<String>>,
    meta_created: Option<Vec<String>>,
    meta_updated: Option<Vec<String>>,
    readings: Option<Vec<Reading>>,
}
```

- Every XML field that can appear multiple times is represented as an `Option<Vec<T>>`.
- Nested structures (like `<reading>`) get their own structs with typed fields.

---

### 2. `Reading` (Nested Sub-Record)
```rust
struct Reading {
    timestamp: Option<String>,
    sensor: Option<String>,
    value: Option<f64>,
}
```
Created when the parser encounters a `<reading>` start tag, populated as child tags (`<timestamp>`, `<sensor>`, `<value>`) are read.

---

### 3. `FlatRecord` (Flattened Tabular Row)
```rust
struct FlatRecord {
    id: Option<i32>,
    name: Option<String>,
    value: Option<f64>,
    category: Option<String>,
    tag: Option<String>,
    meta_created: Option<String>,
    meta_updated: Option<String>,
    reading_timestamp: Option<String>,
    reading_sensor: Option<String>,
    reading_value: Option<f64>,
}
```
Produced by the `Record::flatten()` method.  
Each combination of `(id × tag × reading)` yields one `FlatRecord`.

---

### 4. Parser (`parse_records`)
- Uses `quick_xml::Reader` to **stream** through the document.
- Maintains a live `path: Vec<String>` stack.
- Calls `insert_text(&path, &text)` on each text event.
- On `</record>` close, appends the fully populated `Record` to a results vector.

---

## How to Adapt to a New Schema

### Define your new data model

If your new XML contains multiple repeating nested structures (e.g. `<measurements>`, `<components>`, `<logs>`), give each one its own typed struct:

```rust
#[derive(Debug, Clone, Default)]
struct Measurement {
    type_: Option<String>,
    unit: Option<String>,
    value: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct Component {
    id: Option<String>,
    status: Option<String>,
    readings: Option<Vec<Measurement>>,
}
```

Then include them inside `Record`:
```rust
struct Record {
    ...
    components: Option<Vec<Component>>,
    measurements: Option<Vec<Measurement>>,
}
```

---

### Extend `insert_text` with new XML paths

Each unique leaf node in your schema corresponds to a new `match` arm:
```rust
match joined.as_str() {
    "record.components.component.id" =>
        if let Some(vec) = self.components.as_mut() {
            if let Some(last) = vec.last_mut() {
                last.id = Some(text.trim().to_string());
            }
        },

    "record.components.component.readings.measurement.value" =>
        if let Ok(v) = text.trim().parse::<f64>() {
            if let Some(cmp) = self.components.as_mut() {
                if let Some(last) = cmp.last_mut() {
                    if let Some(readings) = last.readings.as_mut() {
                        if let Some(last_m) = readings.last_mut() {
                            last_m.value = Some(v);
                        }
                    }
                }
            }
        },
    ...
}
```

> Tip: you can quickly build these match arms by printing `joined` when parsing unknown XML paths.

---

### Detect nested object starts

In your `Event::Start` handler:
```rust
if inside_record && name == "component" {
    current.components.get_or_insert_with(Vec::new).push(Component::default());
}
if inside_record && name == "measurement" {
    if let Some(components) = current.components.as_mut() {
        if let Some(last) = components.last_mut() {
            last.readings.get_or_insert_with(Vec::new).push(Measurement::default());
        }
    }
}
```

This ensures that as soon as you enter a new XML element, the corresponding vector and object exist.

---

### Extend `flatten()` logic

To produce tabular rows, decide which lists to **cross-join**.

For example, if you want one row per `(id × component × measurement)`:
```rust
for id in ids {
    for component in components {
        for meas in component.readings {
            rows.push(FlatRecord {
                id: Some(*id),
                component_id: component.id.clone(),
                measurement_type: meas.type_.clone(),
                measurement_value: meas.value,
                ...
            });
        }
    }
}
```

Keep scalar fields (`name`, `meta_*`) fixed per record.

---

### Update test XML generator

Modify `big_xml()` to synthesize a realistic variant of your target schema.  
Start simple and expand depth gradually:
```xml
<record>
  <components>
    <component>
      <id>COMP123</id>
      <status>active</status>
      <readings>
        <measurement><type>temp</type><unit>C</unit><value>42.0</value></measurement>
      </readings>
    </component>
  </components>
</record>
```

This guarantees your flattening logic is validated before deploying on real data.

---

## General Strategy for Large Schemas (80+ columns)

| Principle | Description |
|------------|--------------|
| **Modular structs** | Break the XML into logical sub-entities, each with its own struct. |
| **Static field mapping** | Avoid dynamic lookups; use static path matches for speed and compile-time safety. |
| **Streaming parse** | Don’t load the entire XML; process `<record>` chunks sequentially. |
| **Deterministic flattening** | Only perform cross-products where necessary; keep scalar metadata fixed. |
| **Graceful defaults** | Use `Option` for optional data and avoid panics for missing fields. |
| **Explicit schema evolution** | When schema changes, create `RecordV2`/`RecordV3` structs to keep old logic stable. |

---

## 🚀 Performance Tips

- Use `--release` mode for >100× faster streaming.
- Avoid cloning large vectors; reuse where possible.
- For parallelization, split XML by `<record>` boundaries and send chunks to thread workers.
- To write to Parquet / Delta:
  - Use [`arrow2`](https://docs.rs/arrow2/) or [`delta-rs`](https://github.com/delta-io/delta-rs) with your flattened rows.
- Bench with realistic data (100K+ records) to measure throughput.

---

## Example Output

```
Record {
  id: Some([1997, 2997]),
  tags: Some(["fast", "resilient"]),
  readings: Some([
    Reading { timestamp: Some("2025-10-14"), sensor: Some("pressure"), value: Some(41.73) },
    Reading { timestamp: Some("2025-10-29"), sensor: Some("temp"), value: Some(66.31) },
  ])
}

Flattened rows:
  FlatRecord { id: Some(1997), tag: Some("fast"), reading_sensor: Some("pressure"), reading_value: Some(41.73) }
  FlatRecord { id: Some(1997), tag: Some("fast"), reading_sensor: Some("temp"), reading_value: Some(66.31) }
  FlatRecord { id: Some(2997), tag: Some("resilient"), reading_sensor: Some("pressure"), reading_value: Some(41.73) }
  ...
```

---

## Dependencies

```toml
[dependencies]
quick-xml = "0.31"
rand = "0.8"
```

---

## Running

```bash
cargo run --release
```

```bash
RUSTFLAGS="-C target-cpu=native -C embed-bitcode=no" cargo build --release
```

---
