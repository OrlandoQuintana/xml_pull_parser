use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::str;
use rand::prelude::*;
use rand::thread_rng;

#[derive(Debug, Clone, Default)]
struct Record {
    id: Option<Vec<i32>>,
    name: Option<Vec<String>>,
    value: Option<Vec<f64>>,
    category: Option<Vec<String>>,
    tags: Option<Vec<String>>,
    meta_created: Option<Vec<String>>,
    meta_updated: Option<Vec<String>>,
    measurements: Option<Vec<f64>>,
}

#[derive(Debug, Clone)]
struct FlatRecord {
    id: Option<i32>,
    name: Option<String>,
    value: Option<f64>,
    category: Option<String>,
    tag: Option<String>,
    meta_created: Option<String>,
    meta_updated: Option<String>,
    measurement: Option<f64>,
}

impl Record {
    fn push<T: ToString>(slot: &mut Option<Vec<T>>, val: T) {
        match slot {
            Some(v) => v.push(val),
            None => *slot = Some(vec![val]),
        }
    }

    fn insert_text(&mut self, path: &[String], text: &str) {
        // strip root "records" if present
        let joined = if path.first().map(|s| s == "records").unwrap_or(false) {
            path[1..].join(".")
        } else {
            path.join(".")
        };

        match joined.as_str() {
            "record.id" => {
                if let Ok(v) = text.trim().parse::<i32>() {
                    Self::push(&mut self.id, v);
                }
            }
            "record.name" => Self::push(&mut self.name, text.trim().to_string()),
            "record.value" => {
                if let Ok(v) = text.trim().parse::<f64>() {
                    Self::push(&mut self.value, v);
                }
            }
            "record.category" => Self::push(&mut self.category, text.trim().to_string()),
            "record.tags.tag" => Self::push(&mut self.tags, text.trim().to_string()),
            "record.meta.created" => Self::push(&mut self.meta_created, text.trim().to_string()),
            "record.meta.updated" => Self::push(&mut self.meta_updated, text.trim().to_string()),
            "record.measurements.experiment.trial.sensor.reading" => {
                if let Ok(v) = text.trim().parse::<f64>() {
                    Self::push(&mut self.measurements, v);
                }
            }
            _ => {}
        }
    }

    fn flatten(&self) -> Vec<FlatRecord> {
        let ids = self.id.as_ref().map(|v| v.clone()).unwrap_or_else(|| vec![i32::default()]);
        let tags = self.tags.as_ref().map(|v| v.clone()).unwrap_or_else(|| vec![String::default()]);
        let measurements = self.measurements.as_ref().map(|v| v.clone()).unwrap_or_else(|| vec![f64::default()]);

        // Scalars remain fixed
        let name = self.name.as_ref().and_then(|v| v.first().cloned());
        let value = self.value.as_ref().and_then(|v| v.first().copied());
        let category = self.category.as_ref().and_then(|v| v.first().cloned());
        let meta_created = self.meta_created.as_ref().and_then(|v| v.first().cloned());
        let meta_updated = self.meta_updated.as_ref().and_then(|v| v.first().cloned());

        let mut rows = Vec::new();

        // Triple nested product of ids × tags × measurements
        for id in &ids {
            for tag in &tags {
                for measurement in &measurements {
                    rows.push(FlatRecord {
                        id: Some(*id),
                        name: name.clone(),
                        value,
                        category: category.clone(),
                        tag: Some(tag.clone()),
                        meta_created: meta_created.clone(),
                        meta_updated: meta_updated.clone(),
                        measurement: Some(*measurement),
                    });
                }
            }
        }

        // Edge case: no lists at all, yield one row
        if rows.is_empty() {
            rows.push(FlatRecord {
                id: None,
                name,
                value,
                category,
                tag: None,
                meta_created,
                meta_updated,
                measurement: None,
            });
        }

        rows
    }
}

fn parse_records(xml: &str) -> Vec<Record> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut path: Vec<String> = Vec::new();
    let mut records = Vec::new();
    let mut current = Record::default();
    let mut inside_record = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = str::from_utf8(e.name().as_ref()).unwrap().to_string();
                path.push(name.clone());
                if name == "record" {
                    inside_record = true;
                    current = Record::default();
                }
            }
            Ok(Event::Text(e)) => {
                if inside_record {
                    let text = e.decode().unwrap().to_string();
                    current.insert_text(&path, &text);
                }
            }
            Ok(Event::End(e)) => {
                let name = str::from_utf8(e.name().as_ref()).unwrap().to_string();
                if name == "record" {
                    records.push(current.clone());
                    inside_record = false;
                }
                path.pop();
            }
            Ok(Event::Eof) => break,
            Err(e) => panic!("XML error: {:?}", e),
            _ => {}
        }
        buf.clear();
    }
    records
}

fn main() {
    let xml = big_xml();
    let records = parse_records(&xml);
    println!("Parsed {} records", records.len());

    for (i, rec) in records.iter().enumerate() { // print first few
        println!("\nRecord {}:\n{:#?}", i, rec);
        for row in rec.flatten().iter() {
            println!("  {:?}", row);
        }
    }
}

/// Generate large XML with varied content and missing fields + nested measurements
fn big_xml() -> String {
    let mut rng = thread_rng();
    let all_tags = [
        "fast", "reliable", "secure", "scalable",
        "resilient", "portable", "robust", "maintainable",
    ];

    let mut xml = String::from("<records>\n");

    for i in 0..10000 {
        let name = if rng.gen_bool(0.9) {
            format!("<name>Alpha{}</name>", i)
        } else {
            String::new()
        };

        let value = if rng.gen_bool(0.85) {
            format!("<value>{:.2}</value>", 40.0 + (i as f64) * 0.1)
        } else {
            String::new()
        };

        let category = if rng.gen_bool(0.8) {
            let cat = match i % 3 {
                0 => "science",
                1 => "engineering",
                _ => "mathematics",
            };
            format!("<category>{}</category>", cat)
        } else {
            String::new()
        };

        // Random 0–5 tags
        let tag_count = rng.gen_range(0..=5);
        let mut tag_block = String::new();
        if tag_count > 0 {
            tag_block.push_str("<tags>\n");
            for _ in 0..tag_count {
                let tag = all_tags.choose(&mut rng).unwrap();
                tag_block.push_str(&format!("    <tag>{}</tag>\n", tag));
            }
            tag_block.push_str("</tags>\n");
        }

        // Deep nested measurements: experiment → trial → sensor → reading
        let mut measurements = String::new();
        if rng.gen_bool(0.7) {
            measurements.push_str("<measurements>\n");
            let exp_count = rng.gen_range(1..=3);
            for _ in 0..exp_count {
                measurements.push_str("  <experiment>\n");
                let trial_count = rng.gen_range(1..=3);
                for _ in 0..trial_count {
                    measurements.push_str("    <trial>\n");
                    let sensor_count = rng.gen_range(1..=2);
                    for _ in 0..sensor_count {
                        measurements.push_str("      <sensor>\n");
                        let reading_count = rng.gen_range(1..=4);
                        for _ in 0..reading_count {
                            let reading: f64 = rng.gen_range(10.0..100.0);
                            measurements.push_str(&format!(
                                "        <reading>{:.2}</reading>\n",
                                reading
                            ));
                        }
                        measurements.push_str("      </sensor>\n");
                    }
                    measurements.push_str("    </trial>\n");
                }
                measurements.push_str("  </experiment>\n");
            }
            measurements.push_str("</measurements>\n");
        }

        // IDs
        let id1 = 1000 + i;
        let id2 = 2000 + i;
        let ids = if rng.gen_bool(0.7) {
            format!("<id>{}</id><id>{}</id>", id1, id2)
        } else {
            format!("<id>{}</id>", id1)
        };

        // Meta
        let created = if rng.gen_bool(0.9) { "<created>2025-10-23</created>" } else { "" };
        let updated = if rng.gen_bool(0.8) { "<updated>2025-10-24</updated>" } else { "" };
        let meta = if !created.is_empty() || !updated.is_empty() {
            format!("<meta>{}{}</meta>", created, updated)
        } else {
            String::new()
        };

        xml.push_str(&format!(
            r#"
        <record>
            {}
            {}
            {}
            {}
            {}
            {}
            {}
        </record>
        "#,
            ids, name, value, category, tag_block, measurements, meta
        ));
    }

    xml.push_str("</records>");
    xml
}
