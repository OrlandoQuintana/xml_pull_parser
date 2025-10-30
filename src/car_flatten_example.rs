#[derive(Debug, Clone, Default)]
struct Exterior {
    paint_id: Option<u64>,
    insured: Option<bool>
}
impl Exterior {
    fn flatten(&self) -> Vec<FlatRecord> {
        vec![FlatRecord {
            paint_id: self.paint_id,
            insured: self.insured,
            ..Default::default()
        }]
    }
}

#[derive(Debug, Clone, Default)]
struct Interior {
    interior_color: Option<String>,
    seat_material: Option<String>,
    heated_seats: Option<bool>
}
impl Interior {
    fn flatten(&self) -> Vec<FlatRecord> {
        vec![FlatRecord {
            interior_color: self.interior_color.clone(),
            seat_material: self.seat_material.clone(),
            heated_seats: self.heated_seats,
            ..Default::default()
        }]
    }
}

#[derive(Debug, Clone, Default)]
struct Engine {
    horsepower: Option<u64>,
    mpg: Option<u64>,
    engine_price: Option<f64>
}
impl Engine {
    fn flatten(&self) -> Vec<FlatRecord> {
        vec![FlatRecord {
            horsepower: self.horsepower,
            mpg: self.mpg,
            engine_price: self.engine_price,
            ..Default::default()
        }]
    }
}

#[derive(Debug, Clone, Default)]
struct Car {
    color: Option<String>,
    top_speed: Option<u64>,
    price: Option<f64>,
    engine: Option<Vec<Engine>>,
    interior: Option<Vec<Interior>>,
    exterior: Option<Vec<Exterior>>
}
impl Car {
    fn flatten(&self) -> Vec<FlatRecord> {
        // Get lists of children or empty Vecs if none exist
        let engines = self.engine.clone().unwrap_or_default();
        let interiors = self.interior.clone().unwrap_or_default();
        let exteriors = self.exterior.clone().unwrap_or_default();

        // If any list is empty, insert one default FlatRecord so we still produce rows
        let flat_engines: Vec<FlatRecord> = if engines.is_empty() {
            vec![FlatRecord::default()]
        } else {
            engines.into_iter().flat_map(|e| e.flatten()).collect()
        };
        let flat_interiors: Vec<FlatRecord> = if interiors.is_empty() {
            vec![FlatRecord::default()]
        } else {
            interiors.into_iter().flat_map(|i| i.flatten()).collect()
        };
        let flat_exteriors: Vec<FlatRecord> = if exteriors.is_empty() {
            vec![FlatRecord::default()]
        } else {
            exteriors.into_iter().flat_map(|x| x.flatten()).collect()
        };

        // Cartesian combination of engine × interior × exterior
        let mut out = Vec::new();
        for engine in &flat_engines {
            for interior in &flat_interiors {
                for exterior in &flat_exteriors {
                    let mut row = FlatRecord::default();

                    // Fill Car’s own scalar fields
                    row.color = self.color.clone();
                    row.top_speed = self.top_speed;
                    row.price = self.price;

                    // Merge flattened Engine
                    row.horsepower = engine.horsepower;
                    row.mpg = engine.mpg;
                    row.engine_price = engine.engine_price;

                    // Merge flattened Interior
                    row.interior_color = interior.interior_color.clone();
                    row.seat_material = interior.seat_material.clone();
                    row.heated_seats = interior.heated_seats;

                    // Merge flattened Exterior
                    row.paint_id = exterior.paint_id;
                    row.insured = exterior.insured;

                    out.push(row);
                }
            }
        }

        // Always emit at least one row
        if out.is_empty() {
            out.push(FlatRecord::default());
        }
        out
    }
}