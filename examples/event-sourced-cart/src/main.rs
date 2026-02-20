use merkql::broker::{Broker, BrokerConfig};
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum CartEvent {
    AddItem {
        cart_id: String,
        item: String,
        qty: u32,
        price: f64,
    },
    RemoveItem {
        cart_id: String,
        item: String,
    },
    Checkout {
        cart_id: String,
    },
}

impl CartEvent {
    fn cart_id(&self) -> &str {
        match self {
            CartEvent::AddItem { cart_id, .. } => cart_id,
            CartEvent::RemoveItem { cart_id, .. } => cart_id,
            CartEvent::Checkout { cart_id, .. } => cart_id,
        }
    }
}

#[derive(Debug, Clone)]
struct LineItem {
    item: String,
    qty: u32,
    price: f64,
}

fn main() {
    let dir = tempfile::tempdir().unwrap();

    println!("=== MerkQL Event-Sourced Cart Example ===\n");

    // Step 1: Open broker
    println!("--- Step 1: Open broker ---");
    let broker = Broker::open(BrokerConfig::new(dir.path())).unwrap();
    println!("Broker opened.\n");

    // Step 2-3: Produce 30 events across 3 carts
    println!("--- Step 2-3: Produce 30 cart events ---");
    let producer = Broker::producer(&broker);

    let events: Vec<CartEvent> = vec![
        // Cart 1: build up and checkout
        CartEvent::AddItem {
            cart_id: "cart-1".into(),
            item: "Widget".into(),
            qty: 2,
            price: 9.99,
        },
        CartEvent::AddItem {
            cart_id: "cart-1".into(),
            item: "Gadget".into(),
            qty: 1,
            price: 24.99,
        },
        CartEvent::AddItem {
            cart_id: "cart-1".into(),
            item: "Doohickey".into(),
            qty: 3,
            price: 4.50,
        },
        CartEvent::RemoveItem {
            cart_id: "cart-1".into(),
            item: "Doohickey".into(),
        },
        CartEvent::AddItem {
            cart_id: "cart-1".into(),
            item: "Thingamajig".into(),
            qty: 1,
            price: 15.00,
        },
        CartEvent::Checkout {
            cart_id: "cart-1".into(),
        },
        // Cart 2: build up and checkout
        CartEvent::AddItem {
            cart_id: "cart-2".into(),
            item: "Sprocket".into(),
            qty: 5,
            price: 3.25,
        },
        CartEvent::AddItem {
            cart_id: "cart-2".into(),
            item: "Cog".into(),
            qty: 10,
            price: 1.50,
        },
        CartEvent::AddItem {
            cart_id: "cart-2".into(),
            item: "Gear".into(),
            qty: 2,
            price: 12.00,
        },
        CartEvent::RemoveItem {
            cart_id: "cart-2".into(),
            item: "Cog".into(),
        },
        CartEvent::AddItem {
            cart_id: "cart-2".into(),
            item: "Bearing".into(),
            qty: 4,
            price: 6.75,
        },
        CartEvent::Checkout {
            cart_id: "cart-2".into(),
        },
        // Cart 3: build up, no checkout yet
        CartEvent::AddItem {
            cart_id: "cart-3".into(),
            item: "Bolt".into(),
            qty: 100,
            price: 0.10,
        },
        CartEvent::AddItem {
            cart_id: "cart-3".into(),
            item: "Nut".into(),
            qty: 100,
            price: 0.08,
        },
        CartEvent::AddItem {
            cart_id: "cart-3".into(),
            item: "Washer".into(),
            qty: 50,
            price: 0.05,
        },
        // More events to reach 30
        CartEvent::AddItem {
            cart_id: "cart-1".into(),
            item: "Gizmo".into(),
            qty: 2,
            price: 7.50,
        },
        CartEvent::AddItem {
            cart_id: "cart-2".into(),
            item: "Flange".into(),
            qty: 1,
            price: 18.00,
        },
        CartEvent::AddItem {
            cart_id: "cart-3".into(),
            item: "Rivet".into(),
            qty: 200,
            price: 0.03,
        },
        CartEvent::RemoveItem {
            cart_id: "cart-3".into(),
            item: "Washer".into(),
        },
        CartEvent::AddItem {
            cart_id: "cart-1".into(),
            item: "Doodad".into(),
            qty: 3,
            price: 5.99,
        },
        CartEvent::AddItem {
            cart_id: "cart-2".into(),
            item: "Bracket".into(),
            qty: 2,
            price: 8.50,
        },
        CartEvent::AddItem {
            cart_id: "cart-3".into(),
            item: "Pin".into(),
            qty: 50,
            price: 0.15,
        },
        CartEvent::AddItem {
            cart_id: "cart-1".into(),
            item: "Widget".into(),
            qty: 1,
            price: 9.99,
        },
        CartEvent::RemoveItem {
            cart_id: "cart-2".into(),
            item: "Flange".into(),
        },
        CartEvent::AddItem {
            cart_id: "cart-2".into(),
            item: "Axle".into(),
            qty: 1,
            price: 35.00,
        },
        CartEvent::AddItem {
            cart_id: "cart-3".into(),
            item: "Screw".into(),
            qty: 150,
            price: 0.04,
        },
        CartEvent::AddItem {
            cart_id: "cart-1".into(),
            item: "Spring".into(),
            qty: 4,
            price: 2.25,
        },
        CartEvent::AddItem {
            cart_id: "cart-2".into(),
            item: "Pulley".into(),
            qty: 2,
            price: 11.00,
        },
        CartEvent::AddItem {
            cart_id: "cart-3".into(),
            item: "Anchor".into(),
            qty: 10,
            price: 1.20,
        },
        CartEvent::AddItem {
            cart_id: "cart-1".into(),
            item: "Lever".into(),
            qty: 1,
            price: 13.50,
        },
    ];

    for event in &events {
        let value = serde_json::to_string(event).unwrap();
        producer
            .send(&ProducerRecord::new(
                "cart-events",
                Some(event.cart_id().to_string()),
                value,
            ))
            .unwrap();
    }
    println!("Produced {} cart events.\n", events.len());

    // Step 4: Projection 1 — Cart state
    println!("--- Step 4: Projection 1 — Cart state ---");
    let mut cart_state: HashMap<String, Vec<LineItem>> = HashMap::new();
    {
        let mut consumer = Broker::consumer(
            &broker,
            ConsumerConfig {
                group_id: "cart-state".into(),
                auto_commit: false,
                offset_reset: OffsetReset::Earliest,
            },
        );
        consumer.subscribe(&["cart-events"]).unwrap();
        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        println!("  Polled {} records.", records.len());

        for record in &records {
            let event: CartEvent = serde_json::from_str(&record.value).unwrap();
            apply_cart_event(&mut cart_state, &event);
        }

        print_cart_state(&cart_state);
        consumer.commit_sync().unwrap();
        consumer.close().unwrap();
    }
    println!();

    // Step 5: Projection 2 — Revenue report
    println!("--- Step 5: Projection 2 — Revenue report ---");
    let mut checked_out: HashMap<String, bool> = HashMap::new();
    let mut revenue_items: HashMap<String, Vec<LineItem>> = HashMap::new();
    {
        let mut consumer = Broker::consumer(
            &broker,
            ConsumerConfig {
                group_id: "revenue".into(),
                auto_commit: false,
                offset_reset: OffsetReset::Earliest,
            },
        );
        consumer.subscribe(&["cart-events"]).unwrap();
        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        println!("  Polled {} records.", records.len());

        for record in &records {
            let event: CartEvent = serde_json::from_str(&record.value).unwrap();
            match &event {
                CartEvent::Checkout { cart_id } => {
                    checked_out.insert(cart_id.clone(), true);
                }
                _ => {
                    apply_cart_event(&mut revenue_items, &event);
                }
            }
        }

        print_revenue(&revenue_items, &checked_out);
        consumer.commit_sync().unwrap();
        consumer.close().unwrap();
    }
    println!();

    // Step 6: Incremental update — 5 more events (cart-3 adds + checkout)
    println!("--- Step 6: Produce 5 more events (cart-3 completion) ---");
    let new_events: Vec<CartEvent> = vec![
        CartEvent::AddItem {
            cart_id: "cart-3".into(),
            item: "Clamp".into(),
            qty: 5,
            price: 2.50,
        },
        CartEvent::AddItem {
            cart_id: "cart-3".into(),
            item: "Hinge".into(),
            qty: 8,
            price: 1.75,
        },
        CartEvent::RemoveItem {
            cart_id: "cart-3".into(),
            item: "Anchor".into(),
        },
        CartEvent::AddItem {
            cart_id: "cart-3".into(),
            item: "Latch".into(),
            qty: 3,
            price: 4.00,
        },
        CartEvent::Checkout {
            cart_id: "cart-3".into(),
        },
    ];

    for event in &new_events {
        let value = serde_json::to_string(event).unwrap();
        producer
            .send(&ProducerRecord::new(
                "cart-events",
                Some(event.cart_id().to_string()),
                value,
            ))
            .unwrap();
    }
    println!("Produced {} more events.\n", new_events.len());

    // Step 7: Both consumers poll again — each gets exactly the 5 new events
    println!("--- Step 7: Incremental updates ---");

    // Cart state projection
    println!("  Cart state consumer:");
    {
        let mut consumer = Broker::consumer(
            &broker,
            ConsumerConfig {
                group_id: "cart-state".into(),
                auto_commit: false,
                offset_reset: OffsetReset::Earliest,
            },
        );
        consumer.subscribe(&["cart-events"]).unwrap();
        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        println!(
            "    Polled {} new records (expected {}).",
            records.len(),
            new_events.len()
        );

        for record in &records {
            let event: CartEvent = serde_json::from_str(&record.value).unwrap();
            apply_cart_event(&mut cart_state, &event);
        }

        consumer.commit_sync().unwrap();
        consumer.close().unwrap();
    }

    // Revenue projection
    println!("  Revenue consumer:");
    {
        let mut consumer = Broker::consumer(
            &broker,
            ConsumerConfig {
                group_id: "revenue".into(),
                auto_commit: false,
                offset_reset: OffsetReset::Earliest,
            },
        );
        consumer.subscribe(&["cart-events"]).unwrap();
        let records = consumer.poll(Duration::from_millis(100)).unwrap();
        println!(
            "    Polled {} new records (expected {}).",
            records.len(),
            new_events.len()
        );

        for record in &records {
            let event: CartEvent = serde_json::from_str(&record.value).unwrap();
            match &event {
                CartEvent::Checkout { cart_id } => {
                    checked_out.insert(cart_id.clone(), true);
                }
                _ => {
                    apply_cart_event(&mut revenue_items, &event);
                }
            }
        }

        consumer.commit_sync().unwrap();
        consumer.close().unwrap();
    }
    println!();

    // Step 8: Print updated projections
    println!("--- Step 8: Updated projections ---");
    println!("  Updated cart state:");
    print_cart_state(&cart_state);
    println!();
    println!("  Updated revenue:");
    print_revenue(&revenue_items, &checked_out);

    println!("\n=== Event-Sourced Cart Example Complete ===");
}

fn apply_cart_event(carts: &mut HashMap<String, Vec<LineItem>>, event: &CartEvent) {
    match event {
        CartEvent::AddItem {
            cart_id,
            item,
            qty,
            price,
        } => {
            let items = carts.entry(cart_id.clone()).or_default();
            if let Some(existing) = items.iter_mut().find(|li| li.item == *item) {
                existing.qty += qty;
            } else {
                items.push(LineItem {
                    item: item.clone(),
                    qty: *qty,
                    price: *price,
                });
            }
        }
        CartEvent::RemoveItem { cart_id, item } => {
            if let Some(items) = carts.get_mut(cart_id) {
                items.retain(|li| li.item != *item);
            }
        }
        CartEvent::Checkout { .. } => {
            // Cart state projection: checkout doesn't change the items
        }
    }
}

fn print_cart_state(carts: &HashMap<String, Vec<LineItem>>) {
    let mut cart_ids: Vec<_> = carts.keys().cloned().collect();
    cart_ids.sort();
    for cart_id in &cart_ids {
        let items = &carts[cart_id];
        let total: f64 = items.iter().map(|li| li.price * li.qty as f64).sum();
        println!("  {}: {} items, total ${:.2}", cart_id, items.len(), total);
        for li in items {
            println!(
                "    - {} x{} @ ${:.2} = ${:.2}",
                li.item,
                li.qty,
                li.price,
                li.price * li.qty as f64
            );
        }
    }
}

fn print_revenue(carts: &HashMap<String, Vec<LineItem>>, checked_out: &HashMap<String, bool>) {
    let mut total_revenue = 0.0;
    let mut cart_ids: Vec<_> = checked_out.keys().cloned().collect();
    cart_ids.sort();
    for cart_id in &cart_ids {
        if let Some(items) = carts.get(cart_id) {
            let cart_total: f64 = items.iter().map(|li| li.price * li.qty as f64).sum();
            println!("  {} (checked out): ${:.2}", cart_id, cart_total);
            total_revenue += cart_total;
        }
    }
    println!("  Total revenue: ${:.2}", total_revenue);
}
