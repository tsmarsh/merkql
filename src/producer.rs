use crate::broker::BrokerRef;
use crate::record::{ProducerRecord, Record};
use anyhow::Result;
use chrono::Utc;

/// Kafka-compatible producer. Routes records through the broker's topics.
pub struct Producer {
    broker: BrokerRef,
}

impl Producer {
    pub(crate) fn new(broker: BrokerRef) -> Self {
        Producer { broker }
    }

    /// Send a record. Auto-creates the topic if the broker is configured for it.
    ///
    /// # Examples
    ///
    /// ```
    /// use merkql::broker::{Broker, BrokerConfig};
    /// use merkql::record::ProducerRecord;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let broker = Broker::open(BrokerConfig::new(dir.path())).unwrap();
    /// let producer = Broker::producer(&broker);
    /// let record = producer.send(&ProducerRecord::new("events", None, "hello")).unwrap();
    /// assert_eq!(record.value, "hello");
    /// ```
    pub fn send(&self, producer_record: &ProducerRecord) -> Result<Record> {
        // Ensure topic exists (auto-create if configured)
        self.broker.ensure_topic(&producer_record.topic)?;

        let mut record = Record {
            key: producer_record.key.clone(),
            value: producer_record.value.clone(),
            topic: producer_record.topic.clone(),
            partition: 0,
            offset: 0,
            timestamp: Utc::now(),
        };

        let topic = self
            .broker
            .topic(&producer_record.topic)
            .ok_or_else(|| anyhow::anyhow!("topic not found: {}", producer_record.topic))?;

        topic.append(&mut record)?;
        Ok(record)
    }

    /// Send a batch of records to the same topic.
    /// Returns all records with their assigned offsets and partitions.
    ///
    /// # Examples
    ///
    /// ```
    /// use merkql::broker::{Broker, BrokerConfig};
    /// use merkql::record::ProducerRecord;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let broker = Broker::open(BrokerConfig::new(dir.path())).unwrap();
    /// let producer = Broker::producer(&broker);
    /// let batch: Vec<ProducerRecord> = (0..3)
    ///     .map(|i| ProducerRecord::new("events", None, format!("msg-{}", i)))
    ///     .collect();
    /// let results = producer.send_batch(&batch).unwrap();
    /// assert_eq!(results.len(), 3);
    /// ```
    pub fn send_batch(&self, producer_records: &[ProducerRecord]) -> Result<Vec<Record>> {
        if producer_records.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(producer_records.len());

        for pr in producer_records {
            let record = self.send(pr)?;
            results.push(record);
        }

        Ok(results)
    }
}
