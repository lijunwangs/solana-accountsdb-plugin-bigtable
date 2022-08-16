use {
    crate::parallel_bigtable_client::BufferedBigtableClient, log::*, prost::Message,
    solana_bigtable_geyser_models::models::slots,
    solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError,
    solana_geyser_plugin_interface::geyser_plugin_interface::SlotStatus, std::time::Duration,
};

pub struct UpdateSlotRequest {
    pub slot: u64,
    pub parent: Option<u64>,
    pub slot_status: SlotStatus,
    pub updated_since_epoch: Duration,
}

impl BufferedBigtableClient {
    /// Update or insert a single account
    pub async fn update_slot(
        &mut self,
        request: UpdateSlotRequest,
    ) -> Result<(usize, usize), GeyserPluginError> {
        let slot_cells = vec![(
            request.slot.to_string(),
            slots::Slot {
                slot: request.slot,
                parent: request.parent,
                status: request.slot_status.as_str().to_string(),
                updated_on: Some(slots::UnixTimestamp {
                    timestamp: request.updated_since_epoch.as_millis() as i64,
                }),
            },
        )];
        let raw_size = slot_cells.iter().map(|(_, m)| m.encoded_len()).sum();

        let client = self.client.lock().unwrap();
        let result = client
            .client
            .put_protobuf_cells_with_retry::<slots::Slot>("slot", &slot_cells, true)
            .await;
        match result {
            Ok(written_size) => Ok((written_size, raw_size)),
            Err(err) => {
                error!("Error persisting into the database: {}", err);
                Err(GeyserPluginError::Custom(Box::new(err)))
            }
        }
    }
}
