#include "analytics/core/TG_DistributedVectorBase.hpp"

std::vector<TG_DistributedVectorBase *> TG_DistributedVectorBase::vectorIDtable;

atom TG_DistributedVectorBase::lock;
node_t* TG_DistributedVectorBase::write_vec_changed = NULL;

node_t** TG_DistributedVectorBase::ggb_pull_idx_per_machine = NULL;
PaddedIdx* TG_DistributedVectorBase::ggb_pull_idx_per_machine_idx;
bool* TG_DistributedVectorBase::ggb_pull_idx_overflow;

TwoLevelBitMap<int64_t> TG_DistributedVectorBase::ggb_msg_received_flags;
TwoLevelBitMap<int64_t> TG_DistributedVectorBase::ggb_reaggregation_flags;
TwoLevelBitMap<int64_t> TG_DistributedVectorBase::ggb_reapply_flags;
TwoLevelBitMap<int64_t> TG_DistributedVectorBase::ggb_pull_message_store_flags;
TwoLevelBitMap<int64_t> TG_DistributedVectorBase::lgb_dirty_flags[2];
TwoLevelBitMap<int64_t>* TG_DistributedVectorBase::ggb_pull_message_store_flags_per_machine;
int64_t         TG_DistributedVectorBase::lgb_toggle = 0;

std::atomic<int64_t> TG_DistributedVectorBase::write_vec_changed_idx;
std::atomic<int64_t> TG_DistributedVectorBase::NumReceivedOV(0L);

std::atomic<int64_t> TG_DistributedVectorBase::SpilledVectorUpdatesInBytes(0);
std::atomic<int64_t> TG_DistributedVectorBase::VectorUpdatesInBytes(0);
std::atomic<int64_t> TG_DistributedVectorBase::NumMessagesBeforeCombiningPerThreadBuffer(0);
std::atomic<int64_t> TG_DistributedVectorBase::NumMessagesAfterCombiningPerThreadBuffer(0);
std::atomic<bool> TG_DistributedVectorBase::update_delta_buffer_overflow;
