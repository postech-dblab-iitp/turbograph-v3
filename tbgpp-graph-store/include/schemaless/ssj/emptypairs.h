

template <class ProbeRecord, class IndexedRecords, class CandidateSet>
inline void outputempty(const ProbeRecord & proberecord, unsigned int proberecordindex, const IndexedRecords & indexedrecords, CandidateSet & candidateSet, int outputall_le) {
	for(unsigned int i = 0; i < proberecordindex; ++i) {
		if((int)(proberecord.tokens.size() + indexedrecords[i].tokens.size()) <= outputall_le) {
			auto & candidateData = candidateSet.getCandidateData(i);
			candidateSet.addRecord(i);
			candidateData.count = 1;
		} else {
			break;
		}
	}
}
