package io.mosip.packet.core.dto.config;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SyncDataResponseDto {
	private String lastSyncTime;
	private List<SyncDataBaseDto> dataToSync;
}
