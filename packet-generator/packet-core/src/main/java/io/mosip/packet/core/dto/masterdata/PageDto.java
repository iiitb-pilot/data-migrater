package io.mosip.packet.core.dto.masterdata;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PageDto<T> {
	private int pageNo;
	private int totalPages;
	private long totalItems;
	private List<T> data;
}
