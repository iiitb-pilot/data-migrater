package io.mosip.packet.core.repository;

import java.util.List;

import io.mosip.packet.core.entity.Location;

import io.mosip.kernel.core.dataaccess.spi.repository.BaseRepository;

/**
 * Location repository
 * 
 * @author Brahmananda Reddy
 *
 */

public interface LocationRepository extends BaseRepository<Location, String> {
	/**
	 * Find master location by hierarchy name and language code.
	 *
	 * @param hierarchyName the hierarchy name
	 * @param langCode      the lang code
	 * @return the list
	 */
	List<Location> findByIsActiveTrueAndHierarchyNameAndLangCode(String hierarchyName, String langCode);

	/**
	 * Find master location by hierarchy name and language code.
	 *
	 * @param hierarchyName the hierarchy name
	 * @param langCode      the lang code
	 * @return the list
	 */
	List<Location> findByIsActiveTrueAndHierarchyLevelAndLangCode(int hierarchyLevel, String langCode);

	/**
	 * Find master location by parent loc code.
	 *
	 * @param parentLocCode the parent loc code
	 * @param langCode      the lang code
	 * @return the list
	 */
	List<Location> findByIsActiveTrueAndParentLocCodeAndLangCode(String parentLocCode, String langCode);

	List<Location> findAllByIsActiveTrue();

	/**
	 * Find master location by language code.
	 *
	 * @param langCode the lang code
	 * @return the list
	 */
	List<Location> findByIsActiveTrueAndLangCode(String langCode);

	/**
	 * @param code code 
	 * @param langCode language code
	 * @return Location
	 */
	Location findByCodeAndLangCode(String code, String langCode);

}
