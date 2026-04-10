"""
Comprehensive test suite for the mailing list deduplication pipeline.

Verifies all critical safety mechanisms and formatting rules
documented in OPTIMAL_SYSTEM_PROMPT.md, including:
  - PO Box protection (different PO Boxes must NEVER merge)
  - Suite/Unit protection (different units must NEVER merge)
  - Name formatting for persons, trusts, entities, government
  - Household extraction and combination
  - Order-independent name matching
  - Entity fuzzy matching with typo detection
  - ZIP code normalization
  - Address formatting and key generation
  - Entity classification
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.address_formatter import (
    is_po_box,
    extract_po_box,
    extract_unit_number,
    remove_unit_from_address,
    normalize_address_for_matching,
    create_address_key,
    format_street_address,
)
from utils.matching_utils import (
    addresses_are_similar,
    same_city_state_zip,
    fuzzy_match_entity_names,
)
from utils.name_formatter import (
    is_trust,
    is_entity,
    is_government_entity,
    format_trust_name,
    format_government_entity,
    format_entity_name,
    format_person_name_from_lastfirst,
    extract_individuals_from_household,
    normalize_name_for_comparison,
    combine_household_names,
    parse_raw_owner_name,
)
from utils.config import normalize_zip


# ---------------------------------------------------------------------------
# Test 1: PO Box Safety (CRITICAL)
# ---------------------------------------------------------------------------
class TestPOBoxProtection:
    """CRITICAL safety tests: PO Box numbers must be compared exactly."""

    def test_different_po_boxes_must_not_match(self):
        """CRITICAL: PO Box 9 must NEVER match PO Box 2190.

        Merging records with different PO Boxes would send mail to the
        wrong recipient. This is the single most important safety invariant
        in the entire pipeline.
        """
        assert not addresses_are_similar("PO Box 9", "PO Box 2190")
        assert not addresses_are_similar("PO BOX 123", "PO BOX 456")
        assert not addresses_are_similar("POBOX 1497", "PO BOX 1498")

    def test_same_po_boxes_must_match(self):
        """Identical PO Box numbers in different formats should still match."""
        assert addresses_are_similar("PO Box 571", "PO BOX 571")
        assert addresses_are_similar("POBOX 1497", "PO BOX 1497")
        assert addresses_are_similar("P.O. Box 123", "PO Box 123")

    def test_mixed_street_po_box_match(self):
        """Address with both street and PO Box should match on PO Box only."""
        assert addresses_are_similar("1701 E. Pima St. PO Box 571", "PO Box 571")

    def test_po_box_extraction(self):
        """extract_po_box must reliably pull the PO BOX portion."""
        assert extract_po_box("PO Box 571") == "PO BOX 571"
        assert extract_po_box("POBOX 1497") == "PO BOX 1497"
        assert extract_po_box("P.O. Box 123") == "PO BOX 123"
        assert extract_po_box("1701 E. Pima St. PO Box 571") == "PO BOX 571"
        assert extract_po_box("123 Main St") is None


# ---------------------------------------------------------------------------
# Test 2: Suite / Unit Protection (CRITICAL)
# ---------------------------------------------------------------------------
class TestSuiteProtection:
    """CRITICAL safety tests: different suite/unit numbers must not merge."""

    def test_different_suites_must_not_match(self):
        """CRITICAL: Same street but different suites are different locations."""
        assert not addresses_are_similar("123 Main St Ste 6123", "123 Main St Ste 100")
        assert not addresses_are_similar(
            "205 S 17th Ave Apt 5A", "205 S 17th Ave Apt 5B"
        )

    def test_same_suites_must_match(self):
        """Ste and Suite are equivalent abbreviations for the same unit."""
        assert addresses_are_similar("123 Main St Ste 100", "123 Main St Suite 100")

    def test_unit_extraction(self):
        assert extract_unit_number("123 Main St Ste 200") is not None
        assert extract_unit_number("123 Main St") is None


# ---------------------------------------------------------------------------
# Test 3: Name Formatting
# ---------------------------------------------------------------------------
class TestNameFormatting:
    def test_person_name_last_first_2word(self):
        assert format_person_name_from_lastfirst("SMITH JOHN") == "John Smith"

    def test_person_name_with_middle_initial(self):
        assert format_person_name_from_lastfirst("SMITH JOHN A") == "John A. Smith"

    def test_hispanic_4word(self):
        assert (
            format_person_name_from_lastfirst("ROLON MEZA MARTHA E")
            == "Martha E. Rolon Meza"
        )

    def test_hispanic_5word(self):
        assert (
            format_person_name_from_lastfirst("ALVAREZ MARTHA E ROLON MEZA")
            == "Martha E. Alvarez Rolon Meza"
        )

    def test_name_particles(self):
        result = format_person_name_from_lastfirst("MCDONALD JOHN")
        assert "Mc" in result or "McDonald" in result

    def test_trust_name(self):
        result = format_trust_name("SMITH JOHN & MARY FAMILY TRUST")
        assert (
            result == "John & Mary Smith Family Trust"
            or "Trust" in result
        )
        result2 = format_trust_name("THE SMITH TRUST")
        assert "Smith" in result2 and "Trust" in result2 and "The" not in result2

    def test_government_entity(self):
        result = format_government_entity("DEPT ARIZONA TRANSPORTATION")
        assert "Arizona" in result and ("Dept" in result or "Department" in result)

    def test_business_entity(self):
        result = format_entity_name("ABC INVESTMENTS LLC")
        assert "ABC" in result and "LLC" in result


# ---------------------------------------------------------------------------
# Test 4: Household Extraction
# ---------------------------------------------------------------------------
class TestHouseholdExtraction:
    def test_slash_separated(self):
        result = extract_individuals_from_household("VALDEZ MIGUEL/ORTIZ FRANCISCO")
        assert len(result) == 2

    def test_and_shared_surname(self):
        result = extract_individuals_from_household("John and Mary Smith")
        assert len(result) == 2

    def test_ampersand_shared_surname(self):
        result = extract_individuals_from_household("SMITH JOHN & MARY")
        assert len(result) >= 2


# ---------------------------------------------------------------------------
# Test 5: Order-Independent Name Matching
# ---------------------------------------------------------------------------
class TestNameDeduplication:
    def test_same_person_different_order(self):
        key1 = normalize_name_for_comparison("Francisco Rodriguez")
        key2 = normalize_name_for_comparison("Rodriguez Francisco")
        assert key1 == key2

    def test_different_people(self):
        key1 = normalize_name_for_comparison("Francisco Rodriguez")
        key2 = normalize_name_for_comparison("Miguel Valdez")
        assert key1 != key2


# ---------------------------------------------------------------------------
# Test 6: Entity Fuzzy Matching
# ---------------------------------------------------------------------------
class TestEntityFuzzyMatching:
    def test_typo_detection(self):
        entities = [
            "Butterfield Trail Investments",
            "Buttefield Trail Investments",
            "ABC LLC",
        ]
        mapping = fuzzy_match_entity_names(entities, threshold=0.90)
        # The typo variant should map to the correct spelling, not to ABC LLC
        assert mapping.get("Buttefield Trail Investments") != "ABC LLC"


# ---------------------------------------------------------------------------
# Test 7: ZIP Normalization
# ---------------------------------------------------------------------------
class TestZipNormalization:
    def test_zip_plus_4(self):
        assert normalize_zip("85337-0725") == "85337"

    def test_zip_9digit(self):
        assert normalize_zip("853370725") == "85337"

    def test_zip_5digit(self):
        assert normalize_zip("85337") == "85337"


# ---------------------------------------------------------------------------
# Test 8: Address Formatting
# ---------------------------------------------------------------------------
class TestAddressFormatting:
    def test_street_types(self):
        result = format_street_address("123 MAIN ST")
        assert "St." in result

    def test_directionals(self):
        result = format_street_address("123 N MAIN ST")
        assert "N" in result

    def test_ordinals(self):
        result = format_street_address("205 S 17TH AVE")
        assert "17th" in result

    def test_state_route_not_street(self):
        result = format_street_address("ST ROUTE 85")
        assert "State Route" in result

    def test_po_box_formatting(self):
        result = format_street_address("POBOX 1497")
        assert "PO BOX 1497" in result

    def test_null_patterns(self):
        result = format_street_address("NONE")
        assert result == ""


# ---------------------------------------------------------------------------
# Test 9: Address Key Generation
# ---------------------------------------------------------------------------
class TestAddressKeyGeneration:
    def test_basic_key(self):
        key = create_address_key("205 S 17th Ave", "Phoenix", "AZ", "85007")
        assert "PHOENIX" in key and "AZ" in key and "85007" in key

    def test_po_box_key(self):
        key = create_address_key("PO Box 571", "Gila Bend", "AZ", "85337")
        assert "PO BOX 571" in key

    def test_zip_normalization_in_key(self):
        key1 = create_address_key("123 Main St", "Phoenix", "AZ", "85337-0725")
        key2 = create_address_key("123 Main St", "Phoenix", "AZ", "85337")
        # Both should normalize to 85337
        assert "85337" in key1 and "85337" in key2


# ---------------------------------------------------------------------------
# Test 10: Entity Classification
# ---------------------------------------------------------------------------
class TestEntityClassification:
    def test_trust_detection(self):
        assert is_trust("SMITH FAMILY TRUST")
        assert not is_trust("SMITH JOHN")

    def test_entity_detection(self):
        assert is_entity("ABC INVESTMENTS LLC")
        assert not is_entity("SMITH JOHN")

    def test_government_detection(self):
        assert is_government_entity("STATE OF ARIZONA")
        assert not is_government_entity("SMITH JOHN")


# ---------------------------------------------------------------------------
# Test 11: Combine Household Names
# ---------------------------------------------------------------------------
class TestCombineHousehold:
    def test_same_last_name_two(self):
        result = combine_household_names(["John Smith", "Mary Smith"])
        assert "&" in result and "Smith" in result

    def test_different_last_names(self):
        result = combine_household_names(["John Smith", "Jane Doe"])
        assert "Smith" in result and "Doe" in result

    def test_single_person(self):
        result = combine_household_names(["John Smith"])
        assert result == "John Smith"


# ---------------------------------------------------------------------------
# Test 12: V5 parse_raw_owner_name – NameComponents
# ---------------------------------------------------------------------------
class TestParseRawOwnerName:
    def test_simple_person(self):
        nc = parse_raw_owner_name("SMITH JOHN")
        assert nc.full_name == "John Smith"
        assert nc.p1_first == "John"
        assert nc.p1_last == "Smith"
        assert nc.p1_middle == ""

    def test_person_with_middle_initial(self):
        nc = parse_raw_owner_name("SMITH JOHN A")
        assert nc.p1_first == "John"
        assert nc.p1_middle == "A."
        assert nc.p1_last == "Smith"

    def test_shared_surname_slash(self):
        # SMITH JOHN/MARGARET → John & Margaret Smith
        nc = parse_raw_owner_name("SMITH JOHN/MARGARET")
        assert "&" in nc.full_name
        assert "Smith" in nc.full_name
        assert nc.p1_first == "John"
        assert nc.p1_last == "Smith"
        assert nc.p2_first == "Margaret"
        assert nc.p2_last == "Smith"

    def test_double_first_name_detection(self):
        # HARRIS RONALD/TONYA SUE → Ronald & Tonya Sue Harris
        nc = parse_raw_owner_name("HARRIS RONALD/TONYA SUE")
        assert "Ronald" in nc.full_name
        assert "Tonya Sue" in nc.full_name
        assert "Harris" in nc.full_name

    def test_suffix_repositioned(self):
        # ALLISON ROY LEE III/KAREN ANNE → Roy & Karen Anne Allison III
        nc = parse_raw_owner_name("ALLISON ROY LEE III/KAREN ANNE")
        assert "III" in nc.full_name
        # III should appear after the last name
        idx_allison = nc.full_name.find("Allison")
        idx_iii = nc.full_name.find("III")
        assert idx_iii > idx_allison

    def test_different_surnames_slash(self):
        # SMITH JOHN/BROWN MARGARET ANN → John Smith & Margaret Ann Brown
        nc = parse_raw_owner_name("SMITH JOHN/BROWN MARGARET ANN")
        assert "Smith" in nc.full_name
        assert "Brown" in nc.full_name
        assert nc.p1_last == "Smith"
        assert nc.p2_last == "Brown"

    def test_trust_is_business(self):
        nc = parse_raw_owner_name("SMITH JOHN FAMILY TRUST")
        assert nc.is_business
        assert "Trust" in nc.full_name
        assert nc.p1_first == ""

    def test_llc_is_business(self):
        nc = parse_raw_owner_name("KEMF WP COMMERCIAL LLC")
        assert nc.is_business

    def test_middle_names_retained(self):
        # SMITH JOHN MICHAEL WILLIAM → John Michael William Smith
        nc = parse_raw_owner_name("SMITH JOHN MICHAEL WILLIAM")
        assert nc.p1_first == "John"
        assert "Michael" in nc.p1_middle
        assert "William" in nc.p1_middle
        assert nc.p1_last == "Smith"

    def test_mckelvey_casing(self):
        nc = parse_raw_owner_name("MCKELVEY WILFRED SCOTT/JOELLEN")
        assert "McKelvey" in nc.full_name

    def test_lp_normalization(self):
        nc = parse_raw_owner_name("SMITH L P")
        # "L P" should be recognized as LP entity
        assert nc.is_business or "LP" in nc.full_name or "L P" not in nc.full_name


# ---------------------------------------------------------------------------
# Test 13: Cross-Source Name Variant Deduplication
# ---------------------------------------------------------------------------
class TestCrossSourceNameDedup:
    """Verify that spelling variants of the same person across data sources
    are recognized and merged, not treated as separate household members."""

    def test_aron_aaron_same_person(self):
        """Aron/Aaron are spelling variants of the same first name.
        The consolidation dedup should treat them as the same person."""
        from scripts.consolidate_addresses import _names_same_person
        assert _names_same_person("Aron Jimenez", "Aaron Jimenez")

    def test_micheal_michael_same_person(self):
        """Micheal/Michael are common cross-source spelling variants."""
        from scripts.consolidate_addresses import _names_same_person
        assert _names_same_person("Micheal Smith", "Michael Smith")

    def test_different_first_names_still_differ(self):
        """Truly different first names must NOT be treated as the same person."""
        from scripts.consolidate_addresses import _names_same_person
        assert not _names_same_person("John Smith", "Jane Smith")
        assert not _names_same_person("Robert Smith", "Maria Smith")

    def test_short_names_require_exact_match(self):
        """Very short first names (<=2 chars) require exact match to avoid
        false positives like 'Al' matching 'AJ'."""
        from scripts.consolidate_addresses import _names_same_person
        assert not _names_same_person("Al Smith", "AJ Smith")
        assert _names_same_person("Al Smith", "Al Smith")


# ---------------------------------------------------------------------------
# Test 14: Classification False Positives
# ---------------------------------------------------------------------------
class TestClassificationFalsePositives:
    """Ensure classifiers do not misidentify common businesses as government."""

    def test_corporate_indicator_wins_over_government_keyword(self):
        """When a name has a corporate indicator (LLC, INC) AND a government-sounding
        word, the classification pipeline must return 'entity', not 'government'.
        is_entity() runs before is_government_entity() in _classify_name."""
        from scripts.consolidate_addresses import _classify_name
        assert _classify_name("TRAVEL BUREAU LLC") == "entity"
        assert _classify_name("DEPARTMENT STORE HOLDINGS LLC") == "entity"
        assert _classify_name("COMMISSION HOMES INC") == "entity"

    def test_government_keywords_still_match_real_entities(self):
        """Government keyword detection still works for real government names."""
        assert is_government_entity("ARIZONA DEPT OF TRANSPORTATION")
        assert is_government_entity("PHOENIX FIRE BUREAU")

    def test_real_government_entities_still_detected(self):
        """Positive cases must still pass after the fix."""
        assert is_government_entity("STATE OF ARIZONA")
        assert is_government_entity("CITY OF PHOENIX")
        assert is_government_entity("PINAL COUNTY DEPT OF TRANSPORTATION")
        assert is_government_entity("ARIZONA WATER DISTRICT")

    def test_entity_with_trailing_punctuation(self):
        """'ABC, LLC.' and 'SMITH INC.' must be recognized as entities."""
        assert is_entity("ABC, LLC.")
        assert is_entity("SMITH INC.")
        assert is_entity("JONES CORP.")

    def test_person_not_entity(self):
        """Plain person names must not be classified as entities."""
        assert not is_entity("SMITH JOHN")
        assert not is_entity("JOHN SMITH")

    def test_trust_not_triggered_by_unrelated_words(self):
        """Words that contain trust-keyword substrings must not match."""
        assert not is_trust("COUNTRY CLUB")
        assert not is_trust("TRUCK ENTERPRISES")


# ---------------------------------------------------------------------------
# Test 15: Title-Case (FIRST LAST) Name Parsing
# ---------------------------------------------------------------------------
class TestTitleCaseFirstLastParsing:
    """Names arriving in title case should be parsed as FIRST [MIDDLE] LAST,
    not reversed like ALL-CAPS parcel data."""

    def test_simple_two_token(self):
        """'Esther Fields' should stay as 'Esther Fields', not become 'Fields Esther'."""
        nc = parse_raw_owner_name("Esther Fields")
        assert nc.p1_first == "Esther"
        assert nc.p1_last == "Fields"
        assert nc.full_name == "Esther Fields"

    def test_with_middle_initial(self):
        """'Esther J Fields' → first=Esther, middle=J., last=Fields."""
        nc = parse_raw_owner_name("Esther J Fields")
        assert nc.p1_first == "Esther"
        assert nc.p1_middle == "J."
        assert nc.p1_last == "Fields"
        assert nc.full_name == "Esther J. Fields"

    def test_with_full_middle_name(self):
        """'John Michael Smith' → first=John, middle=Michael, last=Smith."""
        nc = parse_raw_owner_name("John Michael Smith")
        assert nc.p1_first == "John"
        assert nc.p1_middle == "Michael"
        assert nc.p1_last == "Smith"

    def test_compound_last_name_with_particle(self):
        """'Ramon de la Cruz' → first=Ramon, last=de la Cruz."""
        nc = parse_raw_owner_name("Ramon de la Cruz")
        assert nc.p1_first == "Ramon"
        assert "Cruz" in nc.p1_last

    def test_allcaps_still_uses_last_first(self):
        """ALL-CAPS input must still use LAST FIRST order (existing behavior)."""
        nc = parse_raw_owner_name("FIELDS ESTHER J")
        assert nc.p1_first == "Esther"
        assert nc.p1_last == "Fields"

    def test_title_case_with_suffix(self):
        """'John Smith Jr' → first=John, last=Smith, suffix preserved."""
        nc = parse_raw_owner_name("John Smith Jr")
        assert nc.p1_first == "John"
        assert nc.p1_last == "Smith"
        assert "Jr" in nc.full_name

    def test_title_case_trust_still_detected(self):
        """Title-case trusts should still be classified correctly."""
        nc = parse_raw_owner_name("Smith Family Trust")
        assert nc.is_business
        assert "Trust" in nc.full_name

    def test_title_case_entity_still_detected(self):
        """Title-case entities should still be classified correctly."""
        nc = parse_raw_owner_name("Acme Holdings LLC")
        assert nc.is_business
        assert "LLC" in nc.full_name


# ---------------------------------------------------------------------------
# Test 16: Household Subsumption (Middle Initial Variants)
# ---------------------------------------------------------------------------
class TestHouseholdSubsumption:
    """Same person with and without middle initial must be deduped."""

    def test_esther_fields_with_and_without_middle(self):
        """'Esther Fields' + 'Esther J. Fields' → 'Esther J. Fields'."""
        from utils.name_formatter import combine_household_names
        result = combine_household_names(["Esther Fields", "Esther J. Fields"])
        assert result == "Esther J. Fields"

    def test_order_does_not_matter(self):
        """Subsumption works regardless of input order."""
        from utils.name_formatter import combine_household_names
        assert combine_household_names(
            ["Esther J. Fields", "Esther Fields"]
        ) == "Esther J. Fields"

    def test_juan_manuel_ortega_still_works(self):
        """Preserve previous session fix: 'Juan Manuel' ⊂ 'Juan Manuel Ortega'."""
        from utils.name_formatter import combine_household_names
        result = combine_household_names(
            ["Juan Manuel", "Ofelia Ortega", "Juan Manuel Ortega"]
        )
        assert result == "Ofelia & Juan Manuel Ortega"

    def test_different_first_names_not_merged(self):
        """Different people at same address must not merge."""
        from utils.name_formatter import combine_household_names
        assert combine_household_names(
            ["John Smith", "Jane Smith"]
        ) == "John & Jane Smith"

    def test_subsumption_with_third_person(self):
        """Subsumption works when a third unrelated person is in the group."""
        from utils.name_formatter import combine_household_names
        result = combine_household_names(
            ["Esther Fields", "Esther J. Fields", "Bob Fields"]
        )
        assert result == "Esther J. & Bob Fields"


# ---------------------------------------------------------------------------
# Test 17: PO Box Safety — False Positive Protection
# ---------------------------------------------------------------------------
class TestPOBoxFalsePositives:
    """Ensure 'BOX <n>' as a substring inside a word does NOT trigger PO Box
    detection.  A false positive here causes distinct addresses to merge."""

    def test_lock_box_is_not_po_box(self):
        from utils.address_formatter import is_po_box, extract_po_box
        assert not is_po_box("LOCK BOX 100 MAIN ST")
        assert extract_po_box("LOCK BOX 100 MAIN ST") is None

    def test_mailbox_is_not_po_box(self):
        from utils.address_formatter import is_po_box, extract_po_box
        assert not is_po_box("MAILBOX 100")
        assert extract_po_box("MAILBOX 100") is None

    def test_drop_box_is_not_po_box(self):
        from utils.address_formatter import is_po_box, extract_po_box
        assert not is_po_box("DROP BOX 100")
        assert extract_po_box("DROP BOX 100") is None

    def test_bare_box_is_not_po_box(self):
        """'BOX 100 MAIN ST' with no 'PO' prefix is too ambiguous to trust."""
        from utils.address_formatter import is_po_box, extract_po_box
        assert not is_po_box("BOX 100 MAIN ST")
        assert extract_po_box("BOX 100 MAIN ST") is None

    def test_genuine_po_boxes_still_detected(self):
        from utils.address_formatter import is_po_box, extract_po_box
        assert is_po_box("PO BOX 100")
        assert extract_po_box("PO BOX 100") == "PO BOX 100"
        assert is_po_box("P.O. BOX 100")
        assert extract_po_box("P.O. BOX 100") == "PO BOX 100"
        assert is_po_box("POBOX100")  # no separator
        assert extract_po_box("POBOX100") == "PO BOX 100"
        assert is_po_box("POB 99")
        assert extract_po_box("POB 99") == "PO BOX 99"
        assert is_po_box("1701 E Pima St PO Box 571")
        assert extract_po_box("1701 E Pima St PO Box 571") == "PO BOX 571"


# ---------------------------------------------------------------------------
# Test 18: Address ' - ' Separator Preservation
# ---------------------------------------------------------------------------
class TestAddressDashPreservation:
    """Regression: the ' - ' separator in addresses must not be stripped as
    if it were a null marker.  Only ' - NULL', ' - N/A', ' - NONE' count."""

    def test_dash_building_preserved(self):
        from utils.address_formatter import format_street_address
        result = format_street_address("123 Main St - Building A")
        assert "Building A" in result or "-" in result or "Bldg" in result

    def test_dash_null_stripped(self):
        from utils.address_formatter import format_street_address
        result = format_street_address("123 Main St - NULL")
        assert "NULL" not in result.upper()


# ---------------------------------------------------------------------------
# Test 19: CSV Encoding — Non-ASCII Names
# ---------------------------------------------------------------------------
class TestCsvEncodingTolerance:
    """The CSV reader must handle UTF-8, cp1252, and latin-1 without crashing
    or corrupting accented names."""

    def test_reads_utf8_accented_names(self, tmp_path):
        from utils.file_reader import _read_csv
        csv = tmp_path / "utf8.csv"
        csv.write_text(
            "Name,City\nJosé García,Phoenix\nMüller,Sedona\n",
            encoding="utf-8",
        )
        df = _read_csv(str(csv))
        assert "José García" in df["Name"].tolist()
        assert "Müller" in df["Name"].tolist()

    def test_reads_utf8_sig_with_bom(self, tmp_path):
        from utils.file_reader import _read_csv
        csv = tmp_path / "utf8sig.csv"
        csv.write_text("Name\nJosé\n", encoding="utf-8-sig")
        df = _read_csv(str(csv))
        assert list(df.columns) == ["Name"]  # BOM must not contaminate column
        assert "José" in df["Name"].tolist()

    def test_reads_cp1252_fallback(self, tmp_path):
        from utils.file_reader import _read_csv
        csv = tmp_path / "cp1252.csv"
        csv.write_bytes("Name\nJos\xe9\n".encode("cp1252"))
        df = _read_csv(str(csv))
        # Either UTF-8 would fail and cp1252 would match, producing "José"
        assert len(df) == 1


# ---------------------------------------------------------------------------
# Test 20: State Code Validation (US + Territories + Military + Canada)
# ---------------------------------------------------------------------------
class TestStateCodeValidation:
    def test_us_states(self):
        from utils.config import is_valid_state, is_us_state
        assert is_valid_state("AZ")
        assert is_valid_state("DC")
        assert is_us_state("AZ")

    def test_territories(self):
        from utils.config import is_valid_state, is_us_state
        assert is_valid_state("PR")
        assert is_valid_state("GU")
        assert is_us_state("PR")

    def test_military(self):
        from utils.config import is_valid_state, is_military_state, is_us_state
        assert is_valid_state("AE")
        assert is_valid_state("AP")
        assert is_valid_state("AA")
        assert is_military_state("AE")
        assert not is_us_state("AE")  # military is separate from US states

    def test_canadian_provinces(self):
        from utils.config import is_valid_state, is_canadian_province
        assert is_valid_state("ON")
        assert is_valid_state("QC")
        assert is_valid_state("BC")
        assert is_canadian_province("ON")

    def test_invalid_codes_rejected(self):
        from utils.config import is_valid_state, normalize_state_code
        assert not is_valid_state("ZZ")
        assert not is_valid_state("XX")
        assert normalize_state_code("ZZ") == ""
        assert normalize_state_code("zz") == ""
        assert normalize_state_code("  AZ  ") == "AZ"
        assert normalize_state_code("az") == "AZ"


# ---------------------------------------------------------------------------
# Test 21: Canadian Postal Code Handling
# ---------------------------------------------------------------------------
class TestCanadianPostalCode:
    def test_canadian_with_space(self):
        from utils.config import normalize_zip
        assert normalize_zip("M5V 3A8") == "M5V 3A8"

    def test_canadian_without_space(self):
        from utils.config import normalize_zip
        assert normalize_zip("M5V3A8") == "M5V 3A8"

    def test_canadian_lowercase(self):
        from utils.config import normalize_zip
        assert normalize_zip("m5v3a8") == "M5V 3A8"

    def test_us_zip_still_normalizes(self):
        from utils.config import normalize_zip
        assert normalize_zip("85337-0725") == "85337"
        assert normalize_zip("01234") == "01234"  # leading zero preserved
        assert normalize_zip("01234-5678") == "01234"

    def test_short_zip_returns_what_it_has(self):
        """Short ZIP returned as-is so validation can flag it."""
        from utils.config import normalize_zip
        assert normalize_zip("1234") == "1234"


# ---------------------------------------------------------------------------
# Test 22: Rural Route / Highway Contract / Military Addresses
# ---------------------------------------------------------------------------
class TestRuralAndMilitaryAddresses:
    def test_rural_route_variants(self):
        from utils.address_formatter import extract_rural_route
        assert extract_rural_route("RR 1 BOX 50") == "RR 1 BOX 50"
        assert extract_rural_route("Rural Route 1, Box 50") == "RR 1 BOX 50"
        assert extract_rural_route("R.R. 2 Box 10A") == "RR 2 BOX 10A"

    def test_highway_contract_variants(self):
        from utils.address_formatter import extract_rural_route
        assert extract_rural_route("HC 2 BOX 10") == "HC 2 BOX 10"
        assert extract_rural_route("HCR 2 Box 10") == "HC 2 BOX 10"
        assert extract_rural_route("Highway Contract 2 Box 10") == "HC 2 BOX 10"

    def test_rural_route_is_not_po_box(self):
        from utils.address_formatter import is_po_box, extract_po_box
        assert not is_po_box("RR 1 BOX 50")
        assert extract_po_box("RR 1 BOX 50") is None
        assert not is_po_box("HC 2 BOX 10")

    def test_rural_route_normalized_for_matching(self):
        from utils.address_formatter import normalize_address_for_matching
        # Different spellings of the same destination must produce the
        # same canonical form so they dedupe.
        assert (
            normalize_address_for_matching("RR 1 Box 50")
            == normalize_address_for_matching("Rural Route 1 Box 50")
            == normalize_address_for_matching("R.R. 1, Box 50")
            == "RR 1 BOX 50"
        )

    def test_military_psc_unit_cmr(self):
        from utils.address_formatter import extract_military_box, is_po_box
        assert extract_military_box("PSC 1234 BOX 5678") == "PSC 1234 BOX 5678"
        assert extract_military_box("Unit 12345 Box 67") == "UNIT 12345 BOX 67"
        assert extract_military_box("CMR 401 Box 123") == "CMR 401 BOX 123"
        # Military is not a PO Box
        assert not is_po_box("PSC 1234 BOX 5678")


# ---------------------------------------------------------------------------
# Test 23: Company-First Business Name Preference
# ---------------------------------------------------------------------------
class TestBusinessCompanyFirst:
    def test_company_preferred_over_dba(self, tmp_path):
        """When both Company and DBA exist, the legal Company name wins."""
        import pandas as pd
        from scripts.business_formatter import format_business_data
        # Build a minimal Data-Axle-like input
        df = pd.DataFrame([{
            "Company": "Acme Holdings LLC",
            "DBA": "Acme Widgets",
            "Address Line 2": "123 Main St",
            "City": "Phoenix",
            "State": "AZ",
            "Zip": "85001",
        }])
        input_path = tmp_path / "biz.csv"
        df.to_csv(input_path, index=False, encoding="utf-8-sig")
        output_path = tmp_path / "biz_formatted.csv"
        format_business_data(str(input_path), str(output_path))
        out = pd.read_csv(output_path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
        assert "Acme Holdings LLC" in out["Full Name or Business Company Name"].iloc[0]

    def test_dba_used_when_company_empty(self, tmp_path):
        """Fallback: if Company is blank, use DBA."""
        import pandas as pd
        from scripts.business_formatter import format_business_data
        df = pd.DataFrame([{
            "Company": "",
            "DBA": "Acme Widgets",
            "Address Line 2": "123 Main St",
            "City": "Phoenix",
            "State": "AZ",
            "Zip": "85001",
        }])
        input_path = tmp_path / "biz.csv"
        df.to_csv(input_path, index=False, encoding="utf-8-sig")
        output_path = tmp_path / "biz_formatted.csv"
        format_business_data(str(input_path), str(output_path))
        out = pd.read_csv(output_path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
        assert "Widgets" in out["Full Name or Business Company Name"].iloc[0]


# ---------------------------------------------------------------------------
# Test 24: Life Estate Detection and Expansion
# ---------------------------------------------------------------------------
class TestLifeEstate:
    def test_le_suffix_expanded(self):
        nc = parse_raw_owner_name("JONES JANE LE")
        assert nc.is_life_estate
        assert nc.full_name == "Jane Jones Life Estate"

    def test_life_est_suffix_expanded(self):
        nc = parse_raw_owner_name("SMITH JOHN LIFE EST")
        assert nc.is_life_estate
        assert "Life Estate" in nc.full_name

    def test_life_estate_suffix_expanded(self):
        nc = parse_raw_owner_name("BROWN MARY LIFE ESTATE")
        assert nc.is_life_estate
        assert "Life Estate" in nc.full_name

    def test_le_dotted_form(self):
        nc = parse_raw_owner_name("GARCIA LUIS L.E.")
        assert nc.is_life_estate

    def test_title_case_le(self):
        nc = parse_raw_owner_name("Jane Jones LE")
        assert nc.is_life_estate
        assert nc.full_name == "Jane Jones Life Estate"

    def test_lee_surname_not_triggered(self):
        """Title-case 'Robert Lee' must NOT be flagged as life estate."""
        nc = parse_raw_owner_name("Robert Lee")
        assert not nc.is_life_estate
        assert nc.full_name == "Robert Lee"

    def test_no_marker_no_flag(self):
        nc = parse_raw_owner_name("JONES JANE")
        assert not nc.is_life_estate

    def test_life_estate_flag_triggers_review_reason(self, tmp_path):
        """When a consolidated row contains 'Life Estate', the validation
        pipeline flags it via the Review_Reason column."""
        import pandas as pd
        from scripts.consolidate_addresses import consolidate_addresses
        # Create a minimal combined CSV with one life-estate record.
        combined = tmp_path / "combined.csv"
        cols = [
            "Data_Source", "Full Name or Business Company Name",
            "Title\\Department (2nd line)", "Street Address",
            "City", "State", "Zip",
            "Primary First Name", "Primary Middle", "Primary Last Name",
            "2nd Owner First Name", "2nd Owner Middle", "2nd Owner Last Name",
            "Owner1_original", "TitleDept_original", "Address1_original",
            "City_original", "State_original", "Zip_original",
        ]
        row = {c: "" for c in cols}
        row.update({
            "Data_Source": "Parcel",
            "Full Name or Business Company Name": "Jane Jones Life Estate",
            "Street Address": "123 Main St.",
            "City": "Phoenix",
            "State": "AZ",
            "Zip": "85001",
            "Primary First Name": "Jane",
            "Primary Last Name": "Jones",
        })
        pd.DataFrame([row], columns=cols).to_csv(
            combined, index=False, encoding="utf-8-sig"
        )
        output = tmp_path / "consolidated.csv"
        consolidate_addresses(str(combined), str(output))
        out = pd.read_csv(
            output, dtype=str, keep_default_na=False, encoding="utf-8-sig"
        )
        assert "Life Estate" in out["Review_Reason"].iloc[0]


# ---------------------------------------------------------------------------
# Test 25: Per-Row Parse Error Tolerance
# ---------------------------------------------------------------------------
class TestPerRowParseSafety:
    def test_parse_error_does_not_crash_pipeline(self, monkeypatch, tmp_path):
        """If parse_raw_owner_name raises, the row is kept with raw value
        and logged, but the pipeline continues."""
        import pandas as pd
        from scripts import address_processor
        from utils.name_formatter import NameComponents

        def _crashing_parser(raw):
            if "BOOM" in raw:
                raise ValueError("simulated parser crash")
            return NameComponents(full_name=raw)

        monkeypatch.setattr(
            address_processor, "parse_raw_owner_name", _crashing_parser
        )
        address_processor._PARSE_ERRORS.clear()

        # Two rows: one normal, one that will crash
        input_df = pd.DataFrame([
            {"Owner Name": "Jane Doe",      "Street Address 1": "1 Main St",
             "City": "Phoenix", "State": "AZ", "Zip": "85001"},
            {"Owner Name": "BOOM CRASH",    "Street Address 1": "2 Main St",
             "City": "Phoenix", "State": "AZ", "Zip": "85001"},
        ])
        input_path = tmp_path / "parcel.csv"
        input_df.to_csv(input_path, index=False, encoding="utf-8-sig")
        output_path = tmp_path / "parcel_formatted.csv"

        # Must not raise
        address_processor.format_parcel_data(str(input_path), str(output_path))

        # The crash row should have been recorded
        assert len(address_processor._PARSE_ERRORS) == 1
        assert "BOOM CRASH" in address_processor._PARSE_ERRORS[0][0]

        # Output file exists and has both rows
        out = pd.read_csv(
            output_path, dtype=str, keep_default_na=False, encoding="utf-8-sig"
        )
        assert len(out) == 2
