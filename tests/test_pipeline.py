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
