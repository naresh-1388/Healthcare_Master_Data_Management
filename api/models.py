"""Pydantic request/response models for the mock Healthcare MDM API, shaped to mirror the real IQVIA individual/organization API payloads closely enough for local development and testing."""

from typing import List, Optional
from pydantic import BaseModel


# ==========================================================
# HCP - Individual (raw_clarivate_hcp_name)
# ==========================================================

class Individual(BaseModel):
    individualEid: str

    firstName: Optional[str] = None
    firstName2: Optional[str] = None
    firstNameInitials: Optional[str] = None
    usualFirstName: Optional[str] = None
    middleName: Optional[str] = None
    lastName: Optional[str] = None
    lastName2: Optional[str] = None

    typeCode: Optional[str] = None
    typeLabel: Optional[str] = None
    typeCorporateLabel: Optional[str] = None

    titleCode: Optional[str] = None
    titleLabel: Optional[str] = None
    titleCorporateLabel: Optional[str] = None

    prefixNameCode: Optional[str] = None
    prefixNameLabel: Optional[str] = None
    prefixNameCorporateLabel: Optional[str] = None

    genderCode: Optional[str] = None
    genderLabel: Optional[str] = None
    genderCorporateLabel: Optional[str] = None

    statusCode: Optional[str] = None
    statusLabel: Optional[str] = None
    statusCorporateLabel: Optional[str] = None
    statusDate: Optional[str] = None

    stateCode: Optional[str] = None
    stateLabel: Optional[str] = None
    stateCorporateLabel: Optional[str] = None


# ==========================================================
# HCP - Alternative Name
# (raw_clarivate_hcp_alternative_name)
# ==========================================================

class AlternativeName(BaseModel):
    alternateNameId: Optional[str] = None
    individualEid: Optional[str] = None

    firstName: Optional[str] = None
    middleName: Optional[str] = None
    lastName: Optional[str] = None
    fullName: Optional[str] = None

    nameTypeCode: Optional[str] = None
    nameTypeLabel: Optional[str] = None

    languageCode: Optional[str] = None
    preferredFlag: Optional[bool] = None


# ==========================================================
# HCP - Identifier
# (raw_clarivate_hcp_identifier)
# ==========================================================

class Identifier(BaseModel):
    identifierId: Optional[str] = None
    individualEid: Optional[str] = None

    identifierTypeCode: Optional[str] = None
    identifierTypeLabel: Optional[str] = None

    identifierValue: Optional[str] = None

    issuingAuthority: Optional[str] = None
    issuingCountry: Optional[str] = None

    effectiveDate: Optional[str] = None
    expiryDate: Optional[str] = None

    status: Optional[str] = None


# ==========================================================
# HCP - Address
# (raw_clarivate_hcp_address)
# ==========================================================

class Address(BaseModel):
    addressId: Optional[str] = None
    individualEid: Optional[str] = None

    addressTypeCode: Optional[str] = None
    addressTypeLabel: Optional[str] = None

    addressLine1: Optional[str] = None
    addressLine2: Optional[str] = None
    addressLine3: Optional[str] = None

    city: Optional[str] = None
    state: Optional[str] = None
    postalCode: Optional[str] = None
    country: Optional[str] = None

    latitude: Optional[float] = None
    longitude: Optional[float] = None

    primaryFlag: Optional[bool] = None


# ==========================================================
# HCP - Phone
# (raw_clarivate_hcp_phone)
# ==========================================================

class Phone(BaseModel):
    phoneId: Optional[str] = None
    individualEid: Optional[str] = None

    phoneTypeCode: Optional[str] = None
    phoneTypeLabel: Optional[str] = None

    countryCode: Optional[str] = None
    phoneNumber: Optional[str] = None
    extension: Optional[str] = None

    preferredFlag: Optional[bool] = None


# ==========================================================
# HCP - Specialty
# (raw_clarivate_hcp_specialty)
# ==========================================================

class Specialty(BaseModel):
    specialtyId: Optional[str] = None
    individualEid: Optional[str] = None

    specialtyCode: Optional[str] = None
    specialtyLabel: Optional[str] = None
    specialtyType: Optional[str] = None

    primaryFlag: Optional[bool] = None
    status: Optional[str] = None


# ==========================================================
# HCP - Education
# (raw_clarivate_hcp_education)
# ==========================================================

class Education(BaseModel):
    educationId: Optional[str] = None
    individualEid: Optional[str] = None

    universityName: Optional[str] = None
    degree: Optional[str] = None
    faculty: Optional[str] = None

    graduationYear: Optional[int] = None
    country: Optional[str] = None


# ==========================================================
# HCP - Tendency
# (raw_clarivate_hcp_tendencies)
# ==========================================================

class Tendency(BaseModel):
    tendencyId: Optional[str] = None
    individualEid: Optional[str] = None

    tendencyCode: Optional[str] = None
    tendencyLabel: Optional[str] = None
    tendencyValue: Optional[str] = None


# ==========================================================
# HCP - Origin University
# (raw_clarivate_hcp_origin_university)
# ==========================================================

class OriginUniversity(BaseModel):
    originUniversityId: Optional[str] = None
    individualEid: Optional[str] = None

    universityName: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None


# ==========================================================
# HCP - Affiliation
# (raw_clarivate_affiliation)
# ==========================================================

class Affiliation(BaseModel):
    affiliationId: Optional[str] = None

    individualEid: Optional[str] = None
    organizationEid: Optional[str] = None

    relationshipType: Optional[str] = None
    primaryFlag: Optional[bool] = None

    startDate: Optional[str] = None
    endDate: Optional[str] = None


# ==========================================================
# HCO - Organization
# (raw_clarivate_hco_name)
# ==========================================================

class Organization(BaseModel):
    organizationEid: str

    organizationName: Optional[str] = None
    organizationType: Optional[str] = None

    statusCode: Optional[str] = None
    statusLabel: Optional[str] = None

    stateCode: Optional[str] = None
    stateLabel: Optional[str] = None

    country: Optional[str] = None


# ==========================================================
# HCO - HCO Relation
# (raw_clarivate_hco_hco_relation)
# ==========================================================

class HCORelation(BaseModel):
    relationId: Optional[str] = None

    parentOrganizationId: Optional[str] = None
    childOrganizationId: Optional[str] = None

    relationType: Optional[str] = None

    startDate: Optional[str] = None
    endDate: Optional[str] = None


# ==========================================================
# API Responses
# ==========================================================

class HCPResponse(BaseModel):
    # NOTE: batchId was removed here - it is NOT part of the real IQVIA
    # response payload. Per src_to_raw.md: "System columns such as
    # LOAD_DATE, BATCH_ID, and Source_Name are added" by our own
    # ingestion pipeline (src/ingestion/src_to_raw_ingestion.py) after
    # the API response is received, not sent by IQVIA itself.
    individual: Individual

    alternativeNames: List[AlternativeName] = []

    identifiers: List[Identifier] = []

    addresses: List[Address] = []

    specialties: List[Specialty] = []

    phones: List[Phone] = []

    education: List[Education] = []

    tendencies: List[Tendency] = []

    originUniversity: List[OriginUniversity] = []

    affiliations: List[Affiliation] = []


class HCOResponse(BaseModel):
    # NOTE: batchId removed - same reasoning as HCPResponse above.
    organization: Organization

    identifiers: List[Identifier] = []

    addresses: List[Address] = []

    specialties: List[Specialty] = []

    phones: List[Phone] = []

    alternativeNames: List[AlternativeName] = []

    relations: List[HCORelation] = []
