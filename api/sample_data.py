"""Fixed sample HCP/HCO payloads served by the mock API routes (routes/hcp.py, routes/hco.py) for local development and testing without live IQVIA credentials."""

from models import (
    Individual,
    AlternativeName,
    Identifier,
    Address,
    Phone,
    Specialty,
    Education,
    Tendency,
    OriginUniversity,
    Affiliation,
    Organization,
    HCORelation,
    HCPResponse,
    HCOResponse
)


# ==========================================================
# HCP SAMPLE DATA
# ==========================================================

hcp_sample = HCPResponse(

    batchId="BATCH_20260803_001",

    individual=Individual(

        individualEid="HCP100001",

        firstName="John",
        firstName2=None,
        firstNameInitials="J",
        usualFirstName="John",
        middleName="David",
        lastName="Smith",
        lastName2=None,

        typeCode="HCP",
        typeLabel="Healthcare Professional",
        typeCorporateLabel="Healthcare Professional",

        titleCode="DR",
        titleLabel="Doctor",
        titleCorporateLabel="Doctor",

        prefixNameCode="MR",
        prefixNameLabel="Mr",
        prefixNameCorporateLabel="Mr",

        genderCode="M",
        genderLabel="Male",
        genderCorporateLabel="Male",

        statusCode="ACTIVE",
        statusLabel="Active",
        statusCorporateLabel="Active",

        statusDate="2026-08-03",

        stateCode="CA",
        stateLabel="California",
        stateCorporateLabel="California"

    ),

    alternativeNames=[
        AlternativeName(
            alternateNameId="ALT001",
            individualEid="HCP100001",
            fullName="Dr John Smith"
        )
    ],

    identifiers=[
        Identifier(
            identifierId="ID001",
            individualEid="HCP100001",
            identifierTypeCode="NPI",
            identifierValue="1234567890"
        )
    ],

    addresses=[
        Address(
            addressId="ADDR001",
            individualEid="HCP100001",
            addressLine1="123 Main Street",
            city="Los Angeles",
            state="California",
            postalCode="90001",
            country="USA"
        )
    ],

    phones=[
        Phone(
            phoneId="PH001",
            individualEid="HCP100001",
            phoneNumber="+1-9876543210"
        )
    ],

    specialties=[
        Specialty(
            specialtyId="SP001",
            individualEid="HCP100001",
            specialtyCode="CARD",
            specialtyLabel="Cardiology",
            primaryFlag=True
        )
    ],

    education=[
        Education(
            educationId="ED001",
            individualEid="HCP100001",
            universityName="Stanford University",
            degree="MBBS",
            graduationYear=2012
        )
    ],

    tendencies=[
        Tendency(
            tendencyId="TEN001",
            individualEid="HCP100001",
            tendencyCode="DIGITAL"
        )
    ],

    originUniversity=[
        OriginUniversity(
            originUniversityId="OU001",
            individualEid="HCP100001",
            universityName="Stanford University",
            country="USA"
        )
    ],

    affiliations=[
        Affiliation(
            affiliationId="AFF001",
            individualEid="HCP100001",
            organizationEid="HCO100001",
            relationshipType="Practices At"
        )
    ]

)


# ==========================================================
# HCO SAMPLE DATA
# ==========================================================

hco_sample = HCOResponse(

    batchId="BATCH_20260803_001",

    organization=Organization(

        organizationEid="HCO100001",

        organizationName="ABC Medical Center",

        organizationType="Hospital",

        statusCode="ACTIVE",
        statusLabel="Active",

        stateCode="CA",
        stateLabel="California",

        country="USA"

    ),

    identifiers=[
        Identifier(
            identifierId="ORG001",
            identifierTypeCode="HOSPITAL_CODE",
            identifierValue="HSP001"
        )
    ],

    addresses=[
        Address(
            addressId="ADDR100",
            addressLine1="500 Hospital Road",
            city="Los Angeles",
            state="California",
            postalCode="90010",
            country="USA"
        )
    ],

    phones=[
        Phone(
            phoneId="ORGPHONE1",
            phoneNumber="+1-123456789"
        )
    ],

    specialties=[
        Specialty(
            specialtyId="ORGSP1",
            specialtyCode="MULTI",
            specialtyLabel="Multi Specialty"
        )
    ],

    alternativeNames=[
        AlternativeName(
            alternateNameId="ORGALT1",
            fullName="ABC Hospital"
        )
    ],

    relations=[
        HCORelation(
            relationId="REL001",
            parentOrganizationId="HCO100000",
            childOrganizationId="HCO100001",
            relationType="Parent"
        )
    ]

)