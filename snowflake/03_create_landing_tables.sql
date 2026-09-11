-- ============================================================
-- Healthcare_Master_Data_Management - Snowflake DDL
-- Auto-generated from the audited HMDM_DEV mapping workbook.
-- Layer: LANDING
-- Every table/column here is taken directly from the workbook's
-- Raw_to_Land sheet(s) - nothing here was invented independently of
-- that already-reviewed mapping.
-- ============================================================
USE DATABASE HMDM_DEV;
USE SCHEMA LANDING;

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCO_ADDRESS (
    "Source_FK" VARCHAR(4000),
    "Address_PK" VARCHAR(4000),
    "Address_Eid" VARCHAR(4000),
    "Address_Line_1" VARCHAR(4000),
    "City" VARCHAR(4000),
    "Postal_Code" VARCHAR(4000),
    "Country" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCO_ALTERNATE_NAME (
    "Source_FK" VARCHAR(4000),
    "Alternate_Name_PK" VARCHAR(4000),
    "Alternate_Name" VARCHAR(4000),
    "Alternate_Name_Type" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCO_EMAIL (
    "Source_FK" VARCHAR(4000),
    "Email_PK" VARCHAR(4000),
    "Email" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCO_HIERARCHY (
    "Source_FK" VARCHAR(4000),
    "Hierarchy_PK" VARCHAR(4000),
    "Parent_Organization_EID" VARCHAR(4000),
    "Relationship_Type" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCO_IDENTIFICATION (
    "Source_FK" VARCHAR(4000),
    "Identification_PK" VARCHAR(4000),
    "Identifier_Value" VARCHAR(4000),
    "Identifier_Type" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCO_NAME (
    "Source_FK" VARCHAR(4000),
    "Name_PK" VARCHAR(4000),
    "HCO_Name" VARCHAR(4000),
    "HCO_Type" VARCHAR(4000),
    "Status" VARCHAR(4000),
    "Country" VARCHAR(4000),
    "Website" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ,
    "Population_Name" VARCHAR(4000),
    "HCO_Subtype" VARCHAR(4000),
    "Bed_Count" NUMBER(10,0),
    "Resident_Count" NUMBER(10,0),
    "Transparency_Reporting_Name" VARCHAR(4000),
    "Official_Name" VARCHAR(4000),
    "Parent_Organization_Name" VARCHAR(4000),
    "Teaching_Hospital_Flag" BOOLEAN,
    "Profit_Flag" BOOLEAN,
    "Accept_Medicare" BOOLEAN,
    "E_Medical_Record" VARCHAR(4000),
    "Pay_Perform" VARCHAR(4000),
    "E_Prescribe" VARCHAR(4000),
    "Formulary" VARCHAR(4000),
    "Activation_Date" TIMESTAMP_NTZ,
    "Accept_Medicaid" BOOLEAN,
    "Ownership_Status" VARCHAR(4000),
    "Third_Party_ID" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCO_PHONE (
    "Source_FK" VARCHAR(4000),
    "Phone_PK" VARCHAR(4000),
    "Phone_Number" VARCHAR(4000),
    "Phone_Type" VARCHAR(4000),
    "Phone_Status" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCO_SPECIALTY (
    "Source_FK" VARCHAR(4000),
    "Specialty_PK" VARCHAR(4000),
    "Specialty" VARCHAR(4000),
    "Specialty_Rank" VARCHAR(4000),
    "Specialty_Type" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCO_TAX (
    "Source_FK" VARCHAR(4000),
    "Tax_PK" VARCHAR(4000),
    "Tax_Number" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCP_ADDRESS (
    "Source_FK" VARCHAR(4000),
    "Address_PK" VARCHAR(4000),
    "Address_Line_1" VARCHAR(4000),
    "Address_Line_2" VARCHAR(4000),
    "City" VARCHAR(4000),
    "Country" VARCHAR(4000),
    "Postal_Code" VARCHAR(4000),
    "Region" VARCHAR(4000),
    "Province" VARCHAR(4000),
    "Brick_code" VARCHAR(4000),
    "Address_Type" VARCHAR(4000),
    "Latitude" VARCHAR(4000),
    "Longitude" VARCHAR(4000),
    "Building_Label" VARCHAR(4000),
    "Location_Name" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ,
    "Status" VARCHAR(4000),
    "State" VARCHAR(4000),
    "County" VARCHAR(4000),
    "Primary_Address_Flag" BOOLEAN
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCP_ALTERNATE_NAME (
    "Source_FK" VARCHAR(4000),
    "Alternate_Name_PK" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ,
    "Alternate_Name_Type" VARCHAR(4000),
    "Alternate_Name" VARCHAR(4000),
    "Alternate_Name_Status" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCP_EDUCATION (
    "Source_FK" VARCHAR(4000),
    "Education_PK" VARCHAR(4000),
    "Qualification" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCP_EMAIL (
    "Source_FK" VARCHAR(4000),
    "Email_PK" VARCHAR(4000),
    "Email" VARCHAR(4000),
    "Email_Usage_Type" VARCHAR(4000),
    "Email_Status" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCP_HCO_AFFILIATION (
    "Source_FK" VARCHAR(4000),
    "Affiliation_PK" VARCHAR(4000),
    "HCO_EID" VARCHAR(4000),
    "Relationship_Type" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCP_IDENTIFICATION (
    "Source_FK" VARCHAR(4000),
    "Identification_PK" VARCHAR(4000),
    "Identifier_Value" VARCHAR(4000),
    "Source_Updated_Date" TIMESTAMP_NTZ,
    "Identifier_Type" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCP_LANGUAGE (
    "Source_FK" VARCHAR(4000),
    "Language_PK" VARCHAR(4000),
    "Language" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCP_NAME (
    "Name_PK" VARCHAR(4000),
    "First_Name" VARCHAR(4000),
    "Middle_Name" VARCHAR(4000),
    "Last_Name" VARCHAR(4000),
    "Transparency_Reporting_Name" VARCHAR(4000),
    "Prefix_Name" VARCHAR(4000),
    "Gender" VARCHAR(4000),
    "HCP_Type" VARCHAR(4000),
    "Title" VARCHAR(4000),
    "Record_Status" VARCHAR(4000),
    "Status" VARCHAR(4000),
    "External_Data_Privacy" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ,
    "Third_Party_ID" VARCHAR(4000),
    "Source_ID" VARCHAR(4000),
    "Full_Name" VARCHAR(4000),
    "Exclude_from_Search" VARCHAR(4000),
    "Takeda_Only_Account" NUMBER(10,0),
    "Country" VARCHAR(4000),
    "Population_Name" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCP_ORIGIN_UNIVERSITY (
    "Source_FK" VARCHAR(4000),
    "Origin_University_PK" VARCHAR(4000),
    "University_Name" VARCHAR(4000),
    "University_Code" VARCHAR(4000),
    "Graduation_Year" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCP_PHONE (
    "Source_FK" VARCHAR(4000),
    "Phone_PK" VARCHAR(4000),
    "Phone_Number" VARCHAR(4000),
    "Phone_Number_Extension" VARCHAR(4000),
    "ISO" VARCHAR(4000),
    "Primary_Phone" VARCHAR(4000),
    "Phone_Type" VARCHAR(4000),
    "Phone_Usage_Type" VARCHAR(4000),
    "Phone_Status" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCP_SPECIALTY (
    "Source_FK" VARCHAR(4000),
    "Specialty_PK" VARCHAR(4000),
    "Specialty_Rank" VARCHAR(4000),
    "Specialty" VARCHAR(4000),
    "Global_Specialty" VARCHAR(4000),
    "Group_Specialty" VARCHAR(4000),
    "Source_Name" VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCP_TAX (
    "Source_FK" VARCHAR(4000),
    "Tax_PK" VARCHAR(4000),
    "Tax_Number" VARCHAR(4000),
    "Tax_Number_Type" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS HMDM_DEV.LANDING.HCP_TENDENCIES (
    "Source_FK" VARCHAR(4000),
    "Tendencies_PK" VARCHAR(4000),
    "Code" VARCHAR(4000),
    "Code_Label" VARCHAR(4000),
    "Code_Corporate_Label" VARCHAR(4000),
    "Rank" VARCHAR(4000),
    "Type" VARCHAR(4000),
    "List_Code" VARCHAR(4000),
    "List_Code_Label" VARCHAR(4000),
    "List_Code_Corporate_Label" VARCHAR(4000),
    "Source_Name" VARCHAR(4000),
    "Source_Created_Date" TIMESTAMP_NTZ,
    "Source_Updated_Date" TIMESTAMP_NTZ
);

