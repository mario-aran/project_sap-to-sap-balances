WITH
  /* Documents */
  documents AS (
    SELECT
      *
    FROM
      OINV -- A/R Invoice
    UNION ALL
    SELECT
      *
    FROM
      ORIN -- A/R Credit Memo
  ),
  /* Entries */
  entries AS (
    SELECT
      JDT1."TransId",
      JDT1."Line_ID",
      JDT1."Account" AS "SourceAccount",
      TO_VARCHAR (JDT1."TaxDate", 'YYYYMMDD') AS "DocumentDate",
      TO_VARCHAR (JDT1."DueDate", 'YYYYMMDD') AS "BaselineDate",
      COALESCE(JDT1."FCCurrency", OADM."MainCurncy") AS "Currency",
      OCRD."CardCode",
      COALESCE(OCRD."U_ID_SAP_AFS1", 'NOT MAPPED') AS "Account",
      COALESCE(
        LPAD (OCRD."U_ID_SAP_AFS1", 10, '0'),
        'NOT MAPPED'
      ) || '-' || OCRD."CardCode" AS "ItemText",
      COALESCE(
        d."FolioPref" || '-' || d."FolioNum",
        TO_VARCHAR (JDT1."TransId")
      ) AS "Reference",
      CASE
        WHEN OACT."GroupMask" = 1 THEN '01 assets'
        WHEN OACT."GroupMask" = 2 THEN '02 liabilities'
        WHEN OACT."GroupMask" = 3 THEN '03 equity'
        WHEN OACT."GroupMask" = 4 THEN '04 revenue'
        WHEN OACT."GroupMask" = 5 THEN '05 cost of goods sold'
        WHEN OACT."GroupMask" = 6 THEN '06 expenses'
        WHEN OACT."GroupMask" = 7 THEN '07 other income'
        WHEN OACT."GroupMask" = 8 THEN '08 other expenses'
      END AS "AccountGroup",
      JDT1."BalDueDeb" - JDT1."BalDueCred" AS "AmountLocal",
      CASE
        WHEN JDT1."FCCurrency" IS NOT NULL THEN JDT1."BalDueDeb" - JDT1."BalDueCred"
      END AS "AmountDi",
      CASE
        WHEN JDT1."FCCurrency" IS NOT NULL THEN JDT1."BalFcDeb" - JDT1."BalFcCred"
        ELSE JDT1."BalDueDeb" - JDT1."BalDueCred"
      END AS "Amount"
    FROM
      JDT1
      CROSS JOIN OADM
      INNER JOIN OACT ON OACT."AcctCode" = JDT1."Account"
      INNER JOIN OCRD ON OCRD."CardCode" = JDT1."ShortName"
      LEFT JOIN documents d ON d."TransId" = JDT1."TransId"
    WHERE
      JDT1."RefDate" <= '2026-06-30' -- Filter by posting date
      AND JDT1."BalDueDeb" <> JDT1."BalDueCred" -- Exclude zero-balance due lines
      AND OCRD."CardType" = 'C' -- Keep only customer lines
      AND JDT1."Account" NOT IN ('10103004', '10104004') -- Exclude F.PISCOPO Template Accounts
      AND JDT1."Account" NOT IN (
        '10101001',
        '10101002',
        '10101004',
        '10101005',
        '10101011',
        '10101013',
        '10101031',
        '10101070',
        '10101071',
        '10102011',
        '10103022',
        '10103023',
        '10103026',
        '10104006',
        '10104007',
        '10104008',
        '10104010',
        '10105001',
        '10105002',
        '10105003',
        '10105004',
        '10105005',
        '10105008',
        '10105009',
        '10105010',
        '10105031',
        '10105032',
        '10105033',
        '10105034',
        '10105035',
        '10105043',
        '10107001',
        '10107002',
        '10107003',
        '10107006',
        '10108003',
        '10202006',
        '10206022',
        '10302014',
        '20101045',
        '20102001',
        '20102002',
        '20105001',
        '20105002',
        '20105003',
        '20105004',
        '20105017',
        '20105018',
        '20105019',
        '20105020',
        '20105021',
        '20105022',
        '20105025',
        '20105027',
        '20105028',
        '20105029',
        '20105030',
        '20105031',
        '20105032',
        '20105033',
        '20105034',
        '20105037',
        '20106001',
        '20106002',
        '20107001',
        '20107003',
        '20107004',
        '20107005',
        '20107007',
        '20107008',
        '20107009',
        '20107010',
        '20107011',
        '20201007',
        '30101001',
        '30101002',
        '30101011',
        '30101013'
      ) -- Exclude bp lines but keep bs accounts
  ),
  reconciled_entries AS (
    SELECT
      'D' AS "ItemType", -- Customer item type
      *
    FROM
      entries
    UNION ALL
    SELECT
      'S' AS "ItemType", -- G/L item type
      "TransId",
      "Line_ID",
      "SourceAccount",
      "DocumentDate",
      NULL AS "BaselineDate",
      "Currency",
      "CardCode",
      'P291100001' AS "Account", -- Customer reconciliation account
      "ItemText",
      "Reference",
      'reconciliation' AS "AccountGroup",
      "AmountLocal" * -1 AS "AmountLocal",
      "AmountDi" * -1 AS "AmountDi",
      "Amount" * -1 AS "Amount"
    FROM
      entries
  ),
  /* Calculations */
  calc AS (
    SELECT
      *,
      DENSE_RANK() OVER (
        ORDER BY
          "CardCode",
          "TransId"
      ) AS "Grouping"
    FROM
      reconciled_entries
  ),
  /* Query */
  query AS (
    SELECT
      "Grouping",
      'E930' AS "CompanyCode", -- MGLX company code
      'Z1' AS "DocumentType", -- Customer document type
      "DocumentDate",
      '20260630' AS "PostingDate", -- Adjust based on posting date filter
      "Reference",
      'AR OI-Migration' AS "DocHeaderText",
      "ItemType",
      "Account",
      "Currency",
      REPLACE (CAST("Amount" AS FLOAT), '.', ',') AS "Amount",
      "ItemText",
      "BaselineDate",
      CAST("AmountDi" AS BIGINT) AS "AmountDi",
      "AccountGroup" AS "CheckAccountGroup",
      "SourceAccount" AS "CheckAccount",
      "CardCode" AS "CheckBusinessPartner",
      CAST("AmountLocal" AS BIGINT) AS "CheckAmountLocal"
    FROM
      calc
    ORDER BY
      "CardCode",
      "TransId",
      "Line_ID",
      "ItemType"
  )
SELECT
  "Grouping" AS "1_grouping",
  "CompanyCode" AS "2_company_code",
  "DocumentType" AS "3_document_type",
  "DocumentDate" AS "4_document_date",
  "PostingDate" AS "5_posting_date",
  NULL AS "6_reverse_date",
  NULL AS "7_currency_date",
  "Reference" AS "8_reference",
  "DocHeaderText" AS "9_doc_header_text",
  NULL AS "10_local_ledger",
  NULL AS "11_posting_key",
  "ItemType" AS "12_item_type",
  "Account" AS "13_account",
  NULL AS "14_special_gl_indicator",
  "Currency" AS "15_currency",
  NULL AS "16_exchange_rate",
  "Amount" AS "17_amount",
  NULL AS "18_vat_code",
  NULL AS "19_base_amount",
  NULL AS "20_vat_aut_calculation",
  NULL AS "21_tax_aut_calc",
  NULL AS "22_vat_amount",
  NULL AS "23_balancing_acct",
  NULL AS "24_balancing_profit_center",
  NULL AS "25_assignment",
  "ItemText" AS "26_item_text",
  NULL AS "27_mov_type",
  NULL AS "28_cost_center",
  NULL AS "29_profit_center",
  NULL AS "30_internal_order",
  NULL AS "31_wbe_wbs_element",
  NULL AS "32_plant_site",
  NULL AS "33_material",
  NULL AS "34_quantity",
  NULL AS "35_uom",
  NULL AS "36_brand_category",
  NULL AS "37_product_line",
  NULL AS "38_collection_type",
  NULL AS "39_material_class",
  NULL AS "40_distribution_channel",
  NULL AS "41_geographical_area",
  NULL AS "42_country",
  NULL AS "43_ref_customer",
  NULL AS "44_trading_partner",
  NULL AS "45_reference_key",
  NULL AS "46_key_ref_1",
  NULL AS "47_payment_terms",
  "BaselineDate" AS "48_baseline_date",
  NULL AS "49_payment_method",
  NULL AS "50_payment_block",
  NULL AS "51_segment",
  NULL AS "52_cross_company",
  NULL AS "53_gl_accnt_999",
  NULL AS "54_prctr_999",
  "AmountDi" AS "55_amount_di",
  NULL AS "56_amt_base_di",
  NULL AS "57_date_of_dunning_note",
  NULL AS "58_dunning_level",
  NULL AS "59_base_wt",
  NULL AS "60_base_wt_localc_curr",
  NULL AS "61_amount_wt",
  NULL AS "62_amount_wt_localc_curr",
  NULL AS "63_type_of_wt",
  NULL AS "64_code_of_wt",
  NULL AS "65_payment_reference",
  NULL AS "66_discount_base",
  NULL AS "67_reference_key_2",
  NULL AS "68_invoice_receipt_date",
  "CheckAccountGroup",
  "CheckAccount",
  "CheckBusinessPartner",
  "CheckAmountLocal"
FROM
  query
;
