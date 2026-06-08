WITH
  /* Mappings */
  accounts_mapping AS (
    SELECT
      '10101001' AS "Id",
      'P151132028' AS "MappedId"
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10101002',
      'P151132028'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10101004',
      'P151132028'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10101005',
      'P151132028'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10101011',
      'P141L53001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10101012',
      'P141L53I01'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10101013',
      'P141L53T01'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10101031',
      'P141U53001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10101032',
      'P141U53I01'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10101033',
      'P141U53T01'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10101070',
      'P141100055'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10101071',
      'P141100056'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10102011',
      'P141E99006'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10103001',
      'P147100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10103002',
      'P147100002'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10103004',
      'P141120100'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10103005',
      'P143110000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10103006',
      'P143110000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10103008',
      'P147101102'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10103009',
      'P147100011'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10103011',
      'P141100035'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10103022',
      'P147110000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10103023',
      'P147110000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10104004',
      'P151132028'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10104005',
      'P149100009'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10104006',
      'P149100009'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10104007',
      'P149100008'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10104008',
      'P147100020'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10104010',
      'P149146005'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10104021',
      'P149110004'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10105001',
      'P164100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10105002',
      'P161100041'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10105003',
      'P161100017'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10105004',
      'P164130000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10105005',
      'P163100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10105008',
      'P161100041'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10105009',
      'P162101003'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10105010',
      'P161100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10105017',
      'P167110101'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10105031',
      'P164150000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10105032',
      'P161121004'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10105033',
      'P161121011'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10105034',
      'P164160000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10105035',
      'P163110000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10105043',
      'P167110030'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10107001',
      'P149120000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10107002',
      'P149120015'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10107004',
      'P149140000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10107006',
      'P149160000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10108003',
      'P151132002'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10201002',
      'P111120000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10201003',
      'P111160000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10201004',
      'P111170000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10201005',
      'P111170001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10201006',
      'P111120001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10201007',
      'P111150000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10202006',
      'P122230001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10203001',
      'P111180900'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10204001',
      'P122140000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10206001',
      'P112100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10206002',
      'P112140000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10206003',
      'P112150000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10206004',
      'P112150001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10206005',
      'P112100001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10206006',
      'P112130000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10206021',
      'P124140000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10206022',
      'P127000001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10302013',
      'P147101100'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '10302014',
      'P136120001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20101045',
      'P238000001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20102001',
      'P211210013'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20102002',
      'P241130046'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20103001',
      'P242100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20103002',
      'P242100002'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20103005',
      'P240120000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20103006',
      'P240120000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20103007',
      'P242100004'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20104001',
      'P249100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20104002',
      'P242100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105001',
      'P246110034'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105002',
      'P249100006'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105003',
      'P246121000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105017',
      'P249110007'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105018',
      'P249110000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105019',
      'P249154007'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105020',
      'P249100032'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105022',
      'P249100033'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105025',
      'P249100034'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105027',
      'P249100035'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105028',
      'P249100036'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105029',
      'P249112003'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105030',
      'P249100037'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105031',
      'P249100038'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105032',
      'P249100039'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105033',
      'P249100040'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105034',
      'P249100041'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20105037',
      'P233100001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20106001',
      'P246111000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20106002',
      'P246110011'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20107001',
      'P249120003'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20107003',
      'P251121046'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20107004',
      'P251121005'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20107005',
      'P251121999'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20107007',
      'P251121011'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20107008',
      'P235110000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20107009',
      'P251121005'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20107010',
      'P251121005'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20107011',
      'P251121996'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20107012',
      'P251121999'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '20201007',
      'P238000001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '30101001',
      'P211100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '30101002',
      'P211150000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '30101011',
      'P211210011'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '30101012',
      'P213100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '30101013',
      'P211210005'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '40101001',
      'P314100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '40101002',
      'P311100013'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '40101003',
      'P312000000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '40101004',
      'P312000000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '40101007',
      'P316130009'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '40101008',
      'P311100022'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '40101009',
      'P311100023'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '50101001',
      'P474110033'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '50101002',
      'P415100001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '50101003',
      'P474110045'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '50101004',
      'P474110034'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '50101005',
      'P474110011'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '50101008',
      'P474110045'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '50101010',
      'P411100019'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '50101011',
      'P454000000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '50101013',
      'P411100023'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '50101014',
      'P411100024'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101001',
      'P621100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101002',
      'P622100100'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101003',
      'P622100997'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101004',
      'P625150002'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101005',
      'P621120000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101006',
      'P621100021'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101007',
      'P621100003'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101008',
      'P625163999'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101009',
      'P621110000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101010',
      'P625150001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101012',
      'P625140000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101020',
      'P625130000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101021',
      'P625164000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101101',
      'P625150008'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101102',
      'P625163002'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101103',
      'P621102038'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101104',
      'P532170000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101105',
      'P625163999'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101106',
      'P622100043'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60101111',
      'P625163999'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60102001',
      'P532100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60102002',
      'P532100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60102003',
      'P532100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60102004',
      'P532100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60102005',
      'P532190111'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60103001',
      'P537180000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60103002',
      'P537190000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60103004',
      'P537100100'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60103005',
      'P537201999'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60103006',
      'P535100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60103007',
      'P537201999'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60103008',
      'P521130002'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60103009',
      'P534110000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60103010',
      'P535100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60104001',
      'P511100105'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60104002',
      'P511100105'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60105001',
      'P534140100'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60105002',
      'P534140102'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60105003',
      'P534140003'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60106001',
      'P521130997'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60106004',
      'P521130997'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60106005',
      'P521130997'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60107001',
      'P531100103'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60107002',
      'P531100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60107003',
      'P531100104'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60107004',
      'P531100103'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60108001',
      'P533100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60108002',
      'P533100001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60108003',
      'P533100002'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60108004',
      'P537140000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60108005',
      'P534100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60108006',
      'P534190005'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60108007',
      'P534140002'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60108008',
      'P537160001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60108009',
      'P535104001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60109001',
      'P534160000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60109003',
      'P534180099'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60109004',
      'P537110999'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60109005',
      'P537171002'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60109006',
      'P537171002'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60109007',
      'P532110000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60109008',
      'P723120999'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60109009',
      'P532120000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60109010',
      'P732136002'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60109011',
      'P732136100'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60109013',
      'P537171101'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60109014',
      'P621110023'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60109016',
      'P532190001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '60109031',
      'P537201999'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '70101003',
      'P717120999'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '70101011',
      'P317170999'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80101003',
      'P534180000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80101005',
      'P719140002'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80101006',
      'P719150001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80101007',
      'P719130000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80102001',
      'P631150107'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80102005',
      'P635000001'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80103001',
      'P536101100'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80103002',
      'P536101100'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80103004',
      'P475130003'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80103005',
      'P475130028'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80103006',
      'P475130015'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80103007',
      'P475130004'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80103008',
      'P536101100'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80104001',
      'P721100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80104003',
      'P721100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80105001',
      'P741100000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80105002',
      'P741110000'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80105003',
      'P723120999'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '80106002',
      'P532190005'
    FROM
      DUMMY
    UNION ALL
    SELECT
      '90101001',
      'P751100001'
    FROM
      DUMMY
  ),
  /* Entries */
  entries AS (
    SELECT
      JDT1."Account" AS "ItemText",
      JDT1."Debit" - JDT1."Credit" AS "Amount",
      OADM."MainCurncy" AS "Currency",
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
      COALESCE(am."MappedId", 'NOT MAPPED') AS "Account"
    FROM
      JDT1
      CROSS JOIN OADM
      INNER JOIN OACT ON OACT."AcctCode" = JDT1."Account"
      LEFT JOIN OCRD ON OCRD."CardCode" = JDT1."ShortName"
      LEFT JOIN accounts_mapping am ON am."Id" = JDT1."Account"
    WHERE
      JDT1."RefDate" <= '2026-05-31' -- Filter by posting date
      AND JDT1."Debit" <> JDT1."Credit" -- Exclude zero-balance lines
      AND JDT1."Account" NOT LIKE '102%' -- Exclude FA accounts
      AND OACT."GroupMask" IN (1, 2, 3) -- Keep only BS accounts
      AND OCRD."CardCode" IS NULL -- Exclude bp lines
      AND am."Id" IS NOT NULL -- WARNING: LUT only, exclude unmapped accounts
  ),
  reconciled_entries AS (
    SELECT
      *
    FROM
      entries
    UNION ALL
    SELECT
      "ItemText",
      "Amount" * -1 AS "Amount",
      "Currency",
      'reconciliation' AS "AccountGroup",
      'P291100000' AS "Account" -- BS reconciliation account
    FROM
      entries
  ),
  reconciled_entries2 AS (
    SELECT
      SUM("Amount") AS "Amount",
      "ItemText",
      "Currency",
      "AccountGroup",
      "Account"
    FROM
      reconciled_entries
    GROUP BY
      "ItemText",
      "Currency",
      "AccountGroup",
      "Account"
    HAVING
      SUM("Amount") <> 0 -- Exclude zero-balance sum
  ),
  /* Calculations */
  calc AS (
    SELECT
      *,
      DENSE_RANK() OVER (
        ORDER BY
          "ItemText"
      ) AS "Grouping"
    FROM
      reconciled_entries2
  ),
  /* Query */
  query AS (
    SELECT
      "Grouping",
      'E930' AS "CompanyCode", -- MGLX company code
      'ZS' AS "DocumentType", -- BS document type
      '20260531' AS "PostingDate", -- Adjust based on posting date filter
      'BS-ACCTS' AS "Reference",
      'BS-Migration' AS "DocHeaderText",
      'S' AS "ItemType", -- G/L item type
      "Account",
      "Currency",
      CAST("Amount" AS BIGINT) AS "Amount",
      "ItemText",
      "AccountGroup" AS "CheckAccountGroup"
    FROM
      calc
    ORDER BY
      "ItemText",
      "AccountGroup"
  )
SELECT
  "Grouping" AS "1_grouping",
  "CompanyCode" AS "2_company_code",
  "DocumentType" AS "3_document_type",
  "PostingDate" AS "4_document_date",
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
  NULL AS "48_baseline_date",
  NULL AS "49_payment_method",
  NULL AS "50_payment_block",
  NULL AS "51_segment",
  NULL AS "52_cross_company",
  NULL AS "53_gl_accnt_999",
  NULL AS "54_prctr_999",
  NULL AS "55_amount_di",
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
  "CheckAccountGroup"
FROM
  query
;
