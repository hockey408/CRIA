/* ================================== Query 1: MoM_Account_Detail =======================================
   Non-Accrual MoM Account Detail — Staging layer for Power Query facility rollup and analyst review.

   OUTPUT DESIGN (36 columns, 5 groups):
     G1  Identity & Event Keys        — routing, joining, facility identity
     G2  Movement & Review            — movement type, owner, review tier flags
     G3  Balances                     — MoM balance fields + CIS exposure
     G4  Analyst Review Attributes    — event-side resolved; always populated regardless of IN/OUT
     G5  Auto_Validated Diagnostics   — which of the 5 conditions fired; QA/sanity layer

   MOVEMENT_TYPE (4 values):
     Downgrade IN, Exit, Persisting Balance Changes, No Change.
     RBC status no longer drives Movement_Type — surfaced via Owner and Auto_Validated instead.

   OWNER LOGIC:
     - RBC_Code = 'R' → NO OWNER regardless of all other rules.
     - INFL10 CSS → Daniel (hard override).
     - L-CIT RE Term Loans → Samya (regardless of threshold size).
     - L-CIT Non-RE Term Loans / Commitment Draw → Casey (regardless of threshold size).
     - L-SVB → Eleanor. L-FCB → Connor.
     - Coverage_Threshold is a separate column; owner assignment is not suppressed by it.

   REVIEW TIER (G2 flags):
     Is_Reviewable          = Downgrade IN or Exit with positive exposure. Full in/out universe.
     Coverage_Threshold     = 1/0/NULL. Samya/Casey L-CIT products only. NULL = not applicable.
     Auto_Validated         = Downgrade IN: any of 5 conditions. Exit: RBC=R only.
     Manual_Review_Required = Is_Reviewable AND NOT Auto_Validated AND Coverage_Threshold != 0.
*/

WITH latest_curr AS (
  SELECT MAX(End_of_Month_Date) AS curr_eom
  FROM V_SECURE_PORTFOLIO_DETAIL_LOANHUB
  WHERE Non_Accrual_Flag = 'Y'
),
latest_prev AS (
  SELECT MAX(End_of_Month_Date) AS prev_eom
  FROM V_SECURE_PORTFOLIO_DETAIL_LOANHUB, latest_curr
  WHERE End_of_Month_Date < (SELECT curr_eom FROM latest_curr)
    AND Non_Accrual_Flag = 'Y'
),
params AS (
  SELECT
    (SELECT curr_eom FROM latest_curr) AS curr_eom,
    (SELECT prev_eom FROM latest_prev) AS prev_eom,
    CURRENT_TIMESTAMP                  AS query_run_ts
),

/* ── 1. Base population ────────────────────────────────────────────────────────────────────────── */
base AS (
  SELECT
    End_of_Month_Date,
    UPPER(TRIM(Account_Key))         AS Account_Key,
    UPPER(TRIM(Facility_ID))         AS Facility_ID,
    UPPER(TRIM(CIS_Customer_Number)) AS CIS_Customer_Number,
    Account_Name,
    Contract_Source_System,
    PROD_HIER_LEVEL_5,
    RBC_Code,
    Status_Code_Description,
    GL_ACCOUNT_HIER_LEVEL_4,
    ACCOUNT_IDENTIFIER,
    GL_ACCOUNT_CODE,
    Non_Accrual_Flag,
    Source_System_Balance,
    PD_Grade,
    Days_Past_Due,
    FAS_114_FLAG,
    CHARGE_OFF_AMOUNT_MTD AS CO_MTD,
    CHARGE_OFF_AMOUNT_ITD AS CO_ITD,
    CASE
      WHEN Contract_Source_System IN ('AL','CU','LN','LO','UU','US','TA','LJ','CF','FRDS-EXCPTN','FRDS_EXCPTN')
        THEN 'L-SVB'
      WHEN Contract_Source_System = 'ALL'
           AND GL_ACCOUNT_CODE IN
           ('1051872','1061833','1041344','1041211','1041345','1041364','1041210',
            '1042127','1042213','1042048','1042368','1042408','1042409','1042424',
            '1100001','1142067','1042886','1043008')
        THEN 'L-SVB'
      WHEN Contract_Source_System IN ('ADJ','ALL','ALS','FDR','FISERV','GGSL','GL','LEA','PCFS','PSL','SBO')
        THEN 'L-FCB'
      WHEN Contract_Source_System IN ('ACAR01','ACAR01-EXCPTN','HUBFSV','HUBFSV-EXCPTN',
                                      'INFL05','INFL05-EXCPTN','INFL10','INFL10-EXCPTN',
                                      'LNIQ01','LNIQ01-EXCPTN','MSP001','SBO001','SBO001-EXCPTN',
                                      'STKY01','STKY01-EXCPTN','STRAO1','STRAO1-EXCPTN','STRAT1','STRAT1-EXCPTN')
        THEN 'L-CIT'
      ELSE 'NULL-Need to Research'
    END AS Bank_Code
  FROM V_SECURE_PORTFOLIO_DETAIL_LOANHUB
  WHERE End_of_Month_Date IN ((SELECT prev_eom FROM params),(SELECT curr_eom FROM params))
    AND Non_Accrual_Flag = 'Y'
    AND GL_ACCOUNT_HIER_LEVEL_4 IN ('Total Loans','Lns Held for Sale')
),

/* ── 2. Prior EOM — 1 row per Account_Key ─────────────────────────────────────────────────────── */
prev_norm AS (
  SELECT
    Account_Key,
    MAX(Facility_ID)             AS Prev_Facility_ID,
    MAX(CIS_Customer_Number)     AS Prev_CIS,
    MAX(Account_Name)            AS Prev_Account_Name,
    MAX(Contract_Source_System)  AS Prev_CSS,
    MAX(PROD_HIER_LEVEL_5)       AS Prev_PROD5,
    MAX(RBC_Code)                AS Prev_RBC,
    MAX(Status_Code_Description) AS Prev_Status,
    MAX(Bank_Code)               AS Prev_Bank_Code,
    SUM(Source_System_Balance)   AS Prev_SSB,
    MAX(PD_Grade)                AS Prev_PD_Grade,
    MAX(Days_Past_Due)           AS Prev_DPD,
    MAX(FAS_114_FLAG)            AS Prev_FAS_114_FLAG,
    MAX(GL_ACCOUNT_HIER_LEVEL_4)  AS Prev_GL_L4,
    MAX(ACCOUNT_IDENTIFIER)       AS Prev_Account_Identifier,
    SUM(CO_MTD)                  AS Prev_CO_MTD,
    SUM(CO_ITD)                  AS Prev_CO_ITD
  FROM base
  WHERE End_of_Month_Date = (SELECT prev_eom FROM params)
  GROUP BY Account_Key
),

/* ── 3. Current EOM — 1 row per Account_Key ───────────────────────────────────────────────────── */
curr_norm AS (
  SELECT
    Account_Key,
    MAX(Facility_ID)             AS Curr_Facility_ID,
    MAX(CIS_Customer_Number)     AS Curr_CIS,
    MAX(Account_Name)            AS Curr_Account_Name,
    MAX(Contract_Source_System)  AS Curr_CSS,
    MAX(PROD_HIER_LEVEL_5)       AS Curr_PROD5,
    MAX(RBC_Code)                AS Curr_RBC,
    MAX(Status_Code_Description) AS Curr_Status,
    MAX(Bank_Code)               AS Curr_Bank_Code,
    SUM(Source_System_Balance)   AS Curr_SSB,
    MAX(PD_Grade)                AS Curr_PD_Grade,
    MAX(Days_Past_Due)           AS Curr_DPD,
    MAX(FAS_114_FLAG)            AS Curr_FAS_114_FLAG,
    MAX(GL_ACCOUNT_HIER_LEVEL_4)  AS Curr_GL_L4,
    MAX(ACCOUNT_IDENTIFIER)       AS Curr_Account_Identifier,
    SUM(CO_MTD)                  AS Curr_CO_MTD,
    SUM(CO_ITD)                  AS Curr_CO_ITD
  FROM base
  WHERE End_of_Month_Date = (SELECT curr_eom FROM params)
  GROUP BY Account_Key
),

/* ── 4. Full outer join @ Account_Key ─────────────────────────────────────────────────────────── */
joined AS (
  SELECT
    COALESCE(c.Account_Key, p.Account_Key) AS Account_Key,
    p.Prev_Facility_ID,    p.Prev_CIS,    p.Prev_Account_Name,
    p.Prev_CSS,            p.Prev_PROD5,  p.Prev_RBC,
    p.Prev_Status,         p.Prev_Bank_Code,
    p.Prev_SSB,
    p.Prev_PD_Grade,       p.Prev_DPD,    p.Prev_FAS_114_FLAG,
    p.Prev_CO_MTD,         p.Prev_CO_ITD,
    p.Prev_GL_L4,          p.Prev_Account_Identifier,
    c.Curr_Facility_ID,    c.Curr_CIS,    c.Curr_Account_Name,
    c.Curr_CSS,            c.Curr_PROD5,  c.Curr_RBC,
    c.Curr_Status,         c.Curr_Bank_Code,
    c.Curr_SSB,
    c.Curr_PD_Grade,       c.Curr_DPD,    c.Curr_FAS_114_FLAG,
    c.Curr_CO_MTD,         c.Curr_CO_ITD,
    c.Curr_GL_L4,          c.Curr_Account_Identifier
  FROM curr_norm c
  FULL OUTER JOIN prev_norm p ON c.Account_Key = p.Account_Key
),

/* ── 5. Flags: presence, RBC-auto, auto-validated ─────────────────────────────────────────────── */
with_flags AS (
  SELECT
    j.Account_Key,
    j.Prev_Facility_ID,    j.Prev_CIS,            j.Prev_Account_Name,
    j.Prev_CSS,            j.Prev_PROD5,          j.Prev_RBC,
    j.Prev_Status,         j.Prev_Bank_Code,      j.Prev_SSB,
    j.Prev_PD_Grade,       j.Prev_DPD,            j.Prev_FAS_114_FLAG,
    j.Prev_CO_MTD,         j.Prev_CO_ITD,         j.Prev_GL_L4,
    j.Prev_Account_Identifier,
    j.Curr_Facility_ID,    j.Curr_CIS,            j.Curr_Account_Name,
    j.Curr_CSS,            j.Curr_PROD5,          j.Curr_RBC,
    j.Curr_Status,         j.Curr_Bank_Code,      j.Curr_SSB,
    j.Curr_PD_Grade,       j.Curr_DPD,            j.Curr_FAS_114_FLAG,
    j.Curr_CO_MTD,         j.Curr_CO_ITD,         j.Curr_GL_L4,
    j.Curr_Account_Identifier,

    /* Presence */
    CASE WHEN j.Prev_SSB IS NULL THEN 0 ELSE 1 END AS had_prev,
    CASE WHEN j.Curr_SSB IS NULL THEN 0 ELSE 1 END AS has_curr,

    /* RBC_Auto — RBC='R' only; used for Auto_Validated on Exit events and owner NO OWNER assignment */
    CASE WHEN UPPER(COALESCE(j.Prev_RBC,'')) = 'R' THEN 1 ELSE 0 END AS prev_rbc_auto,
    CASE WHEN UPPER(COALESCE(j.Curr_RBC,'')) = 'R' THEN 1 ELSE 0 END AS curr_rbc_auto,

    /* Auto_Validated — 5 conditions OR'd; drives Auto_Validated and Manual_Review_Required */
    CASE
      WHEN UPPER(COALESCE(j.Prev_RBC,''))           = 'R'               THEN 1
      WHEN COALESCE(j.Prev_DPD, 0)                  >= 90               THEN 1
      WHEN j.Prev_PD_Grade                           IN ('12','13','14') THEN 1
      WHEN UPPER(COALESCE(j.Prev_FAS_114_FLAG,''))  = 'Y'               THEN 1
      WHEN COALESCE(j.Prev_CO_ITD, 0)               > 0                 THEN 1
      ELSE 0
    END AS prev_auto,

    CASE
      WHEN UPPER(COALESCE(j.Curr_RBC,''))           = 'R'               THEN 1
      WHEN COALESCE(j.Curr_DPD, 0)                  >= 90               THEN 1
      WHEN j.Curr_PD_Grade                           IN ('12','13','14') THEN 1
      WHEN UPPER(COALESCE(j.Curr_FAS_114_FLAG,''))  = 'Y'               THEN 1
      WHEN COALESCE(j.Curr_CO_ITD, 0)               > 0                 THEN 1
      ELSE 0
    END AS curr_auto,

    /* Facility-level presence flags — used in event_scoped to drive _For_Event resolution
       at facility grain rather than account grain, matching Movement_Type logic exactly    */
    CASE WHEN SUM(ABS(COALESCE(j.Prev_SSB,0)))
              OVER (PARTITION BY COALESCE(j.Curr_Facility_ID, j.Prev_Facility_ID)) > 0
         THEN 1 ELSE 0 END AS fac_has_prev,
    CASE WHEN SUM(ABS(COALESCE(j.Curr_SSB,0)))
              OVER (PARTITION BY COALESCE(j.Curr_Facility_ID, j.Prev_Facility_ID)) > 0
         THEN 1 ELSE 0 END AS fac_has_curr

  FROM joined j
),

/* ── 6. Event-scoped fields (curr if present, else prev) ──────────────────────────────────────── */
event_scoped AS (
  SELECT
    w.Account_Key,
    w.Prev_Facility_ID,    w.Prev_CIS,            w.Prev_Account_Name,
    w.Prev_CSS,            w.Prev_PROD5,          w.Prev_RBC,
    w.Prev_Status,         w.Prev_Bank_Code,      w.Prev_SSB,
    w.Prev_PD_Grade,       w.Prev_DPD,            w.Prev_FAS_114_FLAG,
    w.Prev_CO_MTD,         w.Prev_CO_ITD,         w.Prev_GL_L4,
    w.Prev_Account_Identifier,
    w.Curr_Facility_ID,    w.Curr_CIS,            w.Curr_Account_Name,
    w.Curr_CSS,            w.Curr_PROD5,          w.Curr_RBC,
    w.Curr_Status,         w.Curr_Bank_Code,      w.Curr_SSB,
    w.Curr_PD_Grade,       w.Curr_DPD,            w.Curr_FAS_114_FLAG,
    w.Curr_CO_MTD,         w.Curr_CO_ITD,         w.Curr_GL_L4,
    w.Curr_Account_Identifier,
    w.had_prev,            w.has_curr,
    w.prev_rbc_auto,       w.curr_rbc_auto,
    w.prev_auto,           w.curr_auto,
    w.fac_has_prev,        w.fac_has_curr,

    /* Identity */
    CASE WHEN w.fac_has_prev=1 AND w.fac_has_curr=0 THEN w.Prev_Facility_ID
         WHEN w.fac_has_curr=1               THEN w.Curr_Facility_ID
         ELSE COALESCE(w.Curr_Facility_ID, w.Prev_Facility_ID)
    END AS Facility_For_Event,

    CASE WHEN w.fac_has_prev=1 AND w.fac_has_curr=0 THEN w.Prev_CIS
         WHEN w.fac_has_curr=1               THEN w.Curr_CIS
         ELSE COALESCE(w.Curr_CIS, w.Prev_CIS)
    END AS CIS_For_Event,

    CASE WHEN w.fac_has_curr=1 THEN w.Curr_Account_Name
         WHEN w.fac_has_prev=1 THEN w.Prev_Account_Name
         ELSE COALESCE(w.Curr_Account_Name, w.Prev_Account_Name)
    END AS Account_Name_For_Event,

    CASE WHEN w.fac_has_prev=1 AND w.fac_has_curr=0 THEN w.Prev_Bank_Code
         WHEN w.fac_has_curr=1               THEN w.Curr_Bank_Code
         ELSE COALESCE(w.Curr_Bank_Code, w.Prev_Bank_Code)
    END AS Bank_For_Event,

    /* Ownership inputs */
    CASE WHEN w.fac_has_prev=1 AND w.fac_has_curr=0 THEN w.Prev_CSS
         WHEN w.fac_has_curr=1               THEN w.Curr_CSS
         ELSE COALESCE(w.Curr_CSS, w.Prev_CSS)
    END AS CSS_For_Event,

    CASE WHEN w.fac_has_prev=1 AND w.fac_has_curr=0 THEN w.Prev_PROD5
         WHEN w.fac_has_curr=1               THEN w.Curr_PROD5
         ELSE COALESCE(w.Curr_PROD5, w.Prev_PROD5)
    END AS PROD5_For_Event,

    CASE WHEN w.fac_has_prev=1 AND w.fac_has_curr=0 THEN w.Prev_RBC
         WHEN w.fac_has_curr=1               THEN w.Curr_RBC
         ELSE COALESCE(w.Curr_RBC, w.Prev_RBC)
    END AS RBC_For_Event,

    /* Auto_Validated event-side flag — mirrors facility_classified logic exactly:
       Downgrade IN (fac_has_curr=1, no prev): all 5 conditions via curr_auto
       Exit (fac_has_prev=1, no curr):          RBC=R only via prev_rbc_auto
       Persisting (both present):               curr_auto for consistency, though not used in review flags */
    CASE
      WHEN w.fac_has_prev=0 AND w.fac_has_curr=1 THEN w.curr_auto
      WHEN w.fac_has_prev=1 AND w.fac_has_curr=0 THEN w.prev_rbc_auto
      ELSE w.curr_auto
    END AS Auto_Validated_For_Event,

    /* Analyst review attributes — event-side resolved; always populated for IN and OUT */
    CASE WHEN w.fac_has_prev=1 AND w.fac_has_curr=0 THEN w.Prev_Status
         WHEN w.fac_has_curr=1               THEN w.Curr_Status
         ELSE COALESCE(w.Curr_Status, w.Prev_Status)
    END AS Status_For_Event,

    CASE WHEN w.fac_has_prev=1 AND w.fac_has_curr=0 THEN w.Prev_PD_Grade
         WHEN w.fac_has_curr=1               THEN w.Curr_PD_Grade
         ELSE COALESCE(w.Curr_PD_Grade, w.Prev_PD_Grade)
    END AS PD_Grade_For_Event,

    CASE WHEN w.fac_has_prev=1 AND w.fac_has_curr=0 THEN w.Prev_DPD
         WHEN w.fac_has_curr=1               THEN w.Curr_DPD
         ELSE COALESCE(w.Curr_DPD, w.Prev_DPD)
    END AS DPD_For_Event,

    CASE WHEN w.fac_has_prev=1 AND w.fac_has_curr=0 THEN w.Prev_FAS_114_FLAG
         WHEN w.fac_has_curr=1               THEN w.Curr_FAS_114_FLAG
         ELSE COALESCE(w.Curr_FAS_114_FLAG, w.Prev_FAS_114_FLAG)
    END AS FAS114_For_Event,

    CASE WHEN w.fac_has_prev=1 AND w.fac_has_curr=0 THEN w.Prev_GL_L4
         WHEN w.fac_has_curr=1               THEN w.Curr_GL_L4
         ELSE COALESCE(w.Curr_GL_L4, w.Prev_GL_L4)
    END AS GL_For_Event,

    CASE WHEN w.fac_has_prev=1 AND w.fac_has_curr=0 THEN w.Prev_Account_Identifier
         WHEN w.fac_has_curr=1               THEN w.Curr_Account_Identifier
         ELSE COALESCE(w.Curr_Account_Identifier, w.Prev_Account_Identifier)
    END AS Account_Identifier_For_Event,

    CASE WHEN w.fac_has_prev=1 AND w.fac_has_curr=0 THEN COALESCE(w.Prev_CO_MTD, 0)
         WHEN w.fac_has_curr=1               THEN COALESCE(w.Curr_CO_MTD, 0)
         ELSE COALESCE(w.Curr_CO_MTD, w.Prev_CO_MTD, 0)
    END AS CO_MTD_For_Event,

    CASE WHEN w.fac_has_prev=1 AND w.fac_has_curr=0 THEN COALESCE(w.Prev_CO_ITD, 0)
         WHEN w.fac_has_curr=1               THEN COALESCE(w.Curr_CO_ITD, 0)
         ELSE COALESCE(w.Curr_CO_ITD, w.Prev_CO_ITD, 0)
    END AS CO_ITD_For_Event,

    /* Balances */
    (COALESCE(w.Curr_SSB,0) - COALESCE(w.Prev_SSB,0)) AS MoM_Delta,

    CASE WHEN w.fac_has_curr=1 THEN COALESCE(w.Curr_SSB,0)
         ELSE COALESCE(w.Prev_SSB,0)
    END AS SSB_For_Event

  FROM with_flags w
),

/* ── 7. CIS exposure basis ────────────────────────────────────────────────────────────────────── */
with_cis_exposure AS (
  SELECT
    e.Account_Key,
    e.Prev_Facility_ID,    e.Prev_CIS,            e.Prev_Account_Name,
    e.Prev_CSS,            e.Prev_PROD5,          e.Prev_RBC,
    e.Prev_Status,         e.Prev_Bank_Code,      e.Prev_SSB,
    e.Prev_PD_Grade,       e.Prev_DPD,            e.Prev_FAS_114_FLAG,
    e.Prev_CO_MTD,         e.Prev_CO_ITD,         e.Prev_GL_L4,
    e.Prev_Account_Identifier,
    e.Curr_Facility_ID,    e.Curr_CIS,            e.Curr_Account_Name,
    e.Curr_CSS,            e.Curr_PROD5,          e.Curr_RBC,
    e.Curr_Status,         e.Curr_Bank_Code,      e.Curr_SSB,
    e.Curr_PD_Grade,       e.Curr_DPD,            e.Curr_FAS_114_FLAG,
    e.Curr_CO_MTD,         e.Curr_CO_ITD,         e.Curr_GL_L4,
    e.Curr_Account_Identifier,
    e.had_prev,            e.has_curr,
    e.prev_rbc_auto,       e.curr_rbc_auto,
    e.prev_auto,           e.curr_auto,
    e.fac_has_prev,        e.fac_has_curr,
    e.Facility_For_Event,  e.CIS_For_Event,       e.Account_Name_For_Event,
    e.Bank_For_Event,      e.CSS_For_Event,        e.PROD5_For_Event,
    e.RBC_For_Event,       e.Auto_Validated_For_Event,
    e.Status_For_Event,    e.PD_Grade_For_Event,  e.DPD_For_Event,
    e.FAS114_For_Event,    e.GL_For_Event,
    e.Account_Identifier_For_Event,
    e.CO_MTD_For_Event,    e.CO_ITD_For_Event,
    e.MoM_Delta,           e.SSB_For_Event,
    SUM(CASE WHEN e.has_curr=1 THEN COALESCE(e.Curr_SSB,0) ELSE 0 END)
      OVER (PARTITION BY e.CIS_For_Event) AS CIS_Curr_SSB_ByEventCIS,
    SUM(CASE WHEN e.had_prev=1 THEN COALESCE(e.Prev_SSB,0) ELSE 0 END)
      OVER (PARTITION BY e.CIS_For_Event) AS CIS_Prev_SSB_ByEventCIS,
    GREATEST(
      SUM(CASE WHEN e.has_curr=1 THEN COALESCE(e.Curr_SSB,0) ELSE 0 END)
        OVER (PARTITION BY e.CIS_For_Event),
      SUM(CASE WHEN e.had_prev=1 THEN COALESCE(e.Prev_SSB,0) ELSE 0 END)
        OVER (PARTITION BY e.CIS_For_Event)
    ) AS CIS_SSB_Exposure_ByEventCIS
  FROM event_scoped e
),

/* ── 8. Account-level owner (event-side) ──────────────────────────────────────────────────────── */
account_owner_event AS (
  SELECT
    x.Account_Key,
    x.Prev_Facility_ID,    x.Prev_CIS,            x.Prev_Account_Name,
    x.Prev_CSS,            x.Prev_PROD5,          x.Prev_RBC,
    x.Prev_Status,         x.Prev_Bank_Code,      x.Prev_SSB,
    x.Prev_PD_Grade,       x.Prev_DPD,            x.Prev_FAS_114_FLAG,
    x.Prev_CO_MTD,         x.Prev_CO_ITD,         x.Prev_GL_L4,
    x.Prev_Account_Identifier,
    x.Curr_Facility_ID,    x.Curr_CIS,            x.Curr_Account_Name,
    x.Curr_CSS,            x.Curr_PROD5,          x.Curr_RBC,
    x.Curr_Status,         x.Curr_Bank_Code,      x.Curr_SSB,
    x.Curr_PD_Grade,       x.Curr_DPD,            x.Curr_FAS_114_FLAG,
    x.Curr_CO_MTD,         x.Curr_CO_ITD,         x.Curr_GL_L4,
    x.Curr_Account_Identifier,
    x.had_prev,            x.has_curr,
    x.prev_rbc_auto,       x.curr_rbc_auto,
    x.prev_auto,           x.curr_auto,
    x.fac_has_prev,        x.fac_has_curr,
    x.Facility_For_Event,  x.CIS_For_Event,       x.Account_Name_For_Event,
    x.Bank_For_Event,      x.CSS_For_Event,        x.PROD5_For_Event,
    x.RBC_For_Event,       x.Auto_Validated_For_Event,
    x.Status_For_Event,    x.PD_Grade_For_Event,  x.DPD_For_Event,
    x.FAS114_For_Event,    x.GL_For_Event,
    x.Account_Identifier_For_Event,
    x.CO_MTD_For_Event,    x.CO_ITD_For_Event,
    x.MoM_Delta,           x.SSB_For_Event,
    x.CIS_Curr_SSB_ByEventCIS,
    x.CIS_Prev_SSB_ByEventCIS,
    x.CIS_SSB_Exposure_ByEventCIS,
    CASE
      /* RBC=R: no analyst owner — reviewed via automated process */
      WHEN UPPER(COALESCE(x.RBC_For_Event,'')) = 'R'                  THEN 'NO OWNER'
      WHEN x.CSS_For_Event = 'INFL10'                                  THEN 'Daniel'
      /* Samya/Casey: owner assigned regardless of threshold size.
         Coverage_Threshold column separately gates Manual_Review_Required. */
      WHEN x.PROD5_For_Event = 'Business RE Term Loans'
           AND x.Bank_For_Event = 'L-CIT'                              THEN 'Samya'
      WHEN x.PROD5_For_Event IN ('Business Non-RE Term Loans','Business Commitment Draw')
           AND x.Bank_For_Event = 'L-CIT'                              THEN 'Casey'
      WHEN x.Bank_For_Event = 'L-SVB'                                  THEN 'Eleanor'
      WHEN x.Bank_For_Event = 'L-FCB'                                  THEN 'Connor'
      ELSE NULL
    END AS Owner_Account_For_Event
  FROM with_cis_exposure x
),

/* ── 9. Rank accounts within facility (largest |SSB_For_Event| = representative row) ─────────── */
ranked_in_facility AS (
  SELECT
    a.Account_Key,
    a.Prev_Facility_ID,    a.Prev_CIS,            a.Prev_Account_Name,
    a.Prev_CSS,            a.Prev_PROD5,          a.Prev_RBC,
    a.Prev_Status,         a.Prev_Bank_Code,      a.Prev_SSB,
    a.Prev_PD_Grade,       a.Prev_DPD,            a.Prev_FAS_114_FLAG,
    a.Prev_CO_MTD,         a.Prev_CO_ITD,         a.Prev_GL_L4,
    a.Prev_Account_Identifier,
    a.Curr_Facility_ID,    a.Curr_CIS,            a.Curr_Account_Name,
    a.Curr_CSS,            a.Curr_PROD5,          a.Curr_RBC,
    a.Curr_Status,         a.Curr_Bank_Code,      a.Curr_SSB,
    a.Curr_PD_Grade,       a.Curr_DPD,            a.Curr_FAS_114_FLAG,
    a.Curr_CO_MTD,         a.Curr_CO_ITD,         a.Curr_GL_L4,
    a.Curr_Account_Identifier,
    a.had_prev,            a.has_curr,
    a.prev_rbc_auto,       a.curr_rbc_auto,
    a.prev_auto,           a.curr_auto,
    a.fac_has_prev,        a.fac_has_curr,
    a.Facility_For_Event,  a.CIS_For_Event,       a.Account_Name_For_Event,
    a.Bank_For_Event,      a.CSS_For_Event,        a.PROD5_For_Event,
    a.RBC_For_Event,       a.Auto_Validated_For_Event,
    a.Status_For_Event,    a.PD_Grade_For_Event,  a.DPD_For_Event,
    a.FAS114_For_Event,    a.GL_For_Event,
    a.Account_Identifier_For_Event,
    a.CO_MTD_For_Event,    a.CO_ITD_For_Event,
    a.MoM_Delta,           a.SSB_For_Event,
    a.CIS_Curr_SSB_ByEventCIS,
    a.CIS_Prev_SSB_ByEventCIS,
    a.CIS_SSB_Exposure_ByEventCIS,
    a.Owner_Account_For_Event,
    ROW_NUMBER() OVER (
      PARTITION BY a.Bank_For_Event, a.Facility_For_Event
      ORDER BY ABS(COALESCE(a.SSB_For_Event,0)) DESC, a.Account_Key
    ) AS rn_fac
  FROM account_owner_event a
),

/* ── 10. Facility rollup ──────────────────────────────────────────────────────────────────────── */
facility_rollup AS (
  SELECT
    Bank_For_Event,
    Facility_For_Event,
    SUM(ABS(COALESCE(Prev_SSB,0))) AS Fac_Prev_Abs_SSB,
    SUM(ABS(COALESCE(Curr_SSB,0))) AS Fac_Curr_Abs_SSB,
    SUM(CASE WHEN COALESCE(Prev_SSB,0) > 0 THEN COALESCE(Prev_SSB,0) ELSE 0 END) AS Fac_Prev_Pos_SSB,
    SUM(CASE WHEN COALESCE(Curr_SSB,0) > 0 THEN COALESCE(Curr_SSB,0) ELSE 0 END) AS Fac_Curr_Pos_SSB,
    SUM(COALESCE(Prev_SSB,0))      AS Fac_Prev_Net_SSB,
    SUM(COALESCE(Curr_SSB,0))      AS Fac_Curr_Net_SSB,
    /* RBC-only flags: used for Auto_Validated on Exit events and NO OWNER assignment */
    MAX(prev_rbc_auto)             AS Fac_Prev_RBC_Auto_Any,
    MAX(curr_rbc_auto)             AS Fac_Curr_RBC_Auto_Any,
    /* Auto_Validated (all 5 conditions): drives owner suppression + Is_Reviewable/Manual_Review_Required */
    MAX(prev_auto)                 AS Fac_Prev_Auto_Any,
    MAX(curr_auto)                 AS Fac_Curr_Auto_Any,
    MAX(CASE WHEN CSS_For_Event='INFL10' THEN 1 ELSE 0 END) AS Fac_INFL10_Any,
    /* Representative attributes from top |SSB_For_Event| account */
    MAX(CASE WHEN rn_fac=1 THEN PROD5_For_Event              END) AS Fac_PROD5_For_Event,
    MAX(CASE WHEN rn_fac=1 THEN RBC_For_Event                END) AS Fac_RBC_For_Event,
    MAX(CASE WHEN rn_fac=1 THEN Owner_Account_For_Event      END) AS Fac_Owner_From_TopRow,
    MAX(CASE WHEN rn_fac=1 THEN COALESCE(CIS_Curr_SSB_ByEventCIS,0)     END) AS Fac_CIS_Curr_SSB,
    MAX(CASE WHEN rn_fac=1 THEN COALESCE(CIS_Prev_SSB_ByEventCIS,0)     END) AS Fac_CIS_Prev_SSB,
    MAX(CASE WHEN rn_fac=1 THEN COALESCE(CIS_SSB_Exposure_ByEventCIS,0) END) AS Fac_CIS_SSB_Exposure,
    MAX(CASE WHEN rn_fac=1 THEN CIS_For_Event                           END) AS Fac_CIS_For_Event
  FROM ranked_in_facility
  GROUP BY Bank_For_Event, Facility_For_Event
),

/* -- 11. CIS-level threshold evaluation                                                           */
/* Aggregates SSB at CIS + Bank + PROD5 grain and evaluates coverage thresholds.
   Downgrade IN uses CIS-level Curr SSB; Exit uses CIS-level Prev SSB.
   One Coverage_Threshold per CIS -- stamped down to all facilities under that CIS.
   Applies only to L-CIT RE Term Loans (Samya >$500k) and Non-RE/Draw (Casey >$250k).
   NULL for all other bank/product combinations. */
cis_threshold AS (
  SELECT
    Bank_For_Event,
    CIS_For_Event,
    PROD5_For_Event,
    SUM(COALESCE(Curr_SSB,0)) AS CIS_Curr_SSB_Sum,
    SUM(COALESCE(Prev_SSB,0)) AS CIS_Prev_SSB_Sum
  FROM ranked_in_facility
  WHERE Bank_For_Event = 'L-CIT'
    AND PROD5_For_Event IN (
          'Business RE Term Loans',
          'Business Non-RE Term Loans',
          'Business Commitment Draw'
        )
  GROUP BY Bank_For_Event, CIS_For_Event, PROD5_For_Event
),


/* ── 12. Facility classification: Movement_Type + Owner + Review Tiers ────────────────────────── */
facility_classified AS (
  SELECT
    fr.Bank_For_Event,
    fr.Facility_For_Event,
    fr.Fac_Prev_Abs_SSB,   fr.Fac_Curr_Abs_SSB,
    fr.Fac_Prev_Pos_SSB,   fr.Fac_Curr_Pos_SSB,
    fr.Fac_Prev_Net_SSB,   fr.Fac_Curr_Net_SSB,
    fr.Fac_Prev_RBC_Auto_Any, fr.Fac_Curr_RBC_Auto_Any,
    fr.Fac_Prev_Auto_Any,  fr.Fac_Curr_Auto_Any,
    fr.Fac_INFL10_Any,
    fr.Fac_PROD5_For_Event, fr.Fac_RBC_For_Event,
    fr.Fac_Owner_From_TopRow,
    fr.Fac_CIS_Curr_SSB,   fr.Fac_CIS_Prev_SSB,   fr.Fac_CIS_SSB_Exposure,
    fr.Fac_CIS_For_Event,
    CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END AS Fac_Had_Prev,
    CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END AS Fac_Has_Curr,

    /* Facility owner — INFL10 hard override; RBC=R → NO OWNER; else named analyst */
    CASE
      WHEN fr.Fac_INFL10_Any = 1                                THEN 'Daniel'
      WHEN UPPER(COALESCE(fr.Fac_RBC_For_Event,'')) = 'R'      THEN 'NO OWNER'
      ELSE fr.Fac_Owner_From_TopRow
    END AS Owner_Facility_For_Event,

    /* Movement_Type — 4 values. All entries are Downgrade IN, all exits are Exit,
       regardless of RBC status. RBC is now reflected in Owner and Auto_Validated. */
    CASE
      WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
       AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
        THEN 'Downgrade IN'

      WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
       AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
        THEN 'Exit'

      WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
       AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
       AND COALESCE(fr.Fac_Curr_Net_SSB,0) <> COALESCE(fr.Fac_Prev_Net_SSB,0)
        THEN 'Persisting Balance Changes'

      WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
       AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
       AND COALESCE(fr.Fac_Curr_Net_SSB,0) = COALESCE(fr.Fac_Prev_Net_SSB,0)
        THEN 'No Change'

      ELSE NULL
    END AS Movement_Type_Facility,

    /* IS_REVIEWABLE
       Any Downgrade IN or Exit with positive exposure on the relevant side.
       No RBC gate, no threshold gate — full in/out universe.
       Coverage_Threshold is evaluated separately below. */
    CASE
      WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
       AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
       AND COALESCE(fr.Fac_Curr_Pos_SSB,0) > 0                         THEN 1
      WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
       AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
       AND COALESCE(fr.Fac_Prev_Pos_SSB,0) > 0                         THEN 1
      ELSE 0
    END AS Is_Reviewable_Facility,

    /* COVERAGE_THRESHOLD (Samya/Casey L-CIT products only -- NULL for all others)
       Evaluated at CIS grain in cis_threshold CTE and stamped to each facility.
       Downgrade IN: threshold checked against CIS-level Curr SSB.
       Exit:         threshold checked against CIS-level Prev SSB.
       1 = clears threshold  |  0 = under threshold  |  NULL = not applicable */
    CASE
      WHEN ct.PROD5_For_Event = 'Business RE Term Loans'
        THEN CASE
               WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
                AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
                  THEN CASE WHEN COALESCE(ct.CIS_Curr_SSB_Sum,0) > 500000 THEN 1 ELSE 0 END
               WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
                AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
                  THEN CASE WHEN COALESCE(ct.CIS_Prev_SSB_Sum,0) > 500000 THEN 1 ELSE 0 END
               ELSE NULL
             END
      WHEN ct.PROD5_For_Event IN ('Business Non-RE Term Loans','Business Commitment Draw')
        THEN CASE
               WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
                AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
                  THEN CASE WHEN COALESCE(ct.CIS_Curr_SSB_Sum,0) > 250000 THEN 1 ELSE 0 END
               WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
                AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
                  THEN CASE WHEN COALESCE(ct.CIS_Prev_SSB_Sum,0) > 250000 THEN 1 ELSE 0 END
               ELSE NULL
             END
      ELSE NULL
    END AS Coverage_Threshold_Facility,

    /* AUTO_VALIDATED (scoped to Is_Reviewable = 1 population only)
       Downgrade IN: any of 5 conditions on curr side fires → auto-validated
         1. RBC_Code = 'R'
         2. PD_Grade IN ('12','13','14')
         3. FAS_114_FLAG = 'Y'
         4. CHARGE_OFF_AMOUNT_ITD > 0
         5. Days_Past_Due >= 90
       Exit: RBC_Code = 'R' on prev side only → auto-validated
         Rationale: exits are reviewed to confirm departure; only RBC drives auto-approval */
    CASE
      WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
       AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
       AND COALESCE(fr.Fac_Curr_Pos_SSB,0) > 0
       AND COALESCE(fr.Fac_Curr_Auto_Any,0)=1                          THEN 1
      WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
       AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
       AND COALESCE(fr.Fac_Prev_Pos_SSB,0) > 0
       AND COALESCE(fr.Fac_Prev_RBC_Auto_Any,0)=1                      THEN 1
      ELSE 0
    END AS Auto_Validated_Facility,

    /* MANUAL_REVIEW_REQUIRED
       Is_Reviewable AND NOT Auto_Validated AND Coverage_Threshold passes (1 or NULL).
       NULL threshold means threshold does not apply → treated as passing for this gate.
       These are the facilities that require an analyst to review. */
    CASE
      WHEN
        /* Must be reviewable in/out with positive exposure */
        (   (   (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
             AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
             AND COALESCE(fr.Fac_Curr_Pos_SSB,0) > 0
             AND COALESCE(fr.Fac_Curr_Auto_Any,0)=0 )
          OR
            (   (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
             AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
             AND COALESCE(fr.Fac_Prev_Pos_SSB,0) > 0
             AND COALESCE(fr.Fac_Prev_RBC_Auto_Any,0)=0 )
        )
        /* Coverage threshold: references CIS-level evaluation from cis_threshold CTE */
        AND COALESCE(
              CASE
                WHEN ct.PROD5_For_Event = 'Business RE Term Loans'
                  THEN CASE
                         WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
                          AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
                            THEN CASE WHEN COALESCE(ct.CIS_Curr_SSB_Sum,0) > 500000 THEN 1 ELSE 0 END
                         WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
                          AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
                            THEN CASE WHEN COALESCE(ct.CIS_Prev_SSB_Sum,0) > 500000 THEN 1 ELSE 0 END
                         ELSE NULL
                       END
                WHEN ct.PROD5_For_Event IN ('Business Non-RE Term Loans','Business Commitment Draw')
                  THEN CASE
                         WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
                          AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
                            THEN CASE WHEN COALESCE(ct.CIS_Curr_SSB_Sum,0) > 250000 THEN 1 ELSE 0 END
                         WHEN (CASE WHEN COALESCE(fr.Fac_Prev_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=1
                          AND (CASE WHEN COALESCE(fr.Fac_Curr_Abs_SSB,0) > 0 THEN 1 ELSE 0 END)=0
                            THEN CASE WHEN COALESCE(ct.CIS_Prev_SSB_Sum,0) > 250000 THEN 1 ELSE 0 END
                         ELSE NULL
                       END
                ELSE NULL
              END,
            1) = 1
      THEN 1 ELSE 0
    END AS Manual_Review_Required_Facility

  FROM facility_rollup fr
  LEFT JOIN cis_threshold ct
    ON  fr.Fac_CIS_For_Event  = ct.CIS_For_Event
    AND fr.Bank_For_Event      = ct.Bank_For_Event
    AND fr.Fac_PROD5_For_Event = ct.PROD5_For_Event
)

/* ══════════════════════════════════════════════════════════════════════════════════════════════════
   FINAL OUTPUT — account_key grain — 39 columns
   G1: Identity (8)  G2: Movement & Review (6)  G3: Balances (7)
   G4: Analyst Attrs (8)  G5: Diagnostics (9)

   REVIEW TIER LOGIC (G2):
     Is_Reviewable          = Any Downgrade IN or Exit with positive exposure. Full in/out universe.
     Coverage_Threshold     = 1/0/NULL. Only L-CIT RE Term Loans (Samya) and Non-RE/Draw (Casey).
                              NULL = threshold does not apply. 0 = under threshold, no review.
     Auto_Validated         = Downgrade IN: any of 5 conditions. Exit: RBC=R only.
                              Scoped to Is_Reviewable population. Automated review credit.
     Manual_Review_Required = Is_Reviewable AND NOT Auto_Validated AND Coverage_Threshold != 0.
                              These land with a named analyst.
     Owner_For_Event        = Named analyst. RBC=R → NO OWNER regardless of other rules.
   ══════════════════════════════════════════════════════════════════════════════════════════════════ */
SELECT

  /* ── G1: Identity & Event Keys ───────────────────────────────────────────────────────────────── */
  (SELECT query_run_ts FROM params) AS Query_Run_Timestamp,
  (SELECT curr_eom    FROM params) AS Curr_EOM,
  (SELECT prev_eom    FROM params) AS Prev_EOM,
  a.Account_Key,
  a.Facility_For_Event,
  a.CIS_For_Event,
  a.Account_Name_For_Event,
  a.Bank_For_Event,

  /* ── G2: Movement & Review Classification ────────────────────────────────────────────────────── */
  fc.Movement_Type_Facility                                                      AS Movement_Type,
  fc.Owner_Facility_For_Event                                                    AS Owner_For_Event,
  fc.Is_Reviewable_Facility                                                      AS Is_Reviewable,
  fc.Coverage_Threshold_Facility                                                 AS Coverage_Threshold,
  fc.Auto_Validated_Facility                                                     AS Auto_Validated,
  fc.Manual_Review_Required_Facility                                             AS Manual_Review_Required,

  /* ── G3: Balances ────────────────────────────────────────────────────────────────────────────── */
  a.Prev_SSB,
  a.Curr_SSB,
  a.MoM_Delta,
  a.SSB_For_Event,
  a.CIS_Curr_SSB_ByEventCIS,
  a.CIS_Prev_SSB_ByEventCIS,
  a.CIS_SSB_Exposure_ByEventCIS,

  /* ── G4: Analyst Review Attributes (event-side resolved — always populated) ──────────────────── */
  a.CSS_For_Event,
  a.PROD5_For_Event,
  a.GL_For_Event,
  a.Account_Identifier_For_Event,
  a.Status_For_Event,
  a.RBC_For_Event,
  a.PD_Grade_For_Event,
  a.DPD_For_Event,
  a.FAS114_For_Event,
  a.CO_MTD_For_Event,
  a.CO_ITD_For_Event,

  /* ── G5: Auto_Validated Diagnostics ─────────────────────────────────────────────────────────── */
  a.Auto_Validated_For_Event,
  a.prev_auto                                                                    AS Prev_Auto_Validated_Flag,
  a.curr_auto                                                                    AS Curr_Auto_Validated_Flag,
  a.prev_rbc_auto                                                                AS Prev_RBC_Auto_Flag,
  a.curr_rbc_auto                                                                AS Curr_RBC_Auto_Flag,
  CASE WHEN UPPER(COALESCE(a.RBC_For_Event,''))      = 'R'               THEN 1 ELSE 0 END AS RBC_R_Flag,
  CASE WHEN COALESCE(a.DPD_For_Event, 0)             >= 90               THEN 1 ELSE 0 END AS DPD_90_Flag,
  CASE WHEN a.PD_Grade_For_Event                      IN ('12','13','14') THEN 1 ELSE 0 END AS PD_High_Flag,
  CASE WHEN UPPER(COALESCE(a.FAS114_For_Event,''))   = 'Y'               THEN 1 ELSE 0 END AS FAS114_Flag,
  CASE WHEN COALESCE(a.CO_ITD_For_Event, 0)          > 0                 THEN 1 ELSE 0 END AS CO_ITD_Flag

FROM ranked_in_facility a
JOIN facility_classified fc
  ON a.Bank_For_Event     = fc.Bank_For_Event
 AND a.Facility_For_Event = fc.Facility_For_Event

WHERE NOT (COALESCE(a.Prev_SSB,0) = 0 AND COALESCE(a.Curr_SSB,0) = 0)

ORDER BY a.Bank_For_Event, a.Facility_For_Event, a.Account_Key;
