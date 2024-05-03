SELECT
    cfa.Level,
    sh.Main_Heading AS Topic,
    cfa.Year,
    COUNT(*) AS Number_of_articles,
    MIN(LENGTH(cfa.Introduction_Summary)) AS Min_Length_Summary,
    MAX(LENGTH(cfa.Introduction_Summary)) AS Max_Length_Summary,
    MIN(LENGTH(cfa.Learning_Outcomes)) AS Min_Length_Learning_Outcomes,
    MAX(LENGTH(cfa.Learning_Outcomes)) AS Max_Length_Learning_Outcomes
FROM {{ var('schema_name') }}.cfa.ctable cfa
JOIN {{ var('schema_name') }}.pfa.ptable sh ON TRIM(sh.Sub_Heading_Separated) = TRIM(cfa.Name_of_the_topic)
GROUP BY cfa.Level, sh.Main_Heading, cfa.Year
ORDER BY cfa.Level, sh.Main_Heading, cfa.Year
